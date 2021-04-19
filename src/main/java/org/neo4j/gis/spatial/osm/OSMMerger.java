/*
 * Copyright (c) 2010-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Spatial.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.osm;

import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexDefinition;

import java.util.ArrayList;

import static org.neo4j.gis.spatial.osm.OSMModel.*;

public class OSMMerger {
    private final OSMLayer layer;

    public OSMMerger(OSMLayer layer) {
        this.layer = layer;
    }

    private boolean checkNodeMoved(Node thisNode, long node_osm_id, String expectedPropKey, String nodePropKey, String keyDescription, Object value) {
        if (expectedPropKey.equals(nodePropKey)) {
            double thatValue = (Double) value;
            double thisValue = (Double) thisNode.getProperty(nodePropKey);
            if (Math.abs(thisValue - thatValue) > 0.00000001) {
                System.out.printf("Node '%d' has moved %s from %f to %f%n", node_osm_id, keyDescription, thisValue, thatValue);
                return true;
            }
        }
        return false;
    }

    private static final long INVALID_OSM_ID = Long.MIN_VALUE;

    private long getOSMId(String propertyKey, Node node) {
        Object result = node.getProperty(propertyKey, null);
        if (result == null) {
            return INVALID_OSM_ID;
        } else if (result instanceof Long) {
            return (Long) result;
        } else if (result instanceof Integer) {
            return (Integer) result;
        } else {
            System.out.printf("Invalid type found for property '%s': %s%n", propertyKey, result);
            return INVALID_OSM_ID;
        }
    }

    public long merge(Transaction tx, OSMLayer otherLayer) {
        OSMDataset dataset = OSMDataset.fromLayer(tx, layer);
        OSMDataset other = OSMDataset.fromLayer(tx, otherLayer);
        otherLayer.clear(tx);

        // Merge all OSM nodes
        MergeStats nodesStats = mergeNodes(tx, dataset.getIndex(tx, LABEL_NODE, PROP_NODE_ID), other.getIndex(tx, LABEL_NODE, PROP_NODE_ID));

        // Merge all OSM Ways
        MergeStats waysStats = mergeWays(tx, dataset, dataset.getIndex(tx, LABEL_WAY, PROP_WAY_ID), other.getIndex(tx, LABEL_WAY, PROP_WAY_ID));

        // Reconnect OSM ways, changesets and users chain to main layer
        reconnectChains(tx, dataset, other, OSMRelation.RELATIONS);
        reconnectChains(tx, dataset, other, OSMRelation.USERS);

        // TODO: changesets

        // Reindex if necessary
        if (nodesStats.needsReindexing() || waysStats.needsReindexing()) {
            this.layer.clear(tx);
            OSMImporter.GeomStats stats = new OSMImporter.GeomStats();
            OSMImporter.OSMIndexer indexer = new OSMImporter.OSMIndexer(layer, stats, false);
            for (Node geomNode : nodesStats.geomNodesToAdd) {
                indexer.indexByGeometryNode(tx, geomNode);
            }
            for (Node way : indexer.allWays(tx)) {
                indexer.indexByWay(tx, way);
            }
            stats.dumpGeomStats();
        }
        return nodesStats.changed() + waysStats.changed();
    }

    private void reconnectChains(Transaction tx, OSMDataset dataset, OSMDataset other, RelationshipType relType) {
        Node lastNode = dataset.getLastNodeInChain(tx, relType);
        Node nextNode = other.getFirstNodeInChain(tx, relType);
        nextNode.getSingleRelationship(relType, Direction.INCOMING).delete();
        lastNode.createRelationshipTo(nextNode, OSMRelation.NEXT);
    }

    private static class MergeStats {
        String name;
        long countMoved = 0;
        long countReplaced = 0;
        long countAdded = 0;
        ArrayList<Node> geomNodesToAdd = new ArrayList<>();

        MergeStats(String name) {
            this.name = name;
        }

        boolean needsReindexing() {
            return countMoved > 0 || countAdded > 0;
        }

        long changed() {
            return countMoved + countAdded;
        }

        void printStats() {
            if (countReplaced > 0) {
                System.out.printf("During merge we found %d existing %s which were replaced%n", countReplaced, name);
            }
            if (countMoved > 0) {
                System.out.printf("During merge %d out of %d existing %s were moved - re-indexing required%n", countMoved, countReplaced, name);
            }
            if (countAdded > 0) {
                System.out.printf("During merge %d %s were added - re-indexing required%n", countAdded, name);
            }
            if (geomNodesToAdd.size() > 0) {
                System.out.printf("During merge %d point geometry nodes were identified%n", geomNodesToAdd.size(), name);
            }
        }
    }

    private MergeStats mergeNodes(Transaction tx, IndexDefinition thisIndex, IndexDefinition thatIndex) {
        Label thisLabelHash = OSMDataset.hashedLabelFrom(thisIndex.getName());
        Label thatLabelHash = OSMDataset.hashedLabelFrom(thatIndex.getName());
        ResourceIterator<Node> nodes = tx.findNodes(thatLabelHash);
        MergeStats stats = new MergeStats("nodes");
        while (nodes.hasNext()) {
            Node node = nodes.next();
            long node_osm_id = getOSMId(PROP_NODE_ID, node);
            if (node_osm_id != INVALID_OSM_ID) {
                Node thisNode = tx.findNode(thisLabelHash, PROP_NODE_ID, node_osm_id);
                if (thisNode != null) {
                    // TODO: Consider comparing 'timestamp' field and always keeping the newer properties instead
                    stats.countReplaced++;
                    boolean moved = false;
                    for (String nodePropKey : node.getPropertyKeys()) {
                        Object value = node.getProperty(nodePropKey);
                        moved = moved || checkNodeMoved(thisNode, node_osm_id, PROP_NODE_LON, nodePropKey, "longitude", value);
                        moved = moved || checkNodeMoved(thisNode, node_osm_id, PROP_NODE_LAT, nodePropKey, "latitude", value);
                        thisNode.setProperty(nodePropKey, value);
                    }
                    if (moved) {
                        stats.countMoved++;
                    }
                    Relationship geomRel = thisNode.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING);
                    if (geomRel != null) {
                        Node geomNode = geomRel.getEndNode();
                        stats.geomNodesToAdd.add(geomNode);
                        double x = (Double) node.getProperty(PROP_NODE_LON);
                        double y = (Double) node.getProperty(PROP_NODE_LAT);
                        geomNode.setProperty(PROP_BBOX, new double[]{x, x, y, y});
                    }
                } else {
                    stats.countAdded++;
                    node.addLabel(thisLabelHash);
                    node.removeLabel(thatLabelHash);
                    Relationship geomRel = node.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING);
                    if (geomRel != null) {
                        Node geomNode = geomRel.getEndNode();
                        stats.geomNodesToAdd.add(geomNode);
                    }
                }
            } else {
                System.out.println("Unexpectedly found OSM node without property: " + PROP_NODE_ID);
            }
        }
        stats.printStats();
        return stats;
    }

    private MergeStats mergeWays(Transaction tx, OSMDataset dataset, IndexDefinition thisIndex, IndexDefinition thatIndex) {
        Label thisLabelHash = OSMDataset.hashedLabelFrom(thisIndex.getName());
        Label thatLabelHash = OSMDataset.hashedLabelFrom(thatIndex.getName());
        Node lastWayInChain = dataset.getLastNodeInChain(tx, OSMRelation.WAYS);
        ResourceIterator<Node> nodes = tx.findNodes(thatLabelHash);
        MergeStats stats = new MergeStats("ways");
        OSMGeometryEncoder geometryEncoder = (OSMGeometryEncoder) layer.getGeometryEncoder();
        while (nodes.hasNext()) {
            Node thatWay = nodes.next();
            long way_osm_id = getOSMId(PROP_WAY_ID, thatWay);
            Node thatGeom = thatWay.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING).getEndNode();
            Envelope thatEnvelope = geometryEncoder.decodeEnvelope(thatGeom);
            if (way_osm_id != INVALID_OSM_ID) {
                Node thisWay = tx.findNode(thisLabelHash, PROP_WAY_ID, way_osm_id);
                if (thisWay != null) {
                    // TODO: consider getting 'timestamp' and always saving the newest data
                    stats.countReplaced++;
                    for (String nodePropKey : thatWay.getPropertyKeys()) {
                        Object value = thatWay.getProperty(nodePropKey);
                        thisWay.setProperty(nodePropKey, value);
                    }
                    Node thisGeom = thisWay.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING).getEndNode();
                    Envelope thisEnvelope = geometryEncoder.decodeEnvelope(thisGeom);
                    if (!thisEnvelope.equals(thatEnvelope)) {
                        stats.countMoved++;
                        // TODO: merge the two ways by finding overlapping nodes
                    }
                } else {
                    stats.countAdded++;
                    extractWayFromChain(thatWay);
                    thatWay.addLabel(thisLabelHash);
                    thatWay.removeLabel(thatLabelHash);
                    if (lastWayInChain == null) {
                        dataset.getDatasetNode(tx).createRelationshipTo(thatWay, OSMRelation.WAYS);
                    } else {
                        lastWayInChain.createRelationshipTo(thatWay, OSMRelation.NEXT);
                    }
                    lastWayInChain = thatWay;
                }
            } else {
                System.out.println("Unexpectedly found OSM way without property: " + PROP_WAY_ID);
            }
        }
        stats.printStats();
        return stats;
    }

    private void extractWayFromChain(Node way) {
        if (!extractWayFromChain(way, OSMRelation.WAYS)) {
            extractWayFromChain(way, OSMRelation.NEXT);
        }
    }

    private boolean extractWayFromChain(Node way, RelationshipType incomingType) {
        Relationship incomingWays = way.getSingleRelationship(incomingType, Direction.INCOMING);
        if (incomingWays != null) {
            Node previous = incomingWays.getStartNode();
            incomingWays.delete();
            Relationship outgoingWays = way.getSingleRelationship(OSMRelation.NEXT, Direction.OUTGOING);
            if (outgoingWays != null) {
                Node next = outgoingWays.getEndNode();
                outgoingWays.delete();
                previous.createRelationshipTo(next, incomingType);
            }
            return true;
        } else {
            return false;
        }
    }
}
