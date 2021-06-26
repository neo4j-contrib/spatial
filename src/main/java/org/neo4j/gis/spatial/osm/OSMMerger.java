/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.gis.spatial.GeometryEncoder;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexDefinition;

import java.util.ArrayList;
import java.util.function.BiConsumer;

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
        MergeStats nodesStats = mergeNodes(tx, dataset, other);

        // Merge all OSM Ways
        MergeStats waysStats = mergeWays(tx, dataset, other);

        // Merge all OSM Relations
        MergeStats relationsStats = mergeRelations(tx, dataset, other);

        // TODO: relabel USERS and CHANGESETS

        // Reindex if necessary
        if (nodesStats.needsReindexing() || waysStats.needsReindexing() || relationsStats.needsReindexing()) {
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
                System.out.printf("During merge %d geometry %s were identified for use in re-indexing%n", geomNodesToAdd.size(), name);
            }
        }
    }

    private MergeStats mergeNodes(Transaction tx, OSMDataset dataset, OSMDataset other) {
        IndexDefinition thisIndex = dataset.getIndex(tx, LABEL_NODE, PROP_NODE_ID);
        IndexDefinition thatIndex = other.getIndex(tx, LABEL_NODE, PROP_NODE_ID);
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

    private MergeStats mergeWays(Transaction tx, OSMDataset dataset, OSMDataset other) {
        IndexDefinition thisIndex = dataset.getIndex(tx, LABEL_WAY, PROP_WAY_ID);
        IndexDefinition thatIndex = other.getIndex(tx, LABEL_WAY, PROP_WAY_ID);
        Label thisLabelHash = OSMDataset.hashedLabelFrom(thisIndex.getName());
        Label thatLabelHash = OSMDataset.hashedLabelFrom(thatIndex.getName());
        return mergeOSMEntities(tx, "ways", PROP_WAY_ID, thisLabelHash, thatLabelHash, this::wayOverlapMerger);
    }

    private MergeStats mergeRelations(Transaction tx, OSMDataset dataset, OSMDataset other) {
        IndexDefinition thisIndex = dataset.getIndex(tx, LABEL_RELATION, PROP_RELATION_ID);
        IndexDefinition thatIndex = other.getIndex(tx, LABEL_RELATION, PROP_RELATION_ID);
        Label thisLabelHash = OSMDataset.hashedLabelFrom(thisIndex.getName());
        Label thatLabelHash = OSMDataset.hashedLabelFrom(thatIndex.getName());
        return mergeOSMEntities(tx, "relations", PROP_RELATION_ID, thisLabelHash, thatLabelHash, this::relationOverlapMerger);
    }

    private void wayOverlapMerger(Node thisWay, Node thatWay) {
        // TODO: Merge ways by sorting nodes
    }

    private void relationOverlapMerger(Node thisWay, Node thatWay) {
        // TODO: Merge relations
    }

    private MergeStats mergeOSMEntities(Transaction tx, String entityName, String propertyKey, Label thisLabelHash, Label thatLabelHash, BiConsumer<Node, Node> overlapMerger) {
        MergeStats stats = new MergeStats(entityName);
        try (ResourceIterator<Node> nodes = tx.findNodes(thatLabelHash)) {
            OSMGeometryEncoder geometryEncoder = (OSMGeometryEncoder) layer.getGeometryEncoder();
            while (nodes.hasNext()) {
                Node thatEntityNode = nodes.next();
                long osm_id = getOSMId(propertyKey, thatEntityNode);
                if (osm_id != INVALID_OSM_ID) {
                    Node thisEntityNode = tx.findNode(thisLabelHash, propertyKey, osm_id);
                    if (thisEntityNode != null) {
                        // TODO: consider getting 'timestamp' and always saving the newest data
                        stats.countReplaced++;
                        for (String nodePropKey : thatEntityNode.getPropertyKeys()) {
                            Object value = thatEntityNode.getProperty(nodePropKey);
                            thisEntityNode.setProperty(nodePropKey, value);
                        }
                        Envelope thisEnvelope = getGeometryEnvelope(thisEntityNode, geometryEncoder);
                        Envelope thatEnvelope = getGeometryEnvelope(thatEntityNode, geometryEncoder);
                        if (thisEnvelope == null && thatEnvelope == null) {
                            System.out.printf("While merging OSM %s, found nodes with %s = %d which have no geometry on either original or merge nodes%n", entityName, propertyKey, osm_id);
                        } else if (thisEnvelope == null) {
                            System.out.printf("While merging OSM %s, found nodes with %s = %d where the original has no geometry, but the merge node has a geometry%n", entityName, propertyKey, osm_id);
                            stats.countMoved++;
                            overlapMerger.accept(thisEntityNode, thatEntityNode);
                        } else if (thatEnvelope == null) {
                            System.out.printf("While merging OSM %s, found nodes with %s = %d where the original has a geometry, but the merge node does not%n", entityName, propertyKey, osm_id);
                        } else if (!thisEnvelope.equals(thatEnvelope)) {
                            stats.countMoved++;
                            overlapMerger.accept(thisEntityNode, thatEntityNode);
                        }
                    } else {
                        stats.countAdded++;
                        thatEntityNode.addLabel(thisLabelHash);
                        thatEntityNode.removeLabel(thatLabelHash);
                    }
                } else {
                    System.out.printf("Unexpectedly found OSM %s at %s without property: %s%n", entityName, thatEntityNode, propertyKey);
                }
            }
            stats.printStats();
            return stats;
        }
    }

    private Envelope getGeometryEnvelope(Node entityNode, GeometryEncoder geometryEncoder) {
        Relationship geomRel = entityNode.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING);
        return geomRel == null ? null : geometryEncoder.decodeEnvelope(geomRel.getEndNode());
    }
}
