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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

public class OSMDataset implements SpatialDataset, Iterator<OSMDataset.Way> {
    private final OSMLayer layer;
    private final long datasetNodeId;
    private Iterator<Node> wayNodeIterator;
    private final LabelHasher labelHasher;

    private OSMDataset(OSMLayer layer, long datasetNodeId) {
        this.layer = layer;
        this.datasetNodeId = datasetNodeId;
        this.layer.setDataset(this);
        try {
            this.labelHasher = new LabelHasher(layer.getName());
        } catch (NoSuchAlgorithmException e) {
            throw new SpatialDatabaseException("Failed to initialize OSM dataset '" + layer.getName() + "': " + e.getMessage(), e);
        }
    }

    /**
     * This method is used to construct the dataset on an existing node when the node id is known,
     * which is the case with OSM importers.
     */
    public static OSMDataset withDatasetId(Transaction tx, OSMLayer layer, long datasetNodeId) {
        Node datasetNode = tx.getNodeById(datasetNodeId);
        Node layerNode = layer.getLayerNode(tx);
        Relationship rel = layerNode.getSingleRelationship(SpatialRelationshipTypes.LAYERS, Direction.INCOMING);
        if (rel == null) {
            datasetNode.createRelationshipTo(layerNode, SpatialRelationshipTypes.LAYERS);
        } else {
            Node node = rel.getStartNode();
            if (!node.equals(datasetNode)) {
                throw new SpatialDatabaseException("Layer '" + layer + "' already belongs to another dataset: " + node);
            }
        }
        return new OSMDataset(layer, datasetNodeId);
    }

    /**
     * This method is used to construct the dataset when only the layer node is known, and the
     * dataset node needs to be searched for.
     */
    public static OSMDataset fromLayer(Transaction tx, OSMLayer layer) {
        Relationship rel = layer.getLayerNode(tx).getSingleRelationship(SpatialRelationshipTypes.LAYERS, Direction.INCOMING);
        if (rel == null) {
            Node datasetNode = tx.createNode(OSMModel.LABEL_DATASET);
            datasetNode.setProperty("name", layer.getName());
            datasetNode.setProperty("type", "osm");
            datasetNode.createRelationshipTo(layer.getLayerNode(tx), SpatialRelationshipTypes.LAYERS);
            return new OSMDataset(layer, datasetNode.getId());
        } else {
            long datasetNodeId = rel.getStartNode().getId();
            return new OSMDataset(layer, datasetNodeId);
        }
    }

    public Iterable<Node> getAllUserNodes(Transaction tx) {
        return () -> tx.findNodes(labelHasher.getLabelHashed(OSMModel.LABEL_USER));
    }

    public Iterable<Node> getAllChangesetNodes(Transaction tx) {
        return () -> tx.findNodes(labelHasher.getLabelHashed(OSMModel.LABEL_CHANGESET));
    }

    public Iterable<Node> getAllWayNodes(Transaction tx) {
        return () -> tx.findNodes(labelHasher.getLabelHashed(OSMModel.LABEL_WAY));
    }

    public Iterable<Node> getAllPointNodes(Transaction tx) {
        return () -> tx.findNodes(labelHasher.getLabelHashed(OSMModel.LABEL_NODE));
    }

    public Iterable<Node> getWayNodes(Node way) {
        TraversalDescription td = new MonoDirectionalTraversalDescription()
                .depthFirst()
                .relationships(OSMRelation.NEXT, Direction.OUTGOING)
                .relationships(OSMRelation.NODE, Direction.OUTGOING)
                .evaluator(Evaluators.includeWhereLastRelationshipTypeIs(OSMRelation.NODE));
        return td.traverse(
                way.getSingleRelationship(OSMRelation.FIRST_NODE, Direction.OUTGOING).getEndNode()
        ).nodes();
    }

    public Node getChangeset(Node way) {
        try {
            return way.getSingleRelationship(OSMRelation.CHANGESET, Direction.OUTGOING).getEndNode();
        } catch (Exception e) {
            System.out.println("Node has no changeset: " + e.getMessage());
            return null;
        }
    }

    public Node getUser(Node nodeWayOrChangeset) {
        TraversalDescription td = new MonoDirectionalTraversalDescription()
                .depthFirst()
                .relationships(OSMRelation.CHANGESET, Direction.OUTGOING)
                .relationships(OSMRelation.USER, Direction.OUTGOING)
                .evaluator(Evaluators.includeWhereLastRelationshipTypeIs(OSMRelation.USER));
        Iterator<Node> results = td.traverse(nodeWayOrChangeset).nodes().iterator();
        return results.hasNext() ? results.next() : null;
    }

    public Node getDatasetNode(Transaction tx) {
        return tx.getNodeById(datasetNodeId);
    }

    public Way getWayFromId(Transaction tx, long id) {
        return getWayFrom(tx.getNodeById(id));
    }

    public Way getWayFrom(Node osmNodeOrWayNodeOrGeomNode) {
        TraversalDescription td = new MonoDirectionalTraversalDescription()
                .depthFirst()
                .relationships(OSMRelation.NODE, Direction.INCOMING)
                .relationships(OSMRelation.NEXT, Direction.INCOMING)
                .relationships(OSMRelation.FIRST_NODE, Direction.INCOMING)
                .relationships(OSMRelation.GEOM, Direction.INCOMING)
                .evaluator(path -> path.endNode().hasProperty("way_osm_id") ? Evaluation.INCLUDE_AND_PRUNE
                        : Evaluation.EXCLUDE_AND_CONTINUE);
        Iterator<Node> results = td.traverse(osmNodeOrWayNodeOrGeomNode).nodes().iterator();
        return results.hasNext() ? new Way(results.next()) : null;
    }

    public class OSMNode {
        protected Node node;
        protected Node geomNode;
        protected Geometry geometry;

        OSMNode(Node node) {
            this.node = node;
            Relationship geomRel = this.node.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING);
            if (geomRel != null) geomNode = geomRel.getEndNode();
        }

        public Way getWay() {
            return OSMDataset.this.getWayFrom(this.node);
        }

        public Geometry getGeometry() {
            if (geometry == null && geomNode != null) {
                geometry = layer.getGeometryEncoder().decodeGeometry(geomNode);
            }
            return geometry;
        }

        public Envelope getEnvelope() {
            return getGeometry().getEnvelopeInternal();
        }

        public boolean equals(OSMNode other) {
            return this.node.getId() == other.node.getId();
        }

        public Node getNode() {
            return node;
        }

        public String toString() {
            if (node.hasProperty("name")) {
                return node.getProperty("name").toString();
            } else if (getGeometry() != null) {
                return getGeometry().getGeometryType();
            } else {
                return node.toString();
            }
        }
    }

    public class Way extends OSMNode implements Iterable<WayPoint>, Iterator<WayPoint> {
        private Iterator<Node> wayPointNodeIterator;

        Way(Node node) {
            super(node);
        }

        Iterable<Node> getWayNodes() {
            return OSMDataset.this.getWayNodes(this.node);
        }

        public Iterable<WayPoint> getWayPoints() {
            return this;
        }

        public Iterator<WayPoint> iterator() {
            if (wayPointNodeIterator == null || !wayPointNodeIterator.hasNext()) {
                wayPointNodeIterator = getWayNodes().iterator();
            }
            return this;
        }

        public boolean hasNext() {
            return wayPointNodeIterator.hasNext();
        }

        public WayPoint next() {
            return new WayPoint(wayPointNodeIterator.next());
        }

        public void remove() {
            throw new UnsupportedOperationException("Cannot modify way-point collection");
        }

        public WayPoint getPointAt(Coordinate coordinate) {
            for (WayPoint wayPoint : getWayPoints()) {
                if (wayPoint.isAt(coordinate))
                    return wayPoint;
            }
            return null;
        }

    }

    public class WayPoint extends OSMNode {
        WayPoint(Node node) {
            super(node);
        }

        boolean isAt(Coordinate coord) {
            return getCoordinate().equals(coord);
        }

        public Coordinate getCoordinate() {
            return new Coordinate(getX(), getY());
        }

        private double getY() {
            return (Double) node.getProperty("latitude", 0.0);
        }

        private double getX() {
            return (Double) node.getProperty("longitude", 0.0);
        }
    }

    @Override
    public Iterable<? extends Geometry> getAllGeometries(Transaction tx) {
        //@TODO: support multiple layers
        return layer.getAllGeometries(tx);
    }

    @Override
    public Iterable<Node> getAllGeometryNodes(Transaction tx) {
        //@TODO: support multiple layers
        return layer.getAllGeometryNodes(tx);
    }

    @Override
    public boolean containsGeometryNode(Transaction tx, Node geomNode) {
        //@TODO: support multiple layers
        return layer.containsGeometryNode(tx, geomNode);
    }

    @Override
    public GeometryEncoder getGeometryEncoder() {
        //@TODO: support multiple layers
        return layer.getGeometryEncoder();
    }

    @Override
    public Iterable<? extends Layer> getLayers() {
        return Collections.singletonList(layer);
    }

    public Iterable<Way> getWays(final Transaction tx) {
        return () -> OSMDataset.this.iterator(tx);
    }

    public Iterator<Way> iterator(Transaction tx) {
        if (wayNodeIterator == null || !wayNodeIterator.hasNext()) {
            wayNodeIterator = getAllWayNodes(tx).iterator();
        }
        return this;
    }

    @Override
    public boolean hasNext() {
        return wayNodeIterator.hasNext();
    }

    @Override
    public Way next() {
        return new Way(wayNodeIterator.next());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot modify way collection");
    }

    public int getPoiCount(Transaction tx) {
        return (Integer) tx.getNodeById(this.datasetNodeId).getProperty("poiCount", 0);
    }

    public int getNodeCount(Transaction tx) {
        return (Integer) tx.getNodeById(this.datasetNodeId).getProperty("nodeCount", 0);
    }

    public int getWayCount(Transaction tx) {
        return (Integer) tx.getNodeById(this.datasetNodeId).getProperty("wayCount", 0);
    }

    public int getRelationCount(Transaction tx) {
        return (Integer) tx.getNodeById(this.datasetNodeId).getProperty("relationCount", 0);
    }

    public int getChangesetCount(Transaction tx) {
        return (Integer) tx.getNodeById(this.datasetNodeId).getProperty("changesetCount", 0);
    }

    public int getUserCount(Transaction tx) {
        return (Integer) tx.getNodeById(this.datasetNodeId).getProperty("userCount", 0);
    }

    public String getIndexName(Transaction tx, Label label, String propertyKey) {
        Node datasetNode = tx.getNodeById(this.datasetNodeId);
        String indexKey = indexKeyFor(label, propertyKey);
        return (String) datasetNode.getProperty(indexKey, null);
    }

    public IndexDefinition getIndex(Transaction tx, Label label, String propertyKey) {
        String indexName = getIndexName(tx, label, propertyKey);
        if (indexName == null) {
            throw new IllegalArgumentException(String.format("OSM Dataset '%s' does not have an index for label '%s' and property '%s'", this.layer.getName(), label.name(), propertyKey));
        } else {
            return tx.schema().getIndexByName(indexName);
        }
    }

    public static String indexKeyFor(Label label, String propertyKey) {
        return String.format("Index:%s:%s", label.name(), propertyKey);
    }

    public static String indexNameFor(String layerName, String hashedLabel, String propertyKey) {
        return String.format("OSM-%s-%s-%s", layerName, hashedLabel, propertyKey);
    }

    public static Label hashedLabelFrom(String indexName) {
        String[] fields = indexName.split("-");
        if (fields.length == 4) {
            return Label.label(fields[2]);
        } else {
            throw new IllegalArgumentException(String.format("Index name '%s' is not correctly formatted - cannot extract label hash", indexName));
        }
    }

    public static class LabelHasher {
        private final String layerHash;
        private final HashMap<Label, Label> hashedLabels = new HashMap<>();

        public LabelHasher(String layerName) throws NoSuchAlgorithmException {
            this.layerHash = md5Hash(layerName);
        }

        public Label getLabelHashed(Label label) {
            if (hashedLabels.containsKey(label)) {
                return hashedLabels.get(label);
            } else {
                Label hashed = Label.label(label.name() + "_" + layerHash);
                hashedLabels.put(label, hashed);
                return hashed;
            }
        }

        public static String md5Hash(String text) throws NoSuchAlgorithmException {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(text.getBytes());
            byte[] digest = md.digest();
            return DatatypeConverter.printHexBinary(digest).toUpperCase();
        }
    }
}
