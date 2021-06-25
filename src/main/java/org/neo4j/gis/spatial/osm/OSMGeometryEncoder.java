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

import org.locationtech.jts.algorithm.ConvexHull;
import org.locationtech.jts.geom.*;
import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;

import java.text.DateFormat;
import java.util.*;

import static org.neo4j.gis.spatial.osm.OSMModel.*;
import static org.neo4j.gis.spatial.utilities.TraverserFactory.createTraverserInBackwardsCompatibleWay;

public class OSMGeometryEncoder extends AbstractGeometryEncoder {

    public static final String PROP_MAX_FAKE_NODE_ID = "max_fake_node_osm_id";
    public static final String PROP_MAX_FAKE_WAY_ID = "max_fake_way_osm_id";
    public static final String PROP_MAX_FAKE_RELATION_ID = "max_fake_relation_osm_id";
    private static int decodedCount = 0;
    private static int overrunCount = 0;
    private FakeOSMId fake_node_osm_id = new FakeOSMId(PROP_MAX_FAKE_NODE_ID);
    private FakeOSMId fake_way_osm_id = new FakeOSMId(PROP_MAX_FAKE_WAY_ID);
    private FakeOSMId fake_relation_osm_id = new FakeOSMId(PROP_MAX_FAKE_RELATION_ID);
    private DateFormat dateTimeFormatter;
    private int vertices;
    private int vertexMismatches = 0;
    private final HashMap<Label, Label> labelHashes = new HashMap<>();

    /**
     * This class allows for OSM to avoid having empty tags nodes when there are
     * no properties on a geometry.
     */
    private static final class NullProperties implements Entity {
        @Override
        public Object getProperty(String key) {
            return null;
        }

        @Override
        public Object getProperty(String key, Object defaultValue) {
            return null;
        }

        @Override
        public Iterable<String> getPropertyKeys() {
            return null;
        }

        @Override
        public Map<String, Object> getProperties(String... strings) {
            return null;
        }

        @Override
        public Map<String, Object> getAllProperties() {
            return null;
        }

        @Override
        public long getId() {
            return 0;
        }

        @Override
        public boolean hasProperty(String key) {
            return false;
        }

        @Override
        public Object removeProperty(String key) {
            return null;
        }

        @Override
        public void setProperty(String key, Object value) {
        }
    }

    /**
     * This class tracks fake OSM ids.
     * Since this encoder can create geometries outside of OpenStreetMap itself,
     * it needs to generate fake ids. The convention used here is to start at -1
     * and decrement with each addition. The use of negative numbers ensures that
     * we never re-use an ID from real OpenStreetMap.
     * To allow server restarts, we also save the current ID on every create,
     * and restore the max ID when initializing the layer.
     */
    private static class FakeOSMId {
        private final String propertyKey;
        private long osm_id;

        private FakeOSMId(String propertyKey) {
            this.propertyKey = propertyKey;
            this.osm_id = 0;
        }

        private void init(Node layerNode) {
            if (layerNode != null) {
                if (layerNode.hasProperty(propertyKey)) {
                    osm_id = (Long) layerNode.getProperty(propertyKey);
                }
            }
        }

        private void save(Node layerNode, long osm_id) {
            this.osm_id = osm_id;
            layerNode.setProperty(propertyKey, osm_id);
        }

        private long next(Transaction tx, Layer layer) {
            osm_id++;
            Node layerNode = layer.getLayerNode(tx);
            layerNode.setProperty(propertyKey, osm_id);
            return -osm_id;
        }
    }

    public static class OSMGraphException extends SpatialDatabaseException {
        private static final long serialVersionUID = -6892234738075001044L;

        OSMGraphException(String message) {
            super(message);
        }

        OSMGraphException(String message, Exception cause) {
            super(message, cause);
        }
    }

    private static Node testIsNode(Entity container) {
        if (!(container instanceof Node)) {
            throw new OSMGraphException("Cannot decode non-node geometry: " + container);
        }
        return (Node) container;
    }

    /**
     * Mostly for testing, this method allows pre-seeding the fake OSM-ids with starting values
     */
    public void configure(Transaction tx, long nodeId, long wayId, long relId) {
        Node layerNode = layer.getLayerNode(tx);
        fake_node_osm_id.save(layerNode, nodeId);
        fake_way_osm_id.save(layerNode, wayId);
        fake_relation_osm_id.save(layerNode, relId);
    }

    @Override
    public void init(Transaction tx, Layer layer) {
        super.init(tx, layer);
        Node layerNode = this.layer.getLayerNode(tx);
        fake_node_osm_id.init(layerNode);
        fake_way_osm_id.init(layerNode);
        fake_relation_osm_id.init(layerNode);
    }

    @Override
    public Envelope decodeEnvelope(Entity container) {
        Node geomNode = testIsNode(container);
        double[] bbox = (double[]) geomNode.getProperty(PROP_BBOX);
        return new Envelope(bbox[0], bbox[1], bbox[2], bbox[3]);
    }

    @Override
    public void encodeEnvelope(Envelope mbb, Entity container) {
        container.setProperty(PROP_BBOX, new double[]{mbb.getMinX(), mbb.getMaxX(), mbb.getMinY(), mbb.getMaxY()});
    }

    public static Node getOSMNodeFromGeometryNode(Node geomNode) {
        Relationship rel = geomNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING);
        if (rel != null) {
            return rel.getStartNode();
        } else {
            throw new IllegalArgumentException("No geom rel");
        }
    }

    public static Node getGeometryNodeFromOSMNode(Node osmNode) {
        return osmNode.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING).getEndNode();
    }

    /**
     * This wrapper class allows the traverser to run simply down the NEXT
     * chain, but we wrap this to return the --NODE-->(node) results instead of
     * the proxy nodes.
     */
    private static class NodeProxyIterator implements Iterator<Node> {
        Iterator<Path> traverser;

        NodeProxyIterator(Node first) {
            TraversalDescription traversalDescription = new MonoDirectionalTraversalDescription().relationships(OSMRelation.NEXT, Direction.OUTGOING);
            traverser = createTraverserInBackwardsCompatibleWay(traversalDescription, first).iterator();
        }

        public boolean hasNext() {
            return traverser.hasNext();
        }

        public Node next() {
            return traverser.next().endNode().getSingleRelationship(OSMRelation.NODE, Direction.OUTGOING).getEndNode();
        }

        public void remove() {
        }

    }

    public Iterable<Node> getPointNodesFromWayNode(Node wayNode) {
        final Node firstNode = wayNode.getSingleRelationship(OSMRelation.FIRST_NODE, Direction.OUTGOING).getEndNode();
        final NodeProxyIterator iterator = new NodeProxyIterator(firstNode);
        return () -> iterator;
    }

    public Geometry decodeGeometry(Entity container) {
        Node geomNode = testIsNode(container);
        try {
            GeometryFactory geomFactory = layer.getGeometryFactory();
            Node osmNode = getOSMNodeFromGeometryNode(geomNode);
            if (osmNode.hasProperty(PROP_NODE_ID)) {
                return geomFactory.createPoint(new Coordinate(
                        (Double) osmNode.getProperty(PROP_NODE_LON, 0.0),
                        (Double) osmNode.getProperty(PROP_NODE_LAT, 0.0)));
            } else if (osmNode.hasProperty(PROP_WAY_ID)) {
                int vertices = (Integer) geomNode.getProperty("vertices");
                int gtype = (Integer) geomNode.getProperty(PROP_TYPE);
                return decodeGeometryFromWay(osmNode, gtype, vertices, geomFactory);
            } else {
                int gtype = (Integer) geomNode.getProperty(PROP_TYPE);
                return decodeGeometryFromRelation(osmNode, gtype, geomFactory);
            }
        } catch (Exception e) {
            throw new OSMGraphException("Failed to decode OSM geometry: " + e.getMessage(), e);
        }
    }

    private Geometry decodeGeometryFromRelation(Node osmNode, int gtype, GeometryFactory geomFactory) {
        switch (gtype) {
            case GTYPE_POLYGON:
                LinearRing outer = null;
                ArrayList<LinearRing> inner = new ArrayList<>();
                for (Relationship rel : osmNode.getRelationships(Direction.OUTGOING, OSMRelation.MEMBER)) {
                    Node wayNode = rel.getEndNode();
                    String role = (String) rel.getProperty("role", null);
                    if (role != null) {
                        LinearRing ring = getOuterLinearRingFromGeometry(decodeGeometryFromWay(wayNode, GTYPE_POLYGON, -1, geomFactory));
                        if (role.equals("outer")) {
                            outer = ring;
                        } else if (role.equals("inner")) {
                            inner.add(ring);
                        }
                    }
                }
                if (outer != null) {
                    return geomFactory.createPolygon(outer, inner.toArray(new LinearRing[0]));
                } else {
                    return null;
                }
            case GTYPE_MULTIPOLYGON:
                ArrayList<Polygon> polygons = new ArrayList<>();
                for (Relationship rel : osmNode.getRelationships(Direction.OUTGOING, OSMRelation.MEMBER)) {
                    Node member = rel.getEndNode();
                    Geometry geometry = null;
                    if (member.hasProperty(PROP_WAY_ID)) {
                        // decode simple polygons from ways
                        geometry = decodeGeometryFromWay(member, GTYPE_POLYGON, -1, geomFactory);
                    } else if (!member.hasProperty(PROP_NODE_ID)) {
                        // decode polygons with holes from relations
                        geometry = decodeGeometryFromRelation(member, GTYPE_POLYGON, geomFactory);
                    }
                    if (geometry instanceof Polygon) {
                        polygons.add((Polygon) geometry);
                    }
                }
                if (polygons.size() > 0) {
                    return geomFactory.createMultiPolygon(polygons.toArray(new Polygon[0]));
                } else {
                    return null;
                }
            default:
                return null;
        }
    }

    /**
     * Since OSM users can construct any weird combinations of geometries, we
     * need general code to make the best guess. This method will find a
     * enclosing LinearRing around any geometry except Point and a straight
     * LineString, and return that. For sensible types, it returns a more
     * sensible result, for example a Polygon will produce its outer LinearRing.
     */
    private LinearRing getOuterLinearRingFromGeometry(Geometry geometry) {
        if (geometry instanceof LineString) {
            LineString line = (LineString) geometry;
            if (line.getCoordinates().length < 3) {
                return null;
            } else {
                Coordinate[] coords = line.getCoordinates();
                if (!line.isClosed()) {
                    coords = closeCoords(coords);
                }
                LinearRing ring = geometry.getFactory().createLinearRing(coords);
                if (ring.isValid()) {
                    return ring;
                } else {
                    return getConvexHull(ring);
                }
            }
        } else if (geometry instanceof Polygon) {
            return ((Polygon) geometry).getExteriorRing();
        } else {
            return getConvexHull(geometry);
        }
    }

    /**
     * Extend the array by copying the first point into the last position
     *
     * @param coords original array that is not closed
     * @return new array one point longer
     */
    private Coordinate[] closeCoords(Coordinate[] coords) {
        Coordinate[] nc = new Coordinate[coords.length + 1];
        System.arraycopy(coords, 0, nc, 0, coords.length);
        nc[coords.length] = coords[0];
        coords = nc;
        return coords;
    }

    /**
     * The convex hull is like an elastic band surrounding all points in the
     * geometry.
     */
    private LinearRing getConvexHull(Geometry geometry) {
        return getOuterLinearRingFromGeometry((new ConvexHull(geometry)).getConvexHull());
    }

    private Geometry decodeGeometryFromWay(Node wayNode, int gtype, int vertices, GeometryFactory geomFactory) {
        ArrayList<Coordinate> coordinates = new ArrayList<>();
        boolean overrun = false;
        for (Node node : getPointNodesFromWayNode(wayNode)) {
            if (coordinates.size() >= vertices) {
                overrun = true;
                overrunCount++;
                break;
            }
            coordinates.add(new Coordinate((Double) node.getProperty(PROP_NODE_LON), (Double) node.getProperty(OSMModel.PROP_NODE_LAT)));
        }
        decodedCount++;
        if (overrun) {
            System.out.println("Overran expected number of way nodes: " + wayNode + " (" + overrunCount + "/" + decodedCount + ")");
        }
        if (coordinates.size() != vertices) {
            if (vertexMismatches++ < 10) {
                System.err.println("Mismatching vertices size for " + SpatialDatabaseService.convertGeometryTypeToName(gtype) + ":"
                        + wayNode + ": " + coordinates.size() + " != " + vertices);
            } else if (vertexMismatches % 100 == 0) {
                System.err.println("Mismatching vertices found " + vertexMismatches + " times");
            }
        }
        switch (coordinates.size()) {
            case 0:
                return null;
            case 1:
                return geomFactory.createPoint(coordinates.get(0));
            default:
                Coordinate[] coords = coordinates.toArray(new Coordinate[0]);
                switch (gtype) {
                    case GTYPE_LINESTRING:
                        return geomFactory.createLineString(coords);
                    case GTYPE_POLYGON:
                        return geomFactory.createPolygon(geomFactory.createLinearRing(coords), new LinearRing[0]);
                    default:
                        return geomFactory.createMultiPointFromCoords(coords);
                }
        }
    }

    /**
     * For OSM data we can build basic geometry shapes as sub-graphs. This code should produce the same kinds of structures that the utilities in the OSMDataset create. However those structures are created from original OSM data, while here we attempt to create equivalent graphs from JTS Geometries. Note that this code is unable to connect the resulting sub-graph into the OSM data model, since the only node it has is the geometry node. Those connections to the rest of the OSM model need to be done in OSMDataset.
     */
    @Override
    protected void encodeGeometryShape(Transaction tx, Geometry geometry, Entity container) {
        Node geomNode = testIsNode(container);
        vertices = 0;
        try {
            int gtype = SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass());
            switch (gtype) {
                case GTYPE_POINT:
                    makeOSMNode(tx, geometry, geomNode);
                    break;
                case GTYPE_LINESTRING:
                case GTYPE_MULTIPOINT:
                case GTYPE_POLYGON:
                    makeOSMWay(tx, geometry, geomNode, gtype);
                    break;
                case GTYPE_MULTILINESTRING:
                case GTYPE_MULTIPOLYGON:
                    int gsubtype = gtype == GTYPE_MULTIPOLYGON ? GTYPE_POLYGON : GTYPE_LINESTRING;
                    Node relationNode = makeOSMRelation(geometry, geomNode);
                    int num = geometry.getNumGeometries();
                    for (int i = 0; i < num; i++) {
                        Geometry geom = geometry.getGeometryN(i);
                        Node wayNode = makeOSMWay(tx, geom, tx.createNode(), gsubtype);
                        relationNode.createRelationshipTo(wayNode, OSMRelation.MEMBER);
                    }
                    break;
                default:
                    throw new SpatialDatabaseException("Unsupported geometry: " + geometry.getClass());
            }
            geomNode.setProperty("vertices", vertices);
        } catch (Exception e) {
            throw new SpatialDatabaseException("Failed to encode geometry '" + geometry.getGeometryType() + "': " + e.getMessage(), e);
        }
    }

    private void makeOSMNode(Transaction tx, Geometry geometry, Node geomNode) {
        Node node = makeOSMNode(tx, geometry.getCoordinate());
        node.createRelationshipTo(geomNode, OSMRelation.GEOM);
    }

    private void addLabelHash(Transaction tx, OSMDataset dataset, Label label, String propertyKey) {
        String indexName = dataset.getIndexName(tx, label, propertyKey);
        if (indexName != null) {
            labelHashes.put(label, OSMDataset.hashedLabelFrom(indexName));
        }
    }

    private void loadLabelHash(Transaction tx) {
        if (labelHashes.isEmpty()) {
            OSMDataset dataset = OSMDataset.fromLayer(tx, (OSMLayer) layer);
            addLabelHash(tx, dataset, LABEL_NODE, PROP_NODE_ID);
            addLabelHash(tx, dataset, LABEL_WAY, PROP_WAY_ID);
            addLabelHash(tx, dataset, LABEL_RELATION, PROP_RELATION_ID);
            addLabelHash(tx, dataset, LABEL_USER, PROP_USER_ID);
            addLabelHash(tx, dataset, LABEL_CHANGESET, PROP_CHANGESET);
        }
    }

    private Label getLabelHash(Transaction tx, Label label) {
        loadLabelHash(tx);
        return labelHashes.get(label);
    }

    private Node makeOSMNode(Transaction tx, Coordinate coordinate) {
        vertices++;
        Node node = tx.createNode(LABEL_NODE);
        Label hashed = getLabelHash(tx, LABEL_NODE);
        if (hashed != null) {
            // This allows this node to be found using the same index that the OSMImporter uses
            node.addLabel(hashed);
        }
        node.setProperty(PROP_NODE_ID, fake_node_osm_id.next(tx, layer));
        node.setProperty(PROP_NODE_LAT, coordinate.y);
        node.setProperty(PROP_NODE_LON, coordinate.x);
        node.setProperty("timestamp", getTimestamp());
        // TODO: Add other common properties, like changeset, uid, user, version
        return node;
    }

    private Node makeOSMWay(Transaction tx, Geometry geometry, Node geomNode, int gtype) {
        Node way = tx.createNode(LABEL_WAY);
        Label hashed = getLabelHash(tx, LABEL_WAY);
        if (hashed != null) {
            // This allows this node to be found using the same index that the OSMImporter uses
            way.addLabel(hashed);
        }
        way.setProperty(PROP_WAY_ID, fake_way_osm_id.next(tx, layer));
        way.setProperty("timestamp", getTimestamp());
        // TODO: Add other common properties, like changeset, uid, user,
        // version, name
        way.createRelationshipTo(geomNode, OSMRelation.GEOM);
        // TODO: if this way is a part of a complex geometry, the sub-geometries
        // are not indexed
        geomNode.setProperty(PROP_TYPE, gtype);
        Node prev = null;
        for (Coordinate coord : geometry.getCoordinates()) {
            Node node = makeOSMNode(tx, coord);
            Node proxyNode = tx.createNode();
            proxyNode.createRelationshipTo(node, OSMRelation.NODE);
            if (prev == null) {
                way.createRelationshipTo(proxyNode, OSMRelation.FIRST_NODE);
            } else {
                prev.createRelationshipTo(proxyNode, OSMRelation.NEXT);
            }
            prev = proxyNode;
        }
        return way;
    }

    @SuppressWarnings("unused")
    private Node makeOSMRelation(Geometry geometry, Node geomNode) {
        throw new SpatialDatabaseException("Unimplemented: makeOSMRelation()");
    }

    private String getTimestamp() {
        if (dateTimeFormatter == null)
            dateTimeFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        return dateTimeFormatter.format(new Date(System.currentTimeMillis()));
    }

    private Node lastGeom = null;
    private CombinedAttributes lastAttr = null;
    private long missingTags = 0;

    private class CombinedAttributes {
        private Node node;
        private Entity properties;
        private final HashMap<String, Object> extra = new HashMap<>();

        CombinedAttributes(Node geomNode) {
            try {
                node = geomNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode();
                properties = node.getSingleRelationship(OSMRelation.TAGS, Direction.OUTGOING).getEndNode();
                Node changeset = node.getSingleRelationship(OSMRelation.CHANGESET, Direction.OUTGOING).getEndNode();
                if (changeset != null) {
                    extra.put(PROP_CHANGESET, changeset.getProperty(PROP_CHANGESET, null));
                    Node user = changeset.getSingleRelationship(OSMRelation.USER, Direction.OUTGOING).getEndNode();
                    if (user != null) {
                        extra.put(PROP_USER_NAME, user.getProperty("name", null));
                        extra.put("user_id", user.getProperty("uid", null));
                    }
                }
            } catch (NullPointerException e) {
                if (missingTags++ < 10) {
                    System.err.println("Geometry has no related tags node: " + geomNode);
                } else if (missingTags % 100 == 0) {
                    System.err.println("Geometries without tags found " + missingTags + " times");
                }
                properties = new NullProperties();
            }
        }

        public boolean hasProperty(String key) {
            return extra.containsKey(key) || node.hasProperty(key) || properties.hasProperty(key);
        }

        public Object getProperty(String key) {
            return extra.containsKey(key) ? extra.get(key) : node.hasProperty(key) ? node.getProperty(key, null) : properties
                    .getProperty(key, null);
        }

    }

    private CombinedAttributes getProperties(Node geomNode) {
        if (geomNode != lastGeom) {
            lastGeom = geomNode;
            lastAttr = new CombinedAttributes(geomNode);
        }
        return lastAttr;
    }

    /**
     * This method wraps the hasProperty(String) method on the geometry node.
     * This means the default way of storing attributes is simply as properties
     * of the geometry node. This behaviour can be changed by other domain
     * models with different encodings.
     *
     * @param geomNode node to test
     * @param name     attribute to check for existence of
     * @return true if node has the specified attribute
     */
    public boolean hasAttribute(Node geomNode, String name) {
        return getProperties(geomNode).hasProperty(name);
    }

    /**
     * This method wraps the getProperty(String,null) method on the geometry
     * node. This means the default way of storing attributes is simply as
     * properties of the geometry node. This behaviour can be changed by other
     * domain models with different encodings. If the property does not exist,
     * the method returns null.
     *
     * @param geomNode node to test
     * @param name     attribute to access
     * @return attribute value, or null
     */
    public Object getAttribute(Node geomNode, String name) {
        return getProperties(geomNode).getProperty(name);
    }
}
