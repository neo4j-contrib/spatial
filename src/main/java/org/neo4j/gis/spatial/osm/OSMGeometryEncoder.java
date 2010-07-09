package org.neo4j.gis.spatial.osm;

import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser.Order;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class OSMGeometryEncoder extends AbstractGeometryEncoder {
    private static int decodedCount = 0;
    private static int overrunCount = 0;

    public static class OSMGraphException extends SpatialDatabaseException {
        private static final long serialVersionUID = -6892234738075001044L;

        public OSMGraphException(String message) {
            super(message);
        }

        public OSMGraphException(String message, Exception cause) {
            super(message, cause);
        }
    }

    private static Node testIsNode(PropertyContainer container) {
        if (!(container instanceof Node)) {
            throw new OSMGraphException("Cannot decode non-node geometry: " + container);
        }
        return (Node)container;
    }

    public Envelope decodeEnvelope(PropertyContainer container) {
        Node geomNode = testIsNode(container);
        double[] bbox = (double[])geomNode.getProperty("bbox");
        return new Envelope(bbox[0], bbox[1], bbox[2], bbox[3]);
    }

    public static Node getWayNodeFromGeometryNode(Node geomNode) {
        return geomNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode();
    }

    public static Node getGeometryNodeFromWayNode(Node wayNode) {
        return wayNode.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING).getEndNode();
    }

    public Iterable<Node> getPointNodesFromWayNode(Node wayNode) {
        final Node firstNode = wayNode.getSingleRelationship(OSMRelation.FIRST_NODE, Direction.OUTGOING).getEndNode();
        final Node lastNode = wayNode.getSingleRelationship(OSMRelation.LAST_NODE, Direction.OUTGOING).getEndNode();
        return firstNode.traverse(Order.DEPTH_FIRST, new StopEvaluator() {

            public boolean isStopNode(TraversalPosition currentPos) {
                return currentPos.currentNode().equals(lastNode);
            }
        }, ReturnableEvaluator.ALL, OSMRelation.NEXT, Direction.OUTGOING);
    }

    public Geometry decodeGeometry(PropertyContainer container) {
        Node geomNode = testIsNode(container);
        try {
            GeometryFactory geomFactory = layer.getGeometryFactory();
            int vertices = (Integer)geomNode.getProperty("vertices");
            Coordinate[] coordinates = new Coordinate[vertices];
            int index = 0;
            boolean overrun = false;
            for (Node node : getPointNodesFromWayNode(getWayNodeFromGeometryNode(geomNode))) {
                if (index >= vertices) {
                    // System.err.println("Exceeding expected number of way nodes: " + (index + 1) +
                    // " > " + vertices);
                    overrun = true;
                    overrunCount ++;
                    break;
                }
                coordinates[index++] = new Coordinate((Double)node.getProperty("lat"), (Double)node.getProperty("lon"));
            }
            decodedCount ++;
            if (overrun) {
                System.out.println("Overran expected number of way nodes: " + geomNode + " (" + overrunCount + "/" + decodedCount + ")");
            }
            switch (vertices) {
            case 0:
                return null;
            case 1:
                return geomFactory.createPoint(coordinates[0]);
            default:
                return geomFactory.createLineString(coordinates);
            }
        } catch (Exception e) {
            throw new OSMGraphException("Failed to decode OSM geometry: " + e.getMessage(), e);
        }
    }

    @Override
    protected void encodeGeometryShape(Geometry geometry, PropertyContainer container) {
        // Node geomNode = testIsNode(container);
        // TODO: implement this similarly to OSMImporter code, but with normal GraphDatabaseService
    }

}
