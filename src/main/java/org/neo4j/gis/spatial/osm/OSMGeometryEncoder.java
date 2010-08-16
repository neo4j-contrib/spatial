package org.neo4j.gis.spatial.osm;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

import org.geotools.referencing.operation.projection.NewZealandMapGrid;
import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.SpatialRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser.Order;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;

public class OSMGeometryEncoder extends AbstractGeometryEncoder {
    private static int decodedCount = 0;
    private static int overrunCount = 0;
    private static int nodeId = 0;
    private static int wayId = 0;
    private static int relationId = 0;
	private DateFormat dateTimeFormatter;
	private int vertices;

	/**
	 * This class allows for OSM to avoid having empty tags nodes when there are no properties on a geometry.
	 * @author craig
	 */
    private final class NullProperties implements PropertyContainer {
	    public GraphDatabaseService getGraphDatabase() {
	        return null;
	    }

	    public Object getProperty(String key) {
	        return null;
	    }

	    public Object getProperty(String key, Object defaultValue) {
	        return null;
	    }

	    public Iterable<String> getPropertyKeys() {
	        return null;
	    }

	    public Iterable<Object> getPropertyValues() {
	        return null;
	    }

	    public boolean hasProperty(String key) {
	        return false;
	    }

	    public Object removeProperty(String key) {
	        return null;
	    }

	    public void setProperty(String key, Object value) {
	    }
    }

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
        return (Node) container;
    }

    public Envelope decodeEnvelope(PropertyContainer container) {
        Node geomNode = testIsNode(container);
        double[] bbox = (double[]) geomNode.getProperty("bbox");
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
            int vertices = (Integer) geomNode.getProperty("vertices");
            ArrayList<Coordinate> coordinates = new ArrayList<Coordinate>();
            boolean overrun = false;
            for (Node node : getPointNodesFromWayNode(getWayNodeFromGeometryNode(geomNode))) {
                if (coordinates.size() >= vertices) {
                    // System.err.println("Exceeding expected number of way nodes: "
                    // + (index + 1) +
                    // " > " + vertices);
                    overrun = true;
                    overrunCount++;
                    break;
                }
                coordinates.add(new Coordinate((Double) node.getProperty("lon"), (Double) node.getProperty("lat")));
            }
            decodedCount++;
            if (overrun) {
                System.out.println("Overran expected number of way nodes: " + geomNode + " (" + overrunCount + "/" + decodedCount + ")");
            }
            if (coordinates.size() != vertices) {
                System.err.println("Mismatching vertices size: " + coordinates.size() + " != " + vertices);
            }
            switch (coordinates.size()) {
            case 0:
                return null;
            case 1:
                return geomFactory.createPoint(coordinates.get(0));
            default:
                return geomFactory.createLineString(coordinates.toArray(new Coordinate[coordinates.size()]));
            }
        } catch (Exception e) {
            throw new OSMGraphException("Failed to decode OSM geometry: " + e.getMessage(), e);
        }
    }

	@Override
	/**
	 * For OSM data we can build basic geometry shapes as sub-graphs. This code should produce the same kinds of structures that the utilities in the OSMDataset create. However those structures are created from original OSM data, while here we attempt to create equivalent graphs from JTS Geometries. Note that this code is unable to connect the resulting sub-graph into the OSM data model, since the only node it has is the geometry node. Those connections to the rest of the OSM model need to be done in OSMDataset.
	 */
	protected void encodeGeometryShape(Geometry geometry, PropertyContainer container) {
		Node geomNode = testIsNode(container);
		vertices = 0;
		switch (SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass())) {
		case GTYPE_POINT:
			makeOSMNode(geometry, geomNode);
			break;
		case GTYPE_LINESTRING:
		case GTYPE_MULTIPOINT:
		case GTYPE_POLYGON:
			makeOSMWay(geometry, geomNode);
			break;
		case GTYPE_MULTILINESTRING:
		case GTYPE_MULTIPOLYGON:
			Node relationNode = makeOSMRelation(geometry, geomNode);
			int num = geometry.getNumGeometries();
			for(int i=0;i<num;i++) {
				Geometry geom = geometry.getGeometryN(i);
				Node wayNode = makeOSMWay(geom, geomNode);
				relationNode.createRelationshipTo(wayNode, OSMRelation.MEMBER);
			}
			break;
		default:
			throw new SpatialDatabaseException("Unsupported geometry: " + geometry.getClass());
		}
		geomNode.setProperty("vertices", vertices);
	}

	private Node makeOSMNode(Geometry geometry, Node geomNode) {
		Node node = makeOSMNode(geometry.getCoordinate(),geomNode.getGraphDatabase());
		node.createRelationshipTo(geomNode, OSMRelation.GEOM);
		return node;
	}

	private Node makeOSMNode(Coordinate coordinate, GraphDatabaseService db) {
		vertices ++;
		nodeId ++;
		Node node = db.createNode();
		//TODO: Generate a valid osm id
		node.setProperty(OSMId.NODE.toString(), nodeId);
		node.setProperty("lat", coordinate.y);
		node.setProperty("lon", coordinate.x);
		node.setProperty("timestamp", getTimestamp());
		//TODO: Add other common properties, like changeset, uid, user, version
		return node;
	}

	private Node makeOSMWay(Geometry geometry, Node geomNode) {
		wayId ++;
		GraphDatabaseService db = geomNode.getGraphDatabase();
		Node way = db.createNode();
		// TODO: Generate a valid osm id
		way.setProperty(OSMId.WAY.toString(), wayId);
		way.setProperty("timestamp", getTimestamp());
		// TODO: Add other common properties, like changeset, uid, user,
		// version, name
		way.createRelationshipTo(geomNode, OSMRelation.GEOM);
		Node prev = null;
		Node node = null;
		for (Coordinate coord : geometry.getCoordinates()) {
			node = makeOSMNode(coord, db);
			if (prev == null) {
				way.createRelationshipTo(node, OSMRelation.FIRST_NODE);
			} else {
				prev.createRelationshipTo(node, OSMRelation.NEXT);
			}
			prev = node;
		}
		if (node != null) {
			way.createRelationshipTo(node, OSMRelation.LAST_NODE);
		}
		return node;
	}

	private Node makeOSMRelation(Geometry geometry, Node geomNode) {
		relationId ++;
		throw new SpatialDatabaseException("Unimplemented: makeOSMRelation()");
    }

	private String getTimestamp() {
		if (dateTimeFormatter == null)
			dateTimeFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
		return dateTimeFormatter.format(new Date(System.currentTimeMillis()));
	}

	private Node lastGeom = null;
	private PropertyContainer lastProp = null;

	private PropertyContainer getProperties(Node geomNode) {
		if (geomNode != lastGeom) {
			lastGeom = geomNode;
			try {
				lastProp = geomNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode().getSingleRelationship(
				        OSMRelation.TAGS, Direction.OUTGOING).getEndNode();
			} catch (NullPointerException e) {
				System.err.println("Geometry has no related tags node: " + e.getMessage());
				lastProp = new NullProperties();
			}
		}
		return lastProp;
	}

	/**
	 * This method wraps the hasProperty(String) method on the geometry node.
	 * This means the default way of storing attributes is simply as properties
	 * of the geometry node. This behaviour can be changed by other domain
	 * models with different encodings.
	 * 
	 * @param geomNode
	 * @param attribute
	 *            to test
	 * @return
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
	 * @param geomNode
	 * @param attribute
	 *            to test
	 * @return attribute, or null
	 */
	public Object getAttribute(Node geomNode, String name) {
		return getProperties(geomNode).getProperty(name, null);
	}

	public enum OSMId {
		NODE("node_osm_id"), WAY("way_osm_id"), RELATION("relation_osm_id");
		private String name;

		OSMId(String name) {
			this.name = name;
		}

		public String toString() {
			return name;
		}
	}
}
