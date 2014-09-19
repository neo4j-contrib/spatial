/**
 * Copyright (c) 2010-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.osm;

import static org.neo4j.gis.spatial.utilities.TraverserFactory.createTraverserInBackwardsCompatibleWay;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalDescription;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

public class OSMGeometryEncoder extends AbstractGeometryEncoder {

	private static int decodedCount = 0;
	private static int overrunCount = 0;
	private static int nodeId = 0;
	private static int wayId = 0;
	private static int relationId = 0;
	private DateFormat dateTimeFormatter;
	private int vertices;
	private int vertexMistmaches = 0;

	/**
	 * This class allows for OSM to avoid having empty tags nodes when there are
	 * no properties on a geometry.
	 * 
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

	@Override
	public Envelope decodeEnvelope(PropertyContainer container) {
		Node geomNode = testIsNode(container);
		double[] bbox = (double[]) geomNode.getProperty(PROP_BBOX);
		// double xmin, double xmax, double ymin, double ymax
		return new Envelope(bbox[0], bbox[1], bbox[2], bbox[3]);
	}

	@Override
	public void encodeEnvelope(Envelope mbb, PropertyContainer container) {
		container.setProperty(PROP_BBOX, new double[] { mbb.getMinX(), mbb.getMaxX(), mbb.getMinY(), mbb.getMaxY() });
	}
	
	public static Node getOSMNodeFromGeometryNode(Node geomNode) {
		return geomNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode();
	}

	public static Node getGeometryNodeFromOSMNode(Node osmNode) {
		return osmNode.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING).getEndNode();
	}

	/**
	 * This wrapper class allows the traverser to run simply down the NEXT
	 * chain, but we wrap this to return the --NODE-->(node) results instead of
	 * the proxy nodes.
	 * 
	 * @author craig
	 */
	private class NodeProxyIterator implements Iterator<Node> {
		Iterator<Path> traverser;

		NodeProxyIterator(Node first) {
            TraversalDescription traversalDescription =  first.getGraphDatabase().traversalDescription().relationships(OSMRelation.NEXT,
                    Direction.OUTGOING);
            traverser = createTraverserInBackwardsCompatibleWay( traversalDescription, first ).iterator();
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
		return new Iterable<Node>() {

			public Iterator<Node> iterator() {
				return iterator;
			}
		};
	}

	public Geometry decodeGeometry(PropertyContainer container) {
		Node geomNode = testIsNode(container);
		try {
			GeometryFactory geomFactory = layer.getGeometryFactory();
			Node osmNode = getOSMNodeFromGeometryNode(geomNode);
			if (osmNode.hasProperty("node_osm_id")) {
				return geomFactory.createPoint(new Coordinate((Double) osmNode.getProperty("lon", 0.0), (Double) osmNode
						.getProperty("lat", 0.0)));
			} else if (osmNode.hasProperty("way_osm_id")) {
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
			ArrayList<LinearRing> inner = new ArrayList<LinearRing>();
			// ArrayList<LinearRing> rings = new ArrayList<LinearRing>();
			for (Relationship rel : osmNode.getRelationships(OSMRelation.MEMBER, Direction.OUTGOING)) {
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
				return geomFactory.createPolygon(outer, inner.toArray(new LinearRing[inner.size()]));
			} else {
				return null;
			}
		case GTYPE_MULTIPOLYGON:
			ArrayList<Polygon> polygons = new ArrayList<Polygon>();
			for (Relationship rel : osmNode.getRelationships(OSMRelation.MEMBER, Direction.OUTGOING)) {
				Node member = rel.getEndNode();
				Geometry geometry = null;
				if (member.hasProperty("way_osm_id")) {
					// decode simple polygons from ways
					geometry = decodeGeometryFromWay(member, GTYPE_POLYGON, -1, geomFactory);
				} else if (!member.hasProperty("node_osm_id")) {
					// decode polygons with holes from relations
					geometry = decodeGeometryFromRelation(member, GTYPE_POLYGON, geomFactory);
				}
				if (geometry != null && geometry instanceof Polygon) {
					polygons.add((Polygon) geometry);
				}
			}
			if (polygons.size() > 0) {
				return geomFactory.createMultiPolygon(polygons.toArray(new Polygon[polygons.size()]));
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
	 * 
	 * @param geometry
	 * @return enclosing LinearRing
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
		} else if (geometry instanceof LinearRing) {
			return (LinearRing) geometry;
		} else if (geometry instanceof Polygon) {
			return (LinearRing) ((Polygon) geometry).getExteriorRing();
		} else {
			return getConvexHull(geometry);
		}
	}

	/**
	 * Extend the array by copying the first point into the last position
	 * 
	 * @param coords
	 * @return new array one point longer
	 */
	private Coordinate[] closeCoords(Coordinate[] coords) {
		Coordinate[] nc = new Coordinate[coords.length + 1];
		for (int i = 0; i < coords.length; i++) {
			nc[i] = coords[i];
		}
		nc[coords.length] = coords[0];
		coords = nc;
		return coords;
	}

	/**
	 * The convex hull is like an elastic band surrounding all points in the
	 * geometry.
	 * 
	 * @param geometry
	 * @return
	 */
	private LinearRing getConvexHull(Geometry geometry) {
		return getOuterLinearRingFromGeometry((new ConvexHull(geometry)).getConvexHull());
	}

	private Geometry decodeGeometryFromWay(Node wayNode, int gtype, int vertices, GeometryFactory geomFactory) {
		ArrayList<Coordinate> coordinates = new ArrayList<Coordinate>();
		boolean overrun = false;
		for (Node node : getPointNodesFromWayNode(wayNode)) {
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
			System.out.println("Overran expected number of way nodes: " + wayNode + " (" + overrunCount + "/" + decodedCount + ")");
		}
		if (coordinates.size() != vertices) {
			if (vertexMistmaches++ < 10) {
				System.err.println("Mismatching vertices size for " + SpatialDatabaseService.convertGeometryTypeToName(gtype) + ":"
						+ wayNode + ": " + coordinates.size() + " != " + vertices);
			} else if (vertexMistmaches % 100 == 0) {
				System.err.println("Mismatching vertices found " + vertexMistmaches + " times");
			}
		}
		switch (coordinates.size()) {
		case 0:
			return null;
		case 1:
			return geomFactory.createPoint(coordinates.get(0));
		default:
			switch (gtype) {
			case GTYPE_LINESTRING:
				return geomFactory.createLineString(coordinates.toArray(new Coordinate[coordinates.size()]));
			case GTYPE_POLYGON:
				return geomFactory.createPolygon(
						geomFactory.createLinearRing(coordinates.toArray(new Coordinate[coordinates.size()])), new LinearRing[0]);
			default:
				return geomFactory.createMultiPoint(coordinates.toArray(new Coordinate[coordinates.size()]));
			}
		}
	}

	@Override
	/**
	 * For OSM data we can build basic geometry shapes as sub-graphs. This code should produce the same kinds of structures that the utilities in the OSMDataset create. However those structures are created from original OSM data, while here we attempt to create equivalent graphs from JTS Geometries. Note that this code is unable to connect the resulting sub-graph into the OSM data model, since the only node it has is the geometry node. Those connections to the rest of the OSM model need to be done in OSMDataset.
	 */
	protected void encodeGeometryShape(Geometry geometry, PropertyContainer container) {
		Node geomNode = testIsNode(container);
		vertices = 0;
		int gtype = SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass());
		switch (gtype) {
		case GTYPE_POINT:
			makeOSMNode(geometry, geomNode);
			break;
		case GTYPE_LINESTRING:
		case GTYPE_MULTIPOINT:
		case GTYPE_POLYGON:
			makeOSMWay(geometry, geomNode, gtype);
			break;
		case GTYPE_MULTILINESTRING:
		case GTYPE_MULTIPOLYGON:
			int gsubtype = gtype == GTYPE_MULTIPOLYGON ? GTYPE_POLYGON : GTYPE_LINESTRING;
			Node relationNode = makeOSMRelation(geometry, geomNode);
			int num = geometry.getNumGeometries();
			for (int i = 0; i < num; i++) {
				Geometry geom = geometry.getGeometryN(i);
				Node wayNode = makeOSMWay(geom, geomNode.getGraphDatabase().createNode(), gsubtype);
				relationNode.createRelationshipTo(wayNode, OSMRelation.MEMBER);
			}
			break;
		default:
			throw new SpatialDatabaseException("Unsupported geometry: " + geometry.getClass());
		}
		geomNode.setProperty("vertices", vertices);
	}

	private Node makeOSMNode(Geometry geometry, Node geomNode) {
		Node node = makeOSMNode(geometry.getCoordinate(), geomNode.getGraphDatabase());
		node.createRelationshipTo(geomNode, OSMRelation.GEOM);
		return node;
	}

	private Node makeOSMNode(Coordinate coordinate, GraphDatabaseService db) {
		vertices++;
		nodeId++;
		Node node = db.createNode();
		// TODO: Generate a valid osm id
		node.setProperty(OSMId.NODE.toString(), nodeId);
		node.setProperty("lat", coordinate.y);
		node.setProperty("lon", coordinate.x);
		node.setProperty("timestamp", getTimestamp());
		// TODO: Add other common properties, like changeset, uid, user, version
		return node;
	}

	private Node makeOSMWay(Geometry geometry, Node geomNode, int gtype) {
		wayId++;
		GraphDatabaseService db = geomNode.getGraphDatabase();
		Node way = db.createNode();
		// TODO: Generate a valid osm id
		way.setProperty(OSMId.WAY.toString(), wayId);
		way.setProperty("timestamp", getTimestamp());
		// TODO: Add other common properties, like changeset, uid, user,
		// version, name
		way.createRelationshipTo(geomNode, OSMRelation.GEOM);
		// TODO: if this way is a part of a complex geometry, the sub-geometries
		// are not indexed
		geomNode.setProperty(PROP_TYPE, gtype);
		Node prev = null;
		for (Coordinate coord : geometry.getCoordinates()) {
			Node node = makeOSMNode(coord, db);
			Node proxyNode = db.createNode();
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

	private Node makeOSMRelation(Geometry geometry, Node geomNode) {
		relationId++;
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
		private PropertyContainer properties;
		private HashMap<String, Object> extra = new HashMap<String, Object>();

		CombinedAttributes(Node geomNode) {
			try {
				node = geomNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode();
				properties = node.getSingleRelationship(OSMRelation.TAGS, Direction.OUTGOING).getEndNode();
				Node changeset = node.getSingleRelationship(OSMRelation.CHANGESET, Direction.OUTGOING).getEndNode();
				if (changeset != null) {
					extra.put("changeset", changeset.getProperty("changeset", null));
					Node user = changeset.getSingleRelationship(OSMRelation.USER, Direction.OUTGOING).getEndNode();
					if (user != null) {
						extra.put("user", user.getProperty("name", null));
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
	 * @param geomNode
	 * @param name to test
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
	 * @param name to test
	 * @return attribute, or null
	 */
	public Object getAttribute(Node geomNode, String name) {
		return getProperties(geomNode).getProperty(name);
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
