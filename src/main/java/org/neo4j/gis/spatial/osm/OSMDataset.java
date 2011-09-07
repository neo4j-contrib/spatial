/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.gis.spatial.GeometryEncoder;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.SpatialDataset;
import org.neo4j.gis.spatial.SpatialRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser.Order;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;


public class OSMDataset implements SpatialDataset, Iterable<OSMDataset.Way>, Iterator<OSMDataset.Way> {
    private OSMLayer layer;
    private Node datasetNode;
    private Iterator<Node> wayNodeIterator;

    /**
     * This method is used to construct the dataset on an existing node when the node id is known,
     * which is the case with OSM importers.
     * 
     * @param spatialDatabase
     * @param osmLayer
     * @param layerNode
     * @param datasetId
     */
    public OSMDataset(SpatialDatabaseService spatialDatabase, OSMLayer osmLayer, Node layerNode, long datasetId) {
        this.layer = osmLayer;
        this.datasetNode = spatialDatabase.getDatabase().getNodeById(datasetId);
        Relationship rel = layerNode.getSingleRelationship(SpatialRelationshipTypes.LAYERS, Direction.INCOMING);
        if (rel == null) {
            Transaction tx = spatialDatabase.getDatabase().beginTx();
            try {
                datasetNode.createRelationshipTo(layerNode, SpatialRelationshipTypes.LAYERS);
                tx.success();
            } finally {
                tx.finish();
            }
        } else {
            Node node = rel.getStartNode();
            if (!node.equals(datasetNode)) {
                throw new SpatialDatabaseException("Layer '" + osmLayer + "' already belongs to another dataset: " + node);
            }
        }
    }

    /**
     * This method is used to construct the dataset when only the layer node is known, and the
     * dataset node needs to be searched for.
     * 
     * @param spatialDatabase2
     * @param osmLayer
     * @param layerNode
     */
    public OSMDataset(SpatialDatabaseService spatialDatabase, OSMLayer osmLayer, Node layerNode) {
        this.layer = osmLayer;
        Relationship rel = layerNode.getSingleRelationship(SpatialRelationshipTypes.LAYERS, Direction.INCOMING);
        if (rel == null) {
            throw new SpatialDatabaseException("Layer '" + osmLayer + "' does not have an associated dataset");
        } else {
            datasetNode = rel.getStartNode();
        }
    }
    
	public Iterable<Node> getAllUserNodes() {
		return datasetNode.traverse(Order.DEPTH_FIRST, StopEvaluator.END_OF_GRAPH, new ReturnableEvaluator(){

			public boolean isReturnableNode(TraversalPosition current) {
				Relationship rel = current.lastRelationshipTraversed();
				return rel != null && rel.isType(OSMRelation.OSM_USER);
			}},
				OSMRelation.USERS, Direction.OUTGOING, OSMRelation.OSM_USER, Direction.OUTGOING);
	}

	public Iterable<Node> getAllChangesetNodes() {
		return datasetNode.traverse(Order.DEPTH_FIRST, StopEvaluator.END_OF_GRAPH, new ReturnableEvaluator(){

			public boolean isReturnableNode(TraversalPosition current) {
				Relationship rel = current.lastRelationshipTraversed();
				return rel != null && rel.isType(OSMRelation.USER);
			}},
				OSMRelation.USERS, Direction.OUTGOING, OSMRelation.OSM_USER, Direction.OUTGOING, OSMRelation.USER, Direction.INCOMING);
	}

	public Iterable<Node> getAllWayNodes() {
		return datasetNode.traverse(Order.DEPTH_FIRST, StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE,
				OSMRelation.WAYS, Direction.OUTGOING, OSMRelation.NEXT, Direction.OUTGOING);
	}

	public Iterable<Node> getAllPointNodes() {
		return datasetNode.traverse(Order.DEPTH_FIRST, StopEvaluator.END_OF_GRAPH, new ReturnableEvaluator() {

			public boolean isReturnableNode(TraversalPosition current) {
				return current.lastRelationshipTraversed().isType(OSMRelation.NODE);
			}
		}, OSMRelation.WAYS, Direction.OUTGOING, OSMRelation.NEXT, Direction.OUTGOING, OSMRelation.FIRST_NODE, Direction.OUTGOING,
				OSMRelation.NODE, Direction.OUTGOING);
	}

	public Iterable<Node> getWayNodes(Node way) {
		return way.getSingleRelationship(OSMRelation.FIRST_NODE, Direction.OUTGOING).getEndNode()
				.traverse(Order.DEPTH_FIRST, StopEvaluator.END_OF_GRAPH, new ReturnableEvaluator() {

					public boolean isReturnableNode(TraversalPosition current) {
						Relationship lastRelationship = current.lastRelationshipTraversed();
						return lastRelationship != null && lastRelationship.isType(OSMRelation.NODE);
					}
				}, OSMRelation.NEXT, Direction.OUTGOING, OSMRelation.NODE, Direction.OUTGOING);
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
		Iterator<Node> results = nodeWayOrChangeset.traverse(Order.DEPTH_FIRST, StopEvaluator.END_OF_GRAPH,
				new ReturnableEvaluator() {

					public boolean isReturnableNode(TraversalPosition current) {
						Relationship lastRelationship = current.lastRelationshipTraversed();
						return lastRelationship != null && lastRelationship.isType(OSMRelation.USER);
					}
				}, OSMRelation.CHANGESET, Direction.OUTGOING, OSMRelation.USER, Direction.OUTGOING).iterator();
		return results.hasNext() ? results.next() : null;
	}

	public Way getWayFromId(long id) {
		return getWayFrom(datasetNode.getGraphDatabase().getNodeById(id));
	}

	public Way getWayFrom(Node osmNodeOrWayNodeOrGeomNode) {
		Iterator<Node> results = osmNodeOrWayNodeOrGeomNode.traverse(Order.DEPTH_FIRST, StopEvaluator.END_OF_GRAPH,
				new ReturnableEvaluator() {

					public boolean isReturnableNode(TraversalPosition current) {
						return current.currentNode().hasProperty("way_osm_id");
					}
				}, OSMRelation.NODE, Direction.INCOMING, OSMRelation.NEXT, Direction.INCOMING, OSMRelation.FIRST_NODE,
				Direction.INCOMING, OSMRelation.GEOM, Direction.INCOMING).iterator();
		return results.hasNext() ? new Way(results.next()) : null;
	}

	public class OSMNode {
		protected Node node;
		protected Node geomNode;
		protected Geometry geometry;

		public OSMNode(Node node) {
			this.node = node;
			Relationship geomRel = this.node.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING);
			if(geomRel != null) geomNode = geomRel.getEndNode();
		}
		
		public Way getWay() {
			return OSMDataset.this.getWayFrom(this.node);
		}

		public Node getGeometryNode() {
			return geomNode;
		}
		
		public Geometry getGeometry() {
			if(geometry == null && geomNode != null) {
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
		public Way(Node node) {
			super(node);
		}
		
		public Iterable<Node> getWayNodes() {
			return OSMDataset.this.getWayNodes(this.node);
		}
		
		public Iterable<WayPoint> getWayPoints() {
			return this;
		}

		public Iterator<WayPoint> iterator() {
			if(wayPointNodeIterator==null || !wayPointNodeIterator.hasNext()) {
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
		public WayPoint(Node node) {
			super(node);
		}

		public boolean isAt(Coordinate coord) {
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

	public Iterable< ? extends Geometry> getAllGeometries() {
        //@TODO: support multiple layers
        return layer.getAllGeometries();
    }

    public Iterable<Node> getAllGeometryNodes() {
        //@TODO: support multiple layers
        return layer.getAllGeometryNodes();
    }

    public GeometryEncoder getGeometryEncoder() {
        //@TODO: support multiple layers
        return layer.getGeometryEncoder();
    }

    public Iterable< ? extends Layer> getLayers() {
        return Arrays.asList(new Layer[]{layer});
    }

	public Iterable<Way> getWays() {
		return this;
	}

	public Iterator<Way> iterator() {
		if(wayNodeIterator==null || !wayNodeIterator.hasNext()) {
			wayNodeIterator = getAllWayNodes().iterator();
		}
		return this;
	}

	public boolean hasNext() {
		return wayNodeIterator.hasNext();
	}

	public Way next() {
		return new Way(wayNodeIterator.next());
	}

	public void remove() {
		throw new UnsupportedOperationException("Cannot modify way collection");
	}

	public int getPoiCount() {
		return (Integer) this.datasetNode.getProperty("poiCount", 0);
	}

	public int getNodeCount() {
		return (Integer) this.datasetNode.getProperty("nodeCount", 0);
	}

	public int getWayCount() {
		return (Integer) this.datasetNode.getProperty("wayCount", 0);
	}

	public int getRelationCount() {
		return (Integer) this.datasetNode.getProperty("relationCount", 0);
	}

	public int getChangesetCount() {
		return (Integer) this.datasetNode.getProperty("changesetCount", 0);
	}

	public int getUserCount() {
		return (Integer) this.datasetNode.getProperty("userCount", 0);
	}
}
