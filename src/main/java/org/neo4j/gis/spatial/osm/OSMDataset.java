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

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.GeometryEncoder;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.SpatialDataset;
import org.neo4j.gis.spatial.SpatialRelationshipTypes;
import org.neo4j.gis.spatial.utilities.RelationshipTraversal;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;

public class OSMDataset implements SpatialDataset, Iterator<OSMDataset.Way> {

	private final OSMLayer layer;
	private final String datasetNodeId;
	private Iterator<Node> wayNodeIterator;

	public OSMDataset(OSMLayer layer, String datasetNodeId) {
		this.layer = layer;
		this.datasetNodeId = datasetNodeId;
		this.layer.setDataset(this);
	}

	/**
	 * This method is used to construct the dataset on an existing node when the node id is known,
	 * which is the case with OSM importers.
	 */
	public static OSMDataset withDatasetId(Transaction tx, OSMLayer layer, String datasetNodeId) {
		Node datasetNode = tx.getNodeByElementId(datasetNodeId);
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
		Relationship rel = layer.getLayerNode(tx)
				.getSingleRelationship(SpatialRelationshipTypes.LAYERS, Direction.INCOMING);
		if (rel == null) {
			throw new SpatialDatabaseException("Layer '" + layer + "' does not have an associated dataset");
		}
		String datasetNodeId = rel.getStartNode().getElementId();
		return new OSMDataset(layer, datasetNodeId);
	}

	public Iterable<Node> getAllUserNodes(Transaction tx) {
		TraversalDescription td = new MonoDirectionalTraversalDescription()
				.depthFirst()
				.relationships(OSMRelation.USERS, Direction.OUTGOING)
				.relationships(OSMRelation.OSM_USER, Direction.OUTGOING)
				.evaluator(Evaluators.includeWhereLastRelationshipTypeIs(OSMRelation.OSM_USER));
		return td.traverse(tx.getNodeByElementId(datasetNodeId)).nodes();
	}

	public Iterable<Node> getAllChangesetNodes(Transaction tx) {
		TraversalDescription td = new MonoDirectionalTraversalDescription()
				.depthFirst()
				.relationships(OSMRelation.USERS, Direction.OUTGOING)
				.relationships(OSMRelation.OSM_USER, Direction.OUTGOING)
				.relationships(OSMRelation.USER, Direction.INCOMING)
				.evaluator(Evaluators.includeWhereLastRelationshipTypeIs(OSMRelation.USER));
		return td.traverse(tx.getNodeByElementId(datasetNodeId)).nodes();
	}

	public Iterable<Node> getAllWayNodes(Transaction tx) {
		TraversalDescription td = new MonoDirectionalTraversalDescription()
				.depthFirst()
				.relationships(OSMRelation.WAYS, Direction.OUTGOING)
				.relationships(OSMRelation.NEXT, Direction.OUTGOING)
				.evaluator(Evaluators.excludeStartPosition());
		return td.traverse(tx.getNodeByElementId(datasetNodeId)).nodes();
	}

	public Iterable<Node> getAllPointNodes(Transaction tx) {
		TraversalDescription td = new MonoDirectionalTraversalDescription()
				.depthFirst()
				.relationships(OSMRelation.WAYS, Direction.OUTGOING)
				.relationships(OSMRelation.NEXT, Direction.OUTGOING)
				.relationships(OSMRelation.FIRST_NODE, Direction.OUTGOING)
				.relationships(OSMRelation.NODE, Direction.OUTGOING)
				.evaluator(Evaluators.includeWhereLastRelationshipTypeIs(OSMRelation.NODE));
		return td.traverse(tx.getNodeByElementId(datasetNodeId)).nodes();
	}

	public static Iterable<Node> getWayNodes(Node way) {
		TraversalDescription td = new MonoDirectionalTraversalDescription()
				.depthFirst()
				.relationships(OSMRelation.NEXT, Direction.OUTGOING)
				.relationships(OSMRelation.NODE, Direction.OUTGOING)
				.evaluator(Evaluators.includeWhereLastRelationshipTypeIs(OSMRelation.NODE));
		return td.traverse(
				way.getSingleRelationship(OSMRelation.FIRST_NODE, Direction.OUTGOING).getEndNode()
		).nodes();
	}

	public static Node getChangeset(Node way) {
		try {
			return way.getSingleRelationship(OSMRelation.CHANGESET, Direction.OUTGOING).getEndNode();
		} catch (Exception e) {
			System.out.println("Node has no changeset: " + e.getMessage());
			return null;
		}
	}

	public static Node getUser(Node nodeWayOrChangeset) {
		TraversalDescription td = new MonoDirectionalTraversalDescription()
				.depthFirst()
				.relationships(OSMRelation.CHANGESET, Direction.OUTGOING)
				.relationships(OSMRelation.USER, Direction.OUTGOING)
				.evaluator(Evaluators.includeWhereLastRelationshipTypeIs(OSMRelation.USER));
		return RelationshipTraversal.getFirstNode(td.traverse(nodeWayOrChangeset).nodes());
	}

	public Way getWayFromId(Transaction tx, String id) {
		return getWayFrom(tx.getNodeByElementId(id));
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
		Node node = RelationshipTraversal.getFirstNode(td.traverse(osmNodeOrWayNodeOrGeomNode).nodes());
		return node != null ? new Way(node) : null;
	}

	public class OSMNode {

		protected final Node node;
		protected Node geomNode;
		protected Geometry geometry;

		OSMNode(Node node) {
			this.node = node;
			Relationship geomRel = this.node.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING);
			if (geomRel != null) {
				geomNode = geomRel.getEndNode();
			}
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
			return Objects.equals(this.node.getElementId(), other.node.getElementId());
		}

		public Node getNode() {
			return node;
		}

		@Override
		public String toString() {
			if (node.hasProperty("name")) {
				return node.getProperty("name").toString();
			}
			if (getGeometry() != null) {
				return getGeometry().getGeometryType();
			}
			return node.toString();
		}
	}

	public class Way extends OSMNode implements Iterable<WayPoint>, Iterator<WayPoint> {

		private Iterator<Node> wayPointNodeIterator;

		Way(Node node) {
			super(node);
		}

		Iterable<Node> getWayNodes() {
			return OSMDataset.getWayNodes(this.node);
		}

		public Iterable<WayPoint> getWayPoints() {
			return this;
		}

		@Override
		@Nonnull
		public Iterator<WayPoint> iterator() {
			if (wayPointNodeIterator == null || !wayPointNodeIterator.hasNext()) {
				wayPointNodeIterator = getWayNodes().iterator();
			}
			return this;
		}

		@Override
		public boolean hasNext() {
			return wayPointNodeIterator.hasNext();
		}

		@Override
		public WayPoint next() {
			return new WayPoint(wayPointNodeIterator.next());
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Cannot modify way-point collection");
		}

		public WayPoint getPointAt(Coordinate coordinate) {
			for (WayPoint wayPoint : getWayPoints()) {
				if (wayPoint.isAt(coordinate)) {
					return wayPoint;
				}
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
		return (Integer) tx.getNodeByElementId(this.datasetNodeId).getProperty("poiCount", 0);
	}

	public int getNodeCount(Transaction tx) {
		return (Integer) tx.getNodeByElementId(this.datasetNodeId).getProperty("nodeCount", 0);
	}

	public int getWayCount(Transaction tx) {
		return (Integer) tx.getNodeByElementId(this.datasetNodeId).getProperty("wayCount", 0);
	}

	public int getRelationCount(Transaction tx) {
		return (Integer) tx.getNodeByElementId(this.datasetNodeId).getProperty("relationCount", 0);
	}

	public int getChangesetCount(Transaction tx) {
		return (Integer) tx.getNodeByElementId(this.datasetNodeId).getProperty("changesetCount", 0);
	}

	public int getUserCount(Transaction tx) {
		return (Integer) tx.getNodeByElementId(this.datasetNodeId).getProperty("userCount", 0);
	}
}
