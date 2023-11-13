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
package org.neo4j.gis.spatial.encoders;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;

/**
 * Simple encoder that stores geometries as a linked list of point
 * nodes. Only supports LineString geometries.
 */
// TODO: Consider generalizing this code and making a general linked list geometry store available in the library
public class SimpleGraphEncoder extends AbstractGeometryEncoder {

	protected enum SimpleRelationshipTypes implements RelationshipType {
		FIRST, NEXT
	}

	private static Node testIsNode(Entity container) {
		if (!(container instanceof Node)) {
			throw new SpatialDatabaseException("Cannot decode non-node geometry: " + container);
		}
		return (Node) container;
	}

	@Override
	protected void encodeGeometryShape(Transaction tx, Geometry geometry, Entity container) {
		Node node = testIsNode(container);
		node.setProperty(PROP_TYPE, GTYPE_LINESTRING);
		Node prev = null;
		for (Coordinate coord : geometry.getCoordinates()) {
			Node point = tx.createNode();
			point.setProperty("x", coord.x);
			point.setProperty("y", coord.y);
			point.setProperty("z", coord.z);
			if (prev == null) {
				node.createRelationshipTo(point, SimpleRelationshipTypes.FIRST);
			} else {
				prev.createRelationshipTo(point, SimpleRelationshipTypes.NEXT);
			}
			prev = point;
		}
	}

	@Override
	public Geometry decodeGeometry(Entity container) {
		Node node = testIsNode(container);
		CoordinateList coordinates = new CoordinateList();
		TraversalDescription td = new MonoDirectionalTraversalDescription().depthFirst()
				.relationships(SimpleRelationshipTypes.FIRST, Direction.OUTGOING)
				.relationships(SimpleRelationshipTypes.NEXT, Direction.OUTGOING).breadthFirst()
				.evaluator(Evaluators.excludeStartPosition());
		for (Node point : td.traverse(node).nodes()) {
			coordinates.add(new Coordinate((Double) point.getProperty("x"), (Double) point.getProperty("y"),
					(Double) point.getProperty("z")), false);
		}
		return getGeometryFactory().createLineString(coordinates.toCoordinateArray());
	}
}
