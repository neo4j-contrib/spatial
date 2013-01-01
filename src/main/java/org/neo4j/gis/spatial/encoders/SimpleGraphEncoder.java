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
package org.neo4j.gis.spatial.encoders;

import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser.Order;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Simple encoder that stores geometries as an linked list of point
 * nodes. Only supports LineString geometries.
 * 
 * @TODO: Consider generalizing this code and making a general linked
 *        list geometry store available in the library
 * @author craig
 */
public class SimpleGraphEncoder extends AbstractGeometryEncoder {
	private GeometryFactory geometryFactory;
	protected enum SimpleRelationshipTypes implements RelationshipType {
		FIRST, NEXT;
	}

	private GeometryFactory getGeometryFactory() {
		if(geometryFactory==null) geometryFactory = new GeometryFactory();
		return geometryFactory;
	}

	private Node testIsNode(PropertyContainer container) {
		if (!(container instanceof Node)) {
			throw new SpatialDatabaseException("Cannot decode non-node geometry: " + container);
		}
		return (Node) container;
	}

	@Override
	protected void encodeGeometryShape(Geometry geometry, PropertyContainer container) {
		Node node = testIsNode(container);
		node.setProperty("gtype", GTYPE_LINESTRING);
		Node prev = null;
		for (Coordinate coord : geometry.getCoordinates()) {
			Node point = node.getGraphDatabase().createNode();
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

	public Geometry decodeGeometry(PropertyContainer container) {
		Node node = testIsNode(container);
		CoordinateList coordinates = new CoordinateList();
		for (Node point : node.traverse(Order.DEPTH_FIRST, StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE,
		        SimpleRelationshipTypes.FIRST, Direction.OUTGOING, SimpleRelationshipTypes.NEXT, Direction.OUTGOING)) {
			coordinates.add(new Coordinate((Double) point.getProperty("x"), (Double) point.getProperty("y"), (Double) point.getProperty("z")), false);
		}
		return getGeometryFactory().createLineString(coordinates.toCoordinateArray());
	}
}