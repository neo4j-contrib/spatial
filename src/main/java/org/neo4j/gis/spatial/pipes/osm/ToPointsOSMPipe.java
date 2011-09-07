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
package org.neo4j.gis.spatial.pipes.osm;

import java.util.Iterator;

import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.osm.OSMRelation;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.OrderedByTypeExpander;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

import com.tinkerpop.pipes.AbstractPipe;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class ToPointsOSMPipe<S, E> extends
		AbstractPipe<SpatialDatabaseRecord, Point> {

	private Iterator<Path> nodeIterator = null;

	public Point processNextStart() {
		while (true) {

			if (nodeIterator == null || !nodeIterator.hasNext()) {
				final SpatialDatabaseRecord record = this.starts.next();

				Node geomNode = record.getGeomNode();
				Node node = geomNode.getSingleRelationship(OSMRelation.GEOM,
						Direction.INCOMING).getStartNode();

				TraversalDescription td = Traversal
						.description()
						.evaluator(new Evaluator() {

							@Override
							public Evaluation evaluate(Path path) {
								if (path.length() > 0
										&& !path.relationships().iterator()
												.next()
												.isType(OSMRelation.NEXT)
										&& path.lastRelationship().isType(
												OSMRelation.NODE)) {
									return Evaluation.INCLUDE_AND_PRUNE;
								}
								return Evaluation.EXCLUDE_AND_CONTINUE;
							}
						})
						.expand(new OrderedByTypeExpander()
								.add(OSMRelation.FIRST_NODE, Direction.OUTGOING)
								.add(OSMRelation.NEXT, Direction.OUTGOING)
								.add(OSMRelation.NODE, Direction.OUTGOING))
						.uniqueness(Uniqueness.NODE_PATH);
				nodeIterator = td.traverse(node).iterator();
			}

			Path path = nodeIterator.next();
			Node pointNode = path.endNode();
			double longitude = (Double) pointNode.getProperty("lon");
			double latitude = (Double) pointNode.getProperty("lat");
			Coordinate coordinate = new Coordinate(longitude, latitude);
			GeometryFactory geometryFactory = new GeometryFactory();
			return geometryFactory.createPoint(coordinate);

		}

	}
}
