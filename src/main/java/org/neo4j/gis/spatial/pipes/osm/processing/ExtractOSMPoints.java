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
package org.neo4j.gis.spatial.pipes.osm.processing;

import static org.neo4j.gis.spatial.utilities.TraverserFactory.createTraverserInBackwardsCompatibleWay;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.neo4j.gis.spatial.osm.OSMRelation;
import org.neo4j.gis.spatial.pipes.AbstractExtractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.impl.OrderedByTypeExpander;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;

public class ExtractOSMPoints extends AbstractExtractGeoPipe {

	private final GeometryFactory geomFactory;

	public ExtractOSMPoints(GeometryFactory geomFactory) {
		this.geomFactory = geomFactory;
	}

	@Override
	protected void extract(GeoPipeFlow pipeFlow) {
		Node geomNode = pipeFlow.getRecord().getGeomNode();
		Node node = geomNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode();

		TraversalDescription td = new MonoDirectionalTraversalDescription().evaluator(path -> {
					if (path.length() > 0
							&& !path.relationships().iterator().next().isType(OSMRelation.NEXT)
							&& path.lastRelationship().isType(OSMRelation.NODE)) {
						return Evaluation.INCLUDE_AND_PRUNE;
					}

					return Evaluation.EXCLUDE_AND_CONTINUE;
				}).expand(new OrderedByTypeExpander()
						.add(OSMRelation.FIRST_NODE, Direction.OUTGOING)
						.add(OSMRelation.NEXT, Direction.OUTGOING)
						.add(OSMRelation.NODE, Direction.OUTGOING))
				.uniqueness(Uniqueness.NODE_PATH);

		int counter = 0;
		for (Path path : createTraverserInBackwardsCompatibleWay(td, node)) {
			Node pointNode = path.endNode();
			double longitude = (Double) pointNode.getProperty("lon");
			double latitude = (Double) pointNode.getProperty("lat");

			GeoPipeFlow newPoint = pipeFlow.makeClone("osmpoint" + counter++);
			newPoint.setGeometry(geomFactory.createPoint(new Coordinate(longitude, latitude)));
			extracts.add(newPoint);
		}
	}
}
