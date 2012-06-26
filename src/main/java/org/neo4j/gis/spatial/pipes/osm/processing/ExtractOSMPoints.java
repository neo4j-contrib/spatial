/**
 * Copyright (c) 2010-2012 "Neo Technology,"
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
package org.neo4j.gis.spatial.pipes.osm.processing;

import static org.neo4j.gis.spatial.utilities.TraverserFactory.createTraverserInBackwardsCompatibleWay;

import org.neo4j.gis.spatial.osm.OSMRelation;
import org.neo4j.gis.spatial.pipes.AbstractExtractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.OrderedByTypeExpander;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class ExtractOSMPoints extends AbstractExtractGeoPipe {

	private GeometryFactory geomFactory;

	public ExtractOSMPoints(GeometryFactory geomFactory) {
		this.geomFactory = geomFactory;
	}
	
	@Override
	protected void extract(GeoPipeFlow pipeFlow) {
		Node geomNode = pipeFlow.getRecord().getGeomNode();
		Node node = geomNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode();

		TraversalDescription td = Traversal
			.description()
			.evaluator(new Evaluator() {
                @Override
                public Evaluation evaluate(Path path) {
                    if (path.length() > 0
                            && !path.relationships().iterator().next().isType(OSMRelation.NEXT)
                            && path.lastRelationship().isType(OSMRelation.NODE)) {
                        return Evaluation.INCLUDE_AND_PRUNE;
                    }

                    return Evaluation.EXCLUDE_AND_CONTINUE;
                }
            }).expand((RelationshipExpander) new OrderedByTypeExpander()
                        .add(OSMRelation.FIRST_NODE, Direction.OUTGOING)
                        .add(OSMRelation.NEXT, Direction.OUTGOING)
                        .add(OSMRelation.NODE, Direction.OUTGOING))
				.uniqueness(Uniqueness.NODE_PATH);
		
		int counter = 0;
		for (Path path : createTraverserInBackwardsCompatibleWay( td, node )) {
			Node pointNode = path.endNode();
			double longitude = (Double) pointNode.getProperty("lon");
			double latitude = (Double) pointNode.getProperty("lat");
			
			GeoPipeFlow newPoint = pipeFlow.makeClone("osmpoint" + counter++);
			newPoint.setGeometry(geomFactory.createPoint(new Coordinate(longitude, latitude)));
			extracts.add(newPoint);			
		}
	}
}
