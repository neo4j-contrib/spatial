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
package org.neo4j.gis.spatial;

import static org.neo4j.gis.spatial.utilities.TraverserFactory.createTraverserInBackwardsCompatibleWay;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;

import com.vividsolutions.jts.geom.Geometry;

public class OrderedEditableLayer extends EditableLayerImpl {
	private Node previousGeomNode;

	protected Node addGeomNode(Geometry geom, String[] fieldsName, Object[] fields) {
		Node geomNode = super.addGeomNode(geom, fieldsName, fields);
		if (previousGeomNode == null) {
			TraversalDescription traversalDescription = Traversal.description().order(Traversal.postorderBreadthFirst())
					.relationships(SpatialRelationshipTypes.GEOMETRIES, Direction.INCOMING)
					.relationships(SpatialRelationshipTypes.NEXT_GEOM, Direction.INCOMING)
					.evaluator(Evaluators.excludeStartPosition());
			for (Node node : createTraverserInBackwardsCompatibleWay(traversalDescription, layerNode).nodes()) {
				previousGeomNode = node;
			}
		}
		if (previousGeomNode != null) {
			previousGeomNode.createRelationshipTo(geomNode, SpatialRelationshipTypes.NEXT_GEOM);
		} else {
			layerNode.createRelationshipTo(geomNode, SpatialRelationshipTypes.GEOMETRIES);
		}
		previousGeomNode = geomNode;
		return geomNode;
	}

}
