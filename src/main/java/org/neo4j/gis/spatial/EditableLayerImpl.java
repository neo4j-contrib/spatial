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
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;

import com.vividsolutions.jts.geom.Geometry;

public class EditableLayerImpl extends DefaultLayer implements EditableLayer {
	private Node previousGeomNode;

	/**
	 * Add a geometry to this layer.
	 */
	public SpatialDatabaseRecord add(Geometry geometry) {
		return add(geometry, null, null);
	}

	/**
	 * Add a geometry to this layer, including properties.
	 */
	public SpatialDatabaseRecord add(Geometry geometry, String[] fieldsName, Object[] fields) {
		Transaction tx = getDatabase().beginTx();
		try {
			Node geomNode = addGeomNode(geometry, fieldsName, fields);
			index.add(geomNode);
			tx.success();
			return new SpatialDatabaseRecord(this, geomNode, geometry);
		} finally {
			tx.finish();
		}
	}

	public void update(long geomNodeId, Geometry geometry) {
		Transaction tx = getDatabase().beginTx();
		try {
			index.remove(geomNodeId, false);

			Node geomNode = getDatabase().getNodeById(geomNodeId);
			getGeometryEncoder().encodeGeometry(geometry, geomNode);
			index.add(geomNode);
			tx.success();
		} finally {
			tx.finish();
		}
	}

	public void delete(long geomNodeId) {
		Transaction tx = getDatabase().beginTx();
		try {
			index.remove(geomNodeId, true, false);
			tx.success();
		} finally {
			tx.finish();
		}
	}

    @Override
	public void removeFromIndex(long geomNodeId) {
		Transaction tx = getDatabase().beginTx();
		try {
            final boolean deleteGeomNode = false;
            index.remove(geomNodeId, deleteGeomNode, false);
			tx.success();
		} finally {
			tx.finish();
		}
	}

	private Node addGeomNode(Geometry geom, String[] fieldsName, Object[] fields) {
		Node geomNode = getDatabase().createNode();
		if (previousGeomNode == null) {
            TraversalDescription traversalDescription = Traversal.description().order( Traversal.postorderBreadthFirst() )
                    .relationships( SpatialRelationshipTypes.GEOMETRIES, Direction.INCOMING )
                    .relationships( SpatialRelationshipTypes.NEXT_GEOM, Direction.INCOMING )
                    .evaluator( Evaluators.excludeStartPosition() );

            for (Node node : createTraverserInBackwardsCompatibleWay( traversalDescription, layerNode ).nodes())
                        {
				previousGeomNode = node;
			}
		}
		if (previousGeomNode != null) {
			previousGeomNode.createRelationshipTo(geomNode, SpatialRelationshipTypes.NEXT_GEOM);
		} else {
			layerNode.createRelationshipTo(geomNode, SpatialRelationshipTypes.GEOMETRIES);
		}
		previousGeomNode = geomNode;
		// other properties
		if (fieldsName != null) {
			for (int i = 0; i < fieldsName.length; i++) {
				geomNode.setProperty(fieldsName[i], fields[i]);
			}
		}
		getGeometryEncoder().encodeGeometry(geom, geomNode);

		return geomNode;
	}

}
