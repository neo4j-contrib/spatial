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
package org.neo4j.gis.spatial;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * Instances of Layer provide the ability for developers to add/remove and edit geometries
 * associated with a single dataset (or layer). This includes support for several storage
 * mechanisms, like in-node (geometries in properties) and sub-graph (geometries describe by the
 * graph). A Layer can be associated with a dataset. In cases where the dataset contains only one
 * layer, the layer itself is the dataset. See the class DefaultLayer for the standard
 * implementation of that pattern.
 */
public interface EditableLayer extends Layer {

	/**
	 * This method adds existing geometries to the layer for indexing. After this method is called the geometry should
	 * be searchable.
	 *
	 * @param geomNode the node containing the geometry to be added to the layer
	 * @return SpatialDatabaseRecord representation of the geometry added to the database
	 */
	SpatialDatabaseRecord add(Transaction tx, Node geomNode);

	/**
	 * This method adds existing geometries to the layer for indexing in bulk. After this method is called the geometry
	 * should be searchable.
	 *
	 * @param geomNodes the nodes containing the geometries to be added to the layer
	 * @return the number of geometries added to the database
	 */
	int addAll(Transaction tx, List<Node> geomNodes);

	/**
	 * Add a new geometry to the layer. This will add the geometry to the index.
	 */
	SpatialDatabaseRecord add(Transaction tx, Geometry geometry);

	/**
	 * Add a new geometry to the layer. This will add the geometry to the index.
	 *
	 * @param properties the properties to attach to the newly created node
	 */
	SpatialDatabaseRecord add(Transaction tx, Geometry geometry, @Nullable Map<String, Object> properties);

	/**
	 * Delete the geometry identified by the passed node id. This might be as simple as deleting the
	 * geometry node, or it might require extracting and deleting an entire sub-graph.
	 */
	void delete(Transaction tx, String geometryNodeId);

	/**
	 * Update the geometry identified by the passed node id. This might be as simple as changing
	 * node properties, or it might require editing an entire sub-graph.
	 */
	void update(Transaction tx, String geometryNodeId, Geometry geometry);

	void setCoordinateReferenceSystem(Transaction tx, CoordinateReferenceSystem coordinateReferenceSystem);

	void removeFromIndex(Transaction tx, String geomNodeId);

	/**
	 * Do any cleanup or final calculation required by the layer implementation.
	 */
	void finalizeTransaction(Transaction tx);
}
