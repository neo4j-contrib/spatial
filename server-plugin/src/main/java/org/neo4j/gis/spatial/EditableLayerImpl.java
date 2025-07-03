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
import org.locationtech.jts.geom.Geometry;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class EditableLayerImpl extends DefaultLayer implements EditableLayer {

	/**
	 * Add the geometry encoded in the given Node. This causes the geometry to appear in the index.
	 */
	@Override
	public SpatialDatabaseRecord add(Transaction tx, Node geomNode) {
		Geometry geometry = getGeometryEncoder().decodeGeometry(geomNode);

		// add BBOX to Node if it's missing
		getGeometryEncoder().ensureIndexable(geometry, geomNode);

		indexWriter.add(tx, geomNode);
		return new SpatialDatabaseRecord(this, geomNode, geometry);
	}

	@Override
	public int addAll(Transaction tx, List<Node> geomNodes) {
		GeometryEncoder geometryEncoder = getGeometryEncoder();

		for (Node geomNode : geomNodes) {
			Geometry geometry = geometryEncoder.decodeGeometry(geomNode);
			// add BBOX to Node if it's missing
			geometryEncoder.encodeGeometry(tx, geometry, geomNode);
		}
		indexWriter.add(tx, geomNodes);
		return geomNodes.size();
	}

	/**
	 * Add a geometry to this layer.
	 */
	@Override
	public SpatialDatabaseRecord add(Transaction tx, Geometry geometry) {
		return add(tx, geometry, null);
	}

	/**
	 * Add a geometry to this layer, including properties.
	 */
	@Override
	public SpatialDatabaseRecord add(Transaction tx, Geometry geometry, Map<String, Object> properties) {
		Node geomNode = addGeomNode(tx, geometry, properties);
		indexWriter.add(tx, geomNode);
		return new SpatialDatabaseRecord(this, geomNode, geometry);
	}

	@Override
	public void update(Transaction tx, String geomNodeId, Geometry geometry) {
		indexWriter.remove(tx, geomNodeId, false, true);
		Node geomNode = tx.getNodeByElementId(geomNodeId);
		getGeometryEncoder().encodeGeometry(tx, geometry, geomNode);
		indexWriter.add(tx, geomNode);
	}

	@Override
	public void delete(Transaction tx, String geomNodeId) {
		indexWriter.remove(tx, geomNodeId, true, false);
	}

	@Override
	public void removeFromIndex(Transaction tx, String geomNodeId) {
		final boolean deleteGeomNode = false;
		indexWriter.remove(tx, geomNodeId, deleteGeomNode, false);
	}

	protected Node addGeomNode(Transaction tx, Geometry geom, Map<String, Object> properties) {
		Node geomNode = tx.createNode();
		if (properties != null) {
			properties.forEach(geomNode::setProperty);
		}
		getGeometryEncoder().encodeGeometry(tx, geom, geomNode);

		return geomNode;
	}

	@Override
	public String getSignature() {
		return "Editable" + super.getSignature();
	}

	@Override
	public void finalizeTransaction(Transaction tx) {
		getIndex().finalizeTransaction(tx);
	}
}
