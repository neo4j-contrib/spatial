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

import org.locationtech.jts.geom.Geometry;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class EditableLayerImpl extends DefaultLayer implements EditableLayer {

	/**
	 * Add a geometry to this layer.
	 */
	@Override
	public SpatialDatabaseRecord add(Transaction tx, Geometry geometry) {
		return add(tx, geometry, null, null);
	}

	/**
	 * Add a geometry to this layer, including properties.
	 */
	@Override
	public SpatialDatabaseRecord add(Transaction tx, Geometry geometry, String[] fieldsName, Object[] fields) {
		Node geomNode = addGeomNode(tx, geometry, fieldsName, fields);
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

	protected Node addGeomNode(Transaction tx, Geometry geom, String[] fieldsName, Object[] fields) {
		Node geomNode = tx.createNode();
		// other properties
		if (fieldsName != null) {
			for (int i = 0; i < fieldsName.length; i++) {
				if (fieldsName[i] != null && fields[i] != null) {
					geomNode.setProperty(fieldsName[i], fields[i]);
				}
			}
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
