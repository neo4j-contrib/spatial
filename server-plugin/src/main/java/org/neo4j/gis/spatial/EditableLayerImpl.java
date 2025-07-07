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

import static org.neo4j.gis.spatial.Constants.GTYPE_MULTIPOLYGON;
import static org.neo4j.gis.spatial.Constants.GTYPE_POINT;
import static org.neo4j.gis.spatial.Constants.PROP_CRS;
import static org.neo4j.gis.spatial.Constants.PROP_LAYERNODEEXTRAPROPS;
import static org.neo4j.gis.spatial.Constants.PROP_TYPE;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.index.SpatialIndexWriter;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class EditableLayerImpl extends DefaultLayer implements EditableLayer {

	protected SpatialIndexWriter indexWriter;

	@Override
	public void initialize(Transaction tx, IndexManager indexManager, String name, Node layerNode, boolean readOnly) {
		super.initialize(tx, indexManager, name, layerNode, readOnly);
		if (indexReader instanceof SpatialIndexWriter) {
			indexWriter = (SpatialIndexWriter) indexReader;
		} else {
			throw new SpatialDatabaseException("Index writer could not be initialized");
		}
	}

	/**
	 * Add the geometry encoded in the given Node. This causes the geometry to appear in the index.
	 */
	@Override
	public SpatialDatabaseRecord add(Transaction tx, Node geomNode) {
		checkWritable();
		Geometry geometry = getGeometryEncoder().decodeGeometry(geomNode);

		// add BBOX to Node if it's missing
		getGeometryEncoder().ensureIndexable(geometry, geomNode);

		indexWriter.add(tx, geomNode);
		return new SpatialDatabaseRecord(this, geomNode, geometry);
	}

	@Override
	public int addAll(Transaction tx, List<Node> geomNodes) {
		checkWritable();
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
		checkWritable();
		Node geomNode = addGeomNode(tx, geometry, properties);
		indexWriter.add(tx, geomNode);
		return new SpatialDatabaseRecord(this, geomNode, geometry);
	}

	@Override
	public void update(Transaction tx, String geomNodeId, Geometry geometry) {
		checkWritable();
		indexWriter.remove(tx, geomNodeId, false, true);
		Node geomNode = tx.getNodeByElementId(geomNodeId);
		getGeometryEncoder().encodeGeometry(tx, geometry, geomNode);
		indexWriter.add(tx, geomNode);
	}

	@Override
	public void delete(Transaction tx, String geomNodeId) {
		checkWritable();
		indexWriter.remove(tx, geomNodeId, true, false);
	}

	/**
	 * Delete Layer
	 */
	@Override
	public void delete(Transaction tx, Listener monitor) {
		checkWritable();
		indexWriter.removeAll(tx, true, monitor);
		Node layerNode = getLayerNode(tx);
		layerNode.delete();
		layerNodeId = null;
	}

	@Override
	public void removeFromIndex(Transaction tx, String geomNodeId) {
		checkWritable();
		final boolean deleteGeomNode = false;
		indexWriter.remove(tx, geomNodeId, deleteGeomNode, false);
	}

	protected Node addGeomNode(Transaction tx, Geometry geom, Map<String, Object> properties) {
		checkWritable();
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

	public void setCoordinateReferenceSystem(Transaction tx, CoordinateReferenceSystem crs) {
		checkWritable();
		Node layerNode = getLayerNode(tx);
		layerNode.setProperty(PROP_CRS, crs.toWKT());
	}

	public void setGeometryType(Transaction tx, int geometryType) {
		checkWritable();
		Node layerNode = getLayerNode(tx);
		if (geometryType < GTYPE_POINT || geometryType > GTYPE_MULTIPOLYGON) {
			throw new IllegalArgumentException("Unknown geometry type: " + geometryType);
		}

		layerNode.setProperty(PROP_TYPE, geometryType);
	}

	public void setExtraPropertyNames(String[] names, Transaction tx) {
		checkWritable();
		getLayerNode(tx).setProperty(PROP_LAYERNODEEXTRAPROPS, names);
	}

	void mergeExtraPropertyNames(Transaction tx, String[] names) {
		checkWritable();
		Node layerNode = getLayerNode(tx);
		if (layerNode.hasProperty(PROP_LAYERNODEEXTRAPROPS)) {
			String[] actualNames = (String[]) layerNode.getProperty(PROP_LAYERNODEEXTRAPROPS);

			Set<String> mergedNames = new HashSet<>();
			Collections.addAll(mergedNames, names);
			Collections.addAll(mergedNames, actualNames);

			layerNode.setProperty(PROP_LAYERNODEEXTRAPROPS, mergedNames.toArray(new String[0]));
		} else {
			layerNode.setProperty(PROP_LAYERNODEEXTRAPROPS, names);
		}
	}


	@Override
	public void finalizeTransaction(Transaction tx) {
		if (!isReadOnly()){
			getIndex().finalizeTransaction(tx);
		}
	}
}
