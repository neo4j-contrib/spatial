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
package org.neo4j.gis.spatial.pipes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialRecord;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class GeoPipeFlow implements SpatialRecord {

	private final String id;
	private final List<SpatialDatabaseRecord> records = new ArrayList<>();
	private Geometry geometry;
	private Envelope geometryEnvelope;
	private final Map<String, Object> properties = new HashMap<>();

	private GeoPipeFlow(String id) {
		this.id = id;
	}

	public GeoPipeFlow(SpatialDatabaseRecord record) {
		this.id = record.getNodeId();
		this.records.add(record);
		this.geometry = record.getGeometry();
	}

	public SpatialDatabaseRecord getRecord() {
		return records.get(0);
	}

	@Override
	public Node getGeomNode() {
		return getRecord().getGeomNode();
	}

	public int countRecords() {
		return records.size();
	}

	public List<SpatialDatabaseRecord> getRecords() {
		return records;
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public Geometry getGeometry() {
		return geometry;
	}

	public Envelope getEnvelope() {
		if (geometryEnvelope == null) {
			geometryEnvelope = geometry.getEnvelopeInternal();
		}

		return geometryEnvelope;
	}

	public void setGeometry(Geometry geometry) {
		this.geometry = geometry;
		this.geometryEnvelope = null;
	}

	@Override
	public Map<String, Object> getProperties(Transaction ignored) {
		return properties;
	}

	// Alternative method since GeoPipes never work within a transactional context
	public Map<String, Object> getProperties() {
		return properties;
	}

	@Override
	public boolean hasProperty(Transaction tx, String name) {
		return properties.containsKey(name);
	}

	@Override
	public String[] getPropertyNames(Transaction tx) {
		return properties.keySet().toArray(new String[]{});
	}

	@Override
	public Object getProperty(Transaction ignored, String name) {
		return properties.get(name);
	}

	public void merge(GeoPipeFlow other) {
		records.addAll(other.records);
		// TODO id?
		// TODO properties?
	}

	public GeoPipeFlow makeClone(String idSuffix) {
		// we don't need a deeper copy at the moment
		GeoPipeFlow clone = new GeoPipeFlow(id + "-" + idSuffix);
		clone.records.addAll(records);
		clone.geometry = geometry;
		clone.getProperties().putAll(getProperties());
		return clone;
	}
}
