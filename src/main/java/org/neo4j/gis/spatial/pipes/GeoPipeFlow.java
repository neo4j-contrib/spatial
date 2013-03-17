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
package org.neo4j.gis.spatial.pipes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialRecord;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;


public class GeoPipeFlow implements SpatialRecord {

	private String id;
	private List<SpatialDatabaseRecord> records = new ArrayList<SpatialDatabaseRecord>();
	private Geometry geometry;
	private Envelope geometryEnvelope;
	private Map<String,Object> properties = new HashMap<String,Object>();
	
	private GeoPipeFlow(String id) {
		this.id = id;
	}
	
	public GeoPipeFlow(SpatialDatabaseRecord record) {
		this.id = Long.toString(record.getNodeId());
		this.records.add(record);
		this.geometry = record.getGeometry();
	}
	
	public SpatialDatabaseRecord getRecord() {
		return records.get(0);
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
	
	public Map<String,Object> getProperties() {
		return properties;
	}
	
	public boolean hasProperty(String name) {
		return properties.containsKey(name);
	}

	public String[] getPropertyNames() {
		return properties.keySet().toArray(new String[] {});
	}
	
	@Override
	public Object getProperty(String name) {
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