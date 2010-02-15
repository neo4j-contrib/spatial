/*
 * Copyright (c) 2010
 *
 * This program is free software: you can redistribute it and/or modify
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

import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import static org.neo4j.gis.spatial.GeometryUtils.decode;


/**
 * @author Davide Savazzi
 */
public class SpatialDatabaseRecord implements Constants {

	// Public methods
	
	public long getId() {
		return geomNode.getId();
	}
	
	public int getType() {
		return (Integer) geomNode.getProperty(PROP_TYPE);
	}
	
	public Geometry getGeometry() {
		if (geometry == null) {
			geometry = decode(geomNode, geometryFactory);
			geometryFactory = null;
		}
		return geometry;
	}
	
	public Object getProperty(String name) {
		return geomNode.getProperty(name);
	}
	
	public int hashcode() {
		return ((Long) geomNode.getId()).hashCode();
	}
	
	public boolean equals(Object anotherObject) {
		if (!(anotherObject instanceof SpatialDatabaseRecord)) return false;
		
		SpatialDatabaseRecord anotherRecord = (SpatialDatabaseRecord) anotherObject;
		return getId() == anotherRecord.getId();
	}
	
	
	// Protected constructor
	
	protected SpatialDatabaseRecord(Node geomNode, GeometryFactory geometryFactory) {
		this.geomNode = geomNode;
	}

	protected SpatialDatabaseRecord(Node geomNode, Geometry geometry) {
		this.geomNode = geomNode;
		this.geometry = geometry;
	}
	

	// Attributes
	
	private Node geomNode;
	private GeometryFactory geometryFactory;
	private Geometry geometry;
}