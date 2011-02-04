/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.neo4j.graphdb.Node;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Geometry;


/**
 * @author Davide Savazzi
 */
public class SpatialDatabaseRecord implements Constants {

	public SpatialDatabaseRecord(Layer layer, Node geomNode) {
		this(layer, geomNode, null);
	}
	
	// Public methods
	
	public long getId() {
		return geomNode.getId();
	}
	
	public Node getGeomNode() {
		return geomNode;
	}
	
	/**
	 * This method returns a simple integer representation of the geometry. Some
	 * geometry encoders store this directly as a property of the geometry node,
	 * while others might store this information elsewhere in the graph, or
	 * deduce it from other factors of the data model. See the GeometryEncoder
	 * for information about mapping from the data model to the geometry.
	 * 
	 * @return integer representation of a geometry
	 * @deprecated This method is of questionable value, since it is better to
	 *             query the geometry object directly, outside the result
	 */
	public int getType() {
		//TODO: Get the type from the geometryEncoder
		return SpatialDatabaseService.convertJtsClassToGeometryType(getGeometry().getClass());
	}
	
	public Geometry getGeometry() {
		if (geometry == null)
			geometry = layer.getGeometryEncoder().decodeGeometry(geomNode);
		return geometry;
	}
	
	public CoordinateReferenceSystem getCoordinateReferenceSystem() {
		return layer.getCoordinateReferenceSystem();
	}
	
	public String getLayerName() {
		return layer.getName();
	}
	
	/**
	 * Not all geometry records have the same attribute set, so we should test
	 * for each specific record if it contains that property.
	 * 
	 * @param name
	 * @return
	 */
	public boolean hasProperty(String name) {
		return layer.getGeometryEncoder().hasAttribute(geomNode,name);
	}

	public String[] getPropertyNames() {
		return layer.getExtraPropertyNames();
	}
	
	public Object[] getPropertyValues() {
		String[] names = getPropertyNames();
		if (names == null) return null;
		Object[] values = new Object[names.length];
		for (int i = 0; i < names.length; i++) {
			values[i] = getProperty(names[i]);
		}
		return values;
	}

	public Object getProperty(String name) {
		return layer.getGeometryEncoder().getAttribute(geomNode,name);
	}
	
	public void setProperty(String name, Object value) {
		checkIsNotReservedProperty(name);
		geomNode.setProperty(name, value);
	}
	
	public int hashcode() {
		return ((Long) geomNode.getId()).hashCode();
	}
	
	public boolean equals(Object anotherObject) {
		if (!(anotherObject instanceof SpatialDatabaseRecord)) return false;
		
		SpatialDatabaseRecord anotherRecord = (SpatialDatabaseRecord) anotherObject;
		return getId() == anotherRecord.getId();
	}
	
	public String toString() {
	    return "SpatialDatabaseRecord[" + getId() + "]: type='" + getType() + "', props[" + getPropString() + "]";
	}

	
	// Protected Constructors
	
	protected SpatialDatabaseRecord(Layer layer, Node geomNode, Geometry geometry) {
		this.layer = layer;
		this.geomNode = geomNode;
		this.geometry = geometry;
	}

	// Private methods
	
	private void checkIsNotReservedProperty(String name) {
		for (String property : RESERVED_PROPS) {
			if (property.equals(name)) {
				throw new SpatialDatabaseException("Updating not allowed for Reserved Property: " + name);
			}
		}
	}
	
	private String getPropString() {
	    StringBuffer text = new StringBuffer();
	    for (String key : geomNode.getPropertyKeys()) {
	        if (text.length() > 0) text.append(", ");
            text.append(key).append(": ").append(geomNode.getProperty(key).toString());
	    }
	    return text.toString();
	}
	

	// Attributes
	
	private Node geomNode;
	private Geometry geometry;
	private Layer layer;

}