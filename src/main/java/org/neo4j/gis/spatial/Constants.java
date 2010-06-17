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


/**
 * @author Davide Savazzi
 */
public interface Constants {

	// Node properties
	
	String PROP_LAYER = "layer";
	String PROP_LAYERNODEEXTRAPROPS = "layerprops";
	String PROP_CRS = "layercrs";
	String PROP_CREATIONTIME = "ctime";
	String PROP_GEOMENCODER = "geomencoder";
	
	String PROP_TYPE = "gtype";
	String PROP_BBOX = "bbox";
	String PROP_WKB = "wkb";
	String PROP_WKT = "wkt";
	
	String[] RESERVED_PROPS = new String[] { PROP_LAYER, PROP_LAYERNODEEXTRAPROPS, PROP_CREATIONTIME, PROP_TYPE, PROP_BBOX, PROP_WKB };
	
	
	// OpenGIS geometry type numbers 
	
	int GTYPE_POINT = 1;
	int GTYPE_LINESTRING = 2; 
	int GTYPE_POLYGON = 3;
	int GTYPE_MULTIPOINT = 4; 	
	int GTYPE_MULTILINESTRING = 5; 
	int GTYPE_MULTIPOLYGON = 6; 
	
}