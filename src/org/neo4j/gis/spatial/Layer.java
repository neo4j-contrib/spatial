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

import org.neo4j.gis.spatial.index.RTreeIndex;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;


/**
 * @author Davide Savazzi
 */
public class Layer implements Constants {

	// Public methods
	
	public void add(Geometry geometry, String[] fieldsName, Object[] fields) {
		Node geomNode = addGeomNode(geometry, fieldsName, fields);
		index.add(geomNode);
	}	
	
	public SpatialIndexReader getIndex() {
		return this.index;
	}

	
	// Private constructor

	protected Layer(GraphDatabaseService database, Node layerNode) {
		this.database = database;
		this.layerNodeId = layerNode.getId();
		this.index = new RTreeIndex(database, layerNodeId);
	}
	
	
	// Private methods
	
	private Node addGeomNode(Geometry geom, String[] fieldsName, Object[] fields) {
		if (geom.getNumGeometries() > 1) {
			// TODO
			throw new UnsupportedOperationException("multi geometries not yet supported");
		}
		
		Node geomNode = database.createNode();
		geomNode.setProperty(PROP_TYPE, encodeGeometryType(geom.getGeometryType()));

		Envelope mbb = geom.getEnvelopeInternal();				
		geomNode.setProperty(PROP_BBOX, new double[] { mbb.getMinX(), mbb.getMinY(), mbb.getMaxX(), mbb.getMaxY() });					
		
		Coordinate[] rawCoordinates = geom.getCoordinates();
		double[] coordinates = new double[rawCoordinates.length * 2];
		for (int i = 0; i < rawCoordinates.length; i++) {
			coordinates[i * 2] = rawCoordinates[i].x;
			coordinates[i * 2 + 1] = rawCoordinates[i].y;
		}
		geomNode.setProperty(PROP_ORDINATES, coordinates);

		// other properties
		for (int i = 0; i < fieldsName.length; i++) {
			geomNode.setProperty(fieldsName[i], fields[i]);
		}
		
		return geomNode;
	}
	
	private Integer encodeGeometryType(String jtsGeometryType) {
		if ("Point".equals(jtsGeometryType)) {
			return GTYPE_POINT;
		} else if ("LineString".equals(jtsGeometryType)) {
			return GTYPE_LINESTRING;
		} else if ("Polygon".equals(jtsGeometryType)) {
			return GTYPE_POLYGON;
		} else if ("MultiPoint".equals(jtsGeometryType)) {
			return GTYPE_MULTIPOINT;
		} else if ("MultiLineString".equals(jtsGeometryType)) {
			return GTYPE_MULTILINESTRING;
		} else if ("MultiPolygon".equals(jtsGeometryType)) {
			return GTYPE_MULTIPOLYGON;
		} else {
			throw new UnsupportedOperationException("unknown type:" + jtsGeometryType);
		}
	}
	
	
	// Attributes
	
	private GraphDatabaseService database;
	private long layerNodeId;
	private SpatialIndexWriter index;
}