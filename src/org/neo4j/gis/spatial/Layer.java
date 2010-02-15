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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import static org.neo4j.gis.spatial.GeometryUtils.encode;


/**
 * @author Davide Savazzi
 */
public class Layer implements Constants {

	// Public methods
	
	public long add(Geometry geometry, String[] fieldsName, Object[] fields) {
		Node geomNode = addGeomNode(geometry, fieldsName, fields);
		index.add(geomNode);
		return geomNode.getId();
	}	
	
	public SpatialIndexReader getIndex() {
		return index;
	}

	public GeometryFactory getGeometryFactory() {
		return geometryFactory;
	}
	
	
	// Private constructor

	protected Layer(GraphDatabaseService database, Node layerNode) {
		this.database = database;
		this.layerNodeId = layerNode.getId();
		this.index = new RTreeIndex(database, layerNodeId);
		
		// TODO read Precision Model and SRID from layer properties and use them to construct GeometryFactory
		this.geometryFactory = new GeometryFactory();
	}
	
	
	// Private methods
	
	private Node addGeomNode(Geometry geom, String[] fieldsName, Object[] fields) {
		Node geomNode = database.createNode();
		encode(geom, database.createNode());
		
		// other properties
		for (int i = 0; i < fieldsName.length; i++) {
			geomNode.setProperty(fieldsName[i], fields[i]);
		}
		
		return geomNode;
	}
	
	
	// Attributes
	
	private GraphDatabaseService database;
	private long layerNodeId;
	private GeometryFactory geometryFactory;
	private SpatialIndexWriter index;
}