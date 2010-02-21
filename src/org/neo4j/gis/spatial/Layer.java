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

import static org.neo4j.gis.spatial.GeometryUtils.encode;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;


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
	
	public void update(long geomNodeId, Geometry geometry) {
		index.delete(geomNodeId, false);
		
		Node geomNode = database.getNodeById(geomNodeId);		
		encode(geometry, geomNode);
		index.add(geomNode);
	}
	
	public void delete(long geomNodeId) {
		index.delete(geomNodeId, true);
	}
	
	public SpatialIndexReader getIndex() {
		return index;
	}

	public GeometryFactory getGeometryFactory() {
		return geometryFactory;
	}
	
	public String[] getExtraPropertyNames() {
		Node layerNode = getLayerNode();
		if (layerNode.hasProperty(PROP_LAYERNODEEXTRAPROPS)) {
			return (String[]) layerNode.getProperty(PROP_LAYERNODEEXTRAPROPS);
		} else {
			return new String[] {};
		}
	}
	
	public void setExtraPropertyNames(String[] names) {
		getLayerNode().setProperty(PROP_LAYERNODEEXTRAPROPS, names);
	}
	
	public void mergeExtraPropertyNames(String[] names) {
		Node layerNode = getLayerNode();
		if (layerNode.hasProperty(PROP_LAYERNODEEXTRAPROPS)) {
			String[] actualNames = (String[]) layerNode.getProperty(PROP_LAYERNODEEXTRAPROPS);
			
			Set<String> mergedNames = new HashSet<String>();
			for (String name : names) mergedNames.add(name);
			for (String name : actualNames) mergedNames.add(name);

			layerNode.setProperty(PROP_LAYERNODEEXTRAPROPS, (String[]) mergedNames.toArray(new String[mergedNames.size()]));
		} else {
			layerNode.setProperty(PROP_LAYERNODEEXTRAPROPS, names);
		}
	}
	
	
	// Protected constructor

	protected Layer(GraphDatabaseService database, Node layerNode) {
		this.database = database;
		this.layerNodeId = layerNode.getId();
		this.index = new RTreeIndex(database, this);
		
		// TODO read Precision Model and SRID from layer properties and use them to construct GeometryFactory
		this.geometryFactory = new GeometryFactory();
	}
	

	// Protected methods
	
	protected Node getLayerNode() {
		return database.getNodeById(layerNodeId);
	}
	
	protected long getLayerNodeId() {
		return layerNodeId;
	}
	
	
	// Private methods
	
	private Node addGeomNode(Geometry geom, String[] fieldsName, Object[] fields) {
		Node geomNode = database.createNode();
		geomNode.setProperty(PROP_LAYER, layerNodeId);
		encode(geom, geomNode);
		
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