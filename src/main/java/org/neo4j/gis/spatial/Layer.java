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

import org.geotools.factory.FactoryRegistryException;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Instances of Layer provide the ability for developers to add/remove and edit geometries
 * associated with a single dataset (or layer). This includes support for several storage
 * mechanisms, like in-node (geometries in properties) and sub-graph (geometries describe by the
 * graph).
 * 
 * @author Davide Savazzi
 */
public class Layer implements Constants {

	// Public methods

//	public long add(Geometry geometry) {
//		return add(geometry, null, null);
//	}
	
    /**
     *  Add a geometry to this layer, including properties.
     */
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

	public void setCoordinateReferenceSystem(CoordinateReferenceSystem crs) {
		Node layerNode = getLayerNode();
		layerNode.setProperty(PROP_CRS, crs.toWKT());
	}	
	
	public CoordinateReferenceSystem getCoordinateReferenceSystem() {
		Node layerNode = getLayerNode();
		if (layerNode.hasProperty(PROP_CRS)) {
			try {
				return ReferencingFactoryFinder.getCRSFactory(null).createFromWKT((String) layerNode.getProperty(PROP_CRS));
			} catch (FactoryRegistryException e) {
				throw new SpatialDatabaseException(e);
			} catch (FactoryException e) {
				throw new SpatialDatabaseException(e);
			}
		} else {
			return null;
		}
	}
	
	public void setGeometryType(Integer geometryType) {
		Node layerNode = getLayerNode();
		if (geometryType != null) {
			if (geometryType.intValue() < GTYPE_POINT || geometryType.intValue() > GTYPE_MULTIPOLYGON) {
				throw new IllegalArgumentException("Unknown geometry type: " + geometryType);
			}
			
			layerNode.setProperty(PROP_TYPE, geometryType);
		} else {
			layerNode.removeProperty(PROP_TYPE);
		}
	}
	
	public Integer getGeometryType() {
		Node layerNode = getLayerNode();
		if (layerNode.hasProperty(PROP_TYPE)) {
			return (Integer) layerNode.getProperty(PROP_TYPE);
		} else {
			return null;
		}
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
	
	/**
	 * Delete Layer
	 */
	protected void delete() {
		index.deleteAll();
		
		Node layerNode = getLayerNode();
		layerNode.getSingleRelationship(SpatialRelationshipTypes.LAYER, Direction.INCOMING).delete();
		layerNode.delete();
	}
	
	
	// Private methods
	
	private Node addGeomNode(Geometry geom, String[] fieldsName, Object[] fields) {
		Node geomNode = database.createNode();
		//TODO: don't store node ids as properties of other nodes, rather use relationships, or layer name string
		//This seems to only be used by the FakeIndex to find all nodes in the layer. THat is a bad solution, rather just traverse whatever graph the layer normally uses (mostly the r-tree, but without using r-tree intelligence)
		geomNode.setProperty(PROP_LAYER, layerNodeId);
		encode(geom, geomNode);
		
		// other properties
		if (fieldsName != null) {
			for (int i = 0; i < fieldsName.length; i++) {
				geomNode.setProperty(fieldsName[i], fields[i]);
			}
		}
		
		return geomNode;
	}
	
	
	// Attributes
	
	private GraphDatabaseService database;
	private long layerNodeId;
	private GeometryFactory geometryFactory;
	private SpatialIndexWriter index;
}