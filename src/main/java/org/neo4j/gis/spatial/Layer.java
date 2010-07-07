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

import java.util.HashSet;
import java.util.Set;

import org.geotools.factory.FactoryRegistryException;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
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
	
	public String getName() {
		return name;
	}

    /**
     *  Add a geometry to this layer.
     */	
	public SpatialDatabaseRecord add(Geometry geometry) {
		return add(geometry, null, null);
	}
	
    /**
     *  Add a geometry to this layer, including properties.
     */
	public SpatialDatabaseRecord add(Geometry geometry, String[] fieldsName, Object[] fields) {
		Node geomNode = addGeomNode(geometry, fieldsName, fields);
		index.add(geomNode);
		return new SpatialDatabaseRecord(getName(), getGeometryEncoder(), getCoordinateReferenceSystem(), getExtraPropertyNames(), geomNode, geometry);
	}	
	
	/**
	 * Add the geometry encoded in the given Node.
	 */
	public SpatialDatabaseRecord add(Node geomNode) {
		Geometry geometry = getGeometryEncoder().decodeGeometry(geomNode);		
		
		index.add(geomNode);
		return new SpatialDatabaseRecord(getName(), getGeometryEncoder(), getCoordinateReferenceSystem(), getExtraPropertyNames(), geomNode, geometry);
	}
	
	public void update(long geomNodeId, Geometry geometry) {
		index.remove(geomNodeId, false);
		
		Node geomNode = getDatabase().getNodeById(geomNodeId);
		getGeometryEncoder().encodeGeometry(geometry, geomNode);
		index.add(geomNode);
	}
	
	public void delete(long geomNodeId) {
		index.remove(geomNodeId, true);
	}
	
	public SpatialDatabaseService getSpatialDatabase() {
		return spatialDatabase;
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
	
	public GeometryEncoder getGeometryEncoder() {
		return geometryEncoder;
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

	public Integer guessGeometryType() {
		GuessGeometryTypeSearch geomTypeSearch = new GuessGeometryTypeSearch();
		index.executeSearch(geomTypeSearch);
		if (geomTypeSearch.firstFoundType != null) {
			return geomTypeSearch.firstFoundType;
		} else {
			// layer is empty
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

	protected Layer(SpatialDatabaseService spatialDatabase, String name, Node layerNode) {
		this.spatialDatabase = spatialDatabase;
		this.name = name;
		this.layerNodeId = layerNode.getId();
		this.index = new RTreeIndex(spatialDatabase.getDatabase(), this);
		
		// TODO read Precision Model and SRID from layer properties and use them to construct GeometryFactory
		this.geometryFactory = new GeometryFactory();
		
		if (layerNode.hasProperty(PROP_GEOMENCODER)) {
			String encoderClassName = (String) layerNode.getProperty(PROP_GEOMENCODER);
			try {
				this.geometryEncoder = (GeometryEncoder) Class.forName(encoderClassName).newInstance();
			} catch (Exception e) {
				throw new SpatialDatabaseException(e);
			}
		} else {
			this.geometryEncoder = new WKBGeometryEncoder();
		}
		this.geometryEncoder.init(this);
	}
	

	// Protected methods
	
	protected Node getLayerNode() {
		return getDatabase().getNodeById(layerNodeId);
	}
	
	protected long getLayerNodeId() {
		return layerNodeId;
	}
	
	/**
	 * Delete Layer
	 */
	protected void delete(Listener monitor) {
		index.removeAll(true, monitor);

		Transaction tx = getDatabase().beginTx();
		try {
			Node layerNode = getLayerNode();
			layerNode.getSingleRelationship(SpatialRelationshipTypes.LAYER, Direction.INCOMING).delete();
			layerNode.delete();
			
			tx.success();
		} finally {
			tx.finish();
		}
	}
	
	
	// Private methods
	
	private GraphDatabaseService getDatabase() {
		return spatialDatabase.getDatabase();
	}
	
	private Node addGeomNode(Geometry geom, String[] fieldsName, Object[] fields) {
		Node geomNode = getDatabase().createNode();
	
		// TODO: don't store node ids as properties of other nodes, rather use relationships, or layer name string
		// This seems to only be used by the FakeIndex to find all nodes in the layer. 
		// That is a bad solution, rather just traverse whatever graph the layer normally uses (mostly the r-tree, 
		// but without using r-tree intelligence)
		geomNode.setProperty(PROP_LAYER, layerNodeId);
		
		getGeometryEncoder().encodeGeometry(geom, geomNode);
		
		// other properties
		if (fieldsName != null) {
			for (int i = 0; i < fieldsName.length; i++) {
				geomNode.setProperty(fieldsName[i], fields[i]);
			}
		}
		
		return geomNode;
	}
	
	
	// Attributes
	
	private SpatialDatabaseService spatialDatabase;
	private String name;
	private long layerNodeId;
	private GeometryEncoder geometryEncoder;
	private GeometryFactory geometryFactory;
	private SpatialIndexWriter index;
	
	class GuessGeometryTypeSearch extends AbstractSearch {

		Integer firstFoundType;
			
		public boolean needsToVisit(Node indexNode) {
			return firstFoundType == null;
		}

		public void onIndexReference(Node geomNode) {
			if (firstFoundType == null) {
				firstFoundType = (Integer) geomNode.getProperty(PROP_TYPE);
			}
		}
	};

}