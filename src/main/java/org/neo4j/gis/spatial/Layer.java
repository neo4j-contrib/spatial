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
import java.util.Iterator;
import java.util.Set;

import org.geotools.factory.FactoryRegistryException;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Traverser.Order;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Instances of Layer provide the ability for developers to add/remove and edit geometries
 * associated with a single dataset (or layer). This includes support for several storage
 * mechanisms, like in-node (geometries in properties) and sub-graph (geometries describe by the
 * graph). A Layer can be associated with a dataset. In cases where the dataset contains only one
 * layer, the layer itself is the dataset.
 * 
 * @author Davide Savazzi
 */
public class Layer implements Constants, SpatialDataset {

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
	
	public Integer getOrGuessGeometryType() {
		Integer geomType = getGeometryType();
		if (geomType == null) geomType = guessGeometryType();
		return geomType;
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
    protected Layer() {
    }
    
    /**
     * Factory method to construct a layer from an existing layerNode. This will read the layer
     * class from the layer node properties and construct the correct class from that.
     * 
     * @param spatialDatabase
     * @param layerNode
     * @return new layer instance from existing layer node
     */
    protected static Layer makeLayer(SpatialDatabaseService spatialDatabase, Node layerNode) {
        try {
            String name = (String) layerNode.getProperty(PROP_LAYER);
            if (name == null) {
                return null;
            }
            
            String className = null;
            if (layerNode.hasProperty(PROP_LAYER_CLASS)) {
            	className = (String) layerNode.getProperty(PROP_LAYER_CLASS);
            }
            
            Class<? extends Layer> layerClass = className == null ? Layer.class : (Class<? extends Layer>) Class.forName(className);
            return makeLayer(spatialDatabase, name, layerNode, layerClass);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Factory method to construct a layer with the specified layer class. This can be used when
     * creating a layer for the first time.
     * 
     * @param spatialDatabase
     * @param name
     * @param layerClass
     * @return new Layer instance based on newly created layer Node
     */
    protected static Layer makeLayer(SpatialDatabaseService spatialDatabase, String name,
            Class< ? extends GeometryEncoder> geometryEncoderClass, Class< ? extends Layer> layerClass) {
        try {
            Node layerNode = spatialDatabase.getDatabase().createNode();
            layerNode.setProperty(PROP_LAYER, name);
            layerNode.setProperty(PROP_CREATIONTIME, System.currentTimeMillis());
            layerNode.setProperty(PROP_GEOMENCODER, geometryEncoderClass.getCanonicalName());
            layerNode.setProperty(PROP_LAYER_CLASS, layerClass.getCanonicalName());
            return Layer.makeLayer(spatialDatabase, name, layerNode, layerClass);
        } catch (Exception e) {
            throw (RuntimeException)new RuntimeException().initCause(e);
        }
    }

    private static Layer makeLayer(SpatialDatabaseService spatialDatabase, String name, Node layerNode, Class<? extends Layer> layerClass) throws InstantiationException, IllegalAccessException {
        if(layerClass == null) layerClass = Layer.class;
        Layer layer = layerClass.newInstance();
        layer.initialize(spatialDatabase, name, layerNode);
        return layer;
    }

	protected void initialize(SpatialDatabaseService spatialDatabase, String name, Node layerNode) {
		this.spatialDatabase = spatialDatabase;
		this.name = name;
		this.layerNode = layerNode;
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
		return layerNode;
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
		if(lastGeomNode!=null) {
		    lastGeomNode.createRelationshipTo(geomNode, DynamicRelationshipType.withName("NEXT_GEOM"));
		}else{
		    layerNode.createRelationshipTo(geomNode, DynamicRelationshipType.withName("GEOMETRIES"));
		}
	
		// TODO: don't store node ids as properties of other nodes, rather use relationships, or layer name string
		// This seems to only be used by the FakeIndex to find all nodes in the layer. 
		// That is a bad solution, rather just traverse whatever graph the layer normally uses (mostly the r-tree, 
		// but without using r-tree intelligence)
//		geomNode.setProperty(PROP_LAYER, layerNode.getId());
		
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
	private Node layerNode;
	private Node lastGeomNode;
	protected GeometryEncoder geometryEncoder;
	protected GeometryFactory geometryFactory;
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
	}

    public SpatialDataset getDataset() {
        return this;
    };

    /**
     * Provides a method for iterating over all nodes that represent geometries in this dataset.
     * This is similar to the getAllNodes() methods from GraphDatabaseService but will only return
     * nodes that this dataset considers its own, and can be passed to the GeometryEncoder to
     * generate a Geometry. There is no restricting on a node belonging to multiple datasets, or
     * multiple layers within the same dataset.
     * 
     * @return iterable over geometry nodes in the dataset
     */
    public Iterable<Node> getAllGeometryNodes() {
        return layerNode.traverse(Order.DEPTH_FIRST, StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE,
                SpatialRelationshipTypes.GEOMETRIES, Direction.OUTGOING, SpatialRelationshipTypes.NEXT_GEOM, Direction.OUTGOING);
    }

    /**
     * Provides a method for iterating over all geometries in this dataset. This is similar to the
     * getAllGeometryNodes() method but internally converts the Node to a Geometry.
     * 
     * @return iterable over geometries in the dataset
     */
    public Iterable<? extends Geometry> getAllGeometries() {
        return new NodeToGeometryIterable(getAllGeometryNodes());
    }
    
    /**
     * In order to wrap one iterable or iterator in another that converts the objects from one type
     * to another without loading all into memory, we need to use this ugly java-magic. Man, I miss
     * Ruby right now!
     * 
     * @author craig
     * @since 1.0.0
     */
    private class NodeToGeometryIterable implements Iterable<Geometry>  {
        private Iterator<Node> allGeometryNodeIterator;
        private class GeometryIterator implements Iterator<Geometry> {

            public boolean hasNext() {
                return NodeToGeometryIterable.this.allGeometryNodeIterator.hasNext();
            }

            public Geometry next() {
                return geometryEncoder.decodeGeometry(NodeToGeometryIterable.this.allGeometryNodeIterator.next());
            }

            public void remove() {
            }
            
        }
        public NodeToGeometryIterable(Iterable<Node> allGeometryNodes) {
            this.allGeometryNodeIterator = allGeometryNodes.iterator();
        }

        public Iterator<Geometry> iterator() {
            return new GeometryIterator();
        }
        
    }

    /**
     * Return the geometry encoder used by this SpatialDataset to convert individual geometries to
     * and from the database structure.
     * 
     * @return GeometryEncoder for this dataset
     */
    public GeometryEncoder getGeometryEncoder() {
        return geometryEncoder;
    }
}