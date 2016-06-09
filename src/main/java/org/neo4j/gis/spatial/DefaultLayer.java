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
package org.neo4j.gis.spatial;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.geotools.factory.FactoryRegistryException;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.attributes.PropertyMappingManager;
import org.neo4j.gis.spatial.encoders.Configurable;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Instances of Layer provide the ability for developers to add/remove and edit
 * geometries associated with a single dataset (or layer). This includes support
 * for several storage mechanisms, like in-node (geometries in properties) and
 * sub-graph (geometries describe by the graph). A Layer can be associated with
 * a dataset. In cases where the dataset contains only one layer, the layer
 * itself is the dataset.
 * 
 * You should not construct the DefaultLayer directly, but use the included
 * factor methods for creating layers based on configurations. This will
 * instantiate the appropriate class correctly. See the methods
 * makeLayerFromNode and makeLayerInstance.
 */
public class DefaultLayer implements Constants, Layer, SpatialDataset {

    // Public methods
    
    public String getName() {
        return name;
    }

    public SpatialDatabaseService getSpatialDatabase() {
        return spatialDatabase;
    }
    
    public LayerIndexReader getIndex() {
        return index;
    }

    /**
     * Add the geometry encoded in the given Node. This causes the geometry to appear in the index.
     */
    public SpatialDatabaseRecord add(Node geomNode) {
    	Geometry geometry = getGeometryEncoder().decodeGeometry(geomNode);     
    	
    	// add BBOX to Node if it's missing
    	getGeometryEncoder().encodeGeometry(geometry, geomNode);
    	
        index.add(geomNode);
        return new SpatialDatabaseRecord(this, geomNode, geometry);
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
            GuessGeometryTypeSearch geomTypeSearch = new GuessGeometryTypeSearch();
            index.searchIndex(geomTypeSearch).count();
	    
	    // returns null for an empty layer!
	    return geomTypeSearch.firstFoundType;
        }
    }

    private static class GuessGeometryTypeSearch implements SearchFilter {

        Integer firstFoundType;

        @Override
        public boolean needsToVisit(Envelope indexNodeEnvelope) {
            return firstFoundType == null;
        }

        @Override
    	public boolean geometryMatches(Node geomNode) {
            if (firstFoundType == null) {
                firstFoundType = (Integer) geomNode.getProperty(PROP_TYPE);
            }
            
            return false;
        }
    }

    public String[] getExtraPropertyNames() {
        Node layerNode = getLayerNode();
		try (Transaction tx = layerNode.getGraphDatabase().beginTx()) {
			String[] extraPropertyNames;
			if (layerNode.hasProperty(PROP_LAYERNODEEXTRAPROPS)) {
				extraPropertyNames = (String[]) layerNode.getProperty(PROP_LAYERNODEEXTRAPROPS);
			} else {
				extraPropertyNames = new String[] {};
			}
			tx.success();
			return extraPropertyNames;
		}
    }
    
    public void setExtraPropertyNames(String[] names) {
        Transaction tx = getDatabase().beginTx();
        try {
            getLayerNode().setProperty(PROP_LAYERNODEEXTRAPROPS, names);
            tx.success();
        } finally {
            tx.close();
        }
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
    
	/**
	 * The constructor is protected because we should not construct this class
	 * directly, but use the factory methods to create Layers based on
	 * configurations
	 */
	protected DefaultLayer() {
	}
    
    /**
     * Factory method to construct a layer from an existing layerNode. This will read the layer
     * class from the layer node properties and construct the correct class from that.
     * 
     * @param spatialDatabase
     * @param layerNode
     * @return new layer instance from existing layer node
     */
    @SuppressWarnings("unchecked")
    protected static Layer makeLayerFromNode(SpatialDatabaseService spatialDatabase, Node layerNode) {
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
            return makeLayerInstance(spatialDatabase, name, layerNode, layerClass);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Factory method to construct a layer with the specified layer class. This can be used when
     * creating a layer for the first time. It will also construct the underlying Node in the graph.
     * 
     * @param spatialDatabase
     * @param name
     * @param layerClass
     * @return new Layer instance based on newly created layer Node
     */
    protected static Layer makeLayerAndNode(SpatialDatabaseService spatialDatabase, String name,
            Class< ? extends GeometryEncoder> geometryEncoderClass, Class< ? extends Layer> layerClass) {
        try {
            Node layerNode = spatialDatabase.getDatabase().createNode();
            layerNode.setProperty(PROP_LAYER, name);
            layerNode.setProperty(PROP_CREATIONTIME, System.currentTimeMillis());
            layerNode.setProperty(PROP_GEOMENCODER, geometryEncoderClass.getCanonicalName());
            layerNode.setProperty(PROP_LAYER_CLASS, layerClass.getCanonicalName());
            return DefaultLayer.makeLayerInstance(spatialDatabase, name, layerNode, layerClass);
        } catch (Exception e) {
            throw new SpatialDatabaseException(e);
        }
    }

    private static Layer makeLayerInstance(SpatialDatabaseService spatialDatabase, String name, Node layerNode, Class<? extends Layer> layerClass) throws InstantiationException, IllegalAccessException {
        if(layerClass == null) layerClass = Layer.class;
        Layer layer = layerClass.newInstance();
        layer.initialize(spatialDatabase, name, layerNode);
        return layer;
    }

    public void initialize(SpatialDatabaseService spatialDatabase, String name, Node layerNode) {
        this.spatialDatabase = spatialDatabase;
        this.name = name;
        this.layerNode = layerNode;
        
        // TODO read Precision Model and SRID from layer properties and use them to construct GeometryFactory
        this.geometryFactory = new GeometryFactory();
        
        if (layerNode.hasProperty(PROP_GEOMENCODER)) {
            String encoderClassName = (String) layerNode.getProperty(PROP_GEOMENCODER);
            try {
                this.geometryEncoder = (GeometryEncoder) Class.forName(encoderClassName).newInstance();
            } catch (Exception e) {
                throw new SpatialDatabaseException(e);
            }
			if (this.geometryEncoder instanceof Configurable) {
				if (layerNode.hasProperty(PROP_GEOMENCODER_CONFIG)) {
					((Configurable) this.geometryEncoder).setConfiguration((String) layerNode.getProperty(PROP_GEOMENCODER_CONFIG));
				}
			}
        } else {
            this.geometryEncoder = new WKBGeometryEncoder();
        }
        this.geometryEncoder.init(this);
        
        // index must be created *after* geometryEncoder
        this.index = new LayerRTreeIndex(spatialDatabase.getDatabase(), this);
    }
    
    /**
     * All layers are associated with a single node in the database. This node will have properties,
     * relationships (sub-graph) or both to describe the contents of the layer
     */
    public Node getLayerNode() {
        return layerNode;
    }
    
    /**
     * Delete Layer
     */
    public void delete(Listener monitor) {
        index.removeAll(true, monitor);

        Transaction tx = getDatabase().beginTx();
        try {
            Node layerNode = getLayerNode();
            layerNode.getSingleRelationship(SpatialRelationshipTypes.LAYER, Direction.INCOMING).delete();
            layerNode.delete();

            tx.success();
        } finally {
            tx.close();
        }
    }
    
    
    // Private methods
    
    protected GraphDatabaseService getDatabase() {
        return spatialDatabase.getDatabase();
    }
    
    
    // Attributes
    
    private SpatialDatabaseService spatialDatabase;
    private String name;
    protected Node layerNode;
    protected GeometryEncoder geometryEncoder;
    protected GeometryFactory geometryFactory;
    protected LayerRTreeIndex index;
    
    public SpatialDataset getDataset() {
        return this;
    }

    public Iterable<Node> getAllGeometryNodes() {
        return index.getAllIndexedNodes();
    }

    public boolean containsGeometryNode(Node geomNode) {
        return index.isNodeIndexed(geomNode.getId());
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

    /**
     * This dataset contains only one layer, itself.
     * 
     * @return iterable over all Layers that can be viewed from this dataset
     */
    public Iterable< ? extends Layer> getLayers() {
        return Arrays.asList(new Layer[]{this});
    }

	/**
	 * Override this method to provide a style if your layer wishes to control
	 * its own rendering in the GIS. If a Style is returned, it is used. If a
	 * File is returned, it is opened and assumed to contain SLD contents. If a
	 * String is returned, it is assumed to contain SLD contents.
	 * 
	 * @return null
	 */
	public Object getStyle() {
		return null;
	}

	private PropertyMappingManager propertyMappingManager;

	@Override
	public PropertyMappingManager getPropertyMappingManager() {
		if (propertyMappingManager == null) {
			propertyMappingManager = new PropertyMappingManager(this);
		}
		return propertyMappingManager;
	}

}