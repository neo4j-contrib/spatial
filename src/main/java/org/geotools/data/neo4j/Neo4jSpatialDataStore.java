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
package org.geotools.data.neo4j;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.geotools.data.AbstractDataStore;
import org.geotools.data.AbstractFeatureLocking;
import org.geotools.data.AbstractFeatureStore;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureListener;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.FilteringFeatureWriter;
import org.geotools.data.InProcessLockingManager;
import org.geotools.data.Query;
import org.geotools.data.ResourceInfo;
import org.geotools.data.TransactionStateDiff;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.filter.FidFilterImpl;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.styling.SLDParser;
import org.geotools.styling.Style;
import org.geotools.styling.StyleFactory;
import org.geotools.styling.StyleFactoryImpl;
import org.neo4j.gis.spatial.rtree.filter.SearchAll;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.Utilities;
import org.neo4j.gis.spatial.filter.SearchCQL;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Envelope;


/**
 * Geotools DataStore implementation.
 * 
 * @author Davide Savazzi
 */
public class Neo4jSpatialDataStore extends AbstractDataStore implements Constants {

	// Constructor
	
	public Neo4jSpatialDataStore(GraphDatabaseService database) {
		super(true);
		
		this.database = database;
        this.spatialDatabase = new SpatialDatabaseService(database);
	}
	
	
	// Public methods
		
	/**
	 * Return list of not-empty Layer names.
	 * The list is cached in memory.
	 * 
	 * @return layer names
	 */
    public String[] getTypeNames() throws IOException {
		if (typeNames == null) {
			try (Transaction tx = database.beginTx()) {
				List<String> notEmptyTypes = new ArrayList<String>();
				String[] allTypeNames = spatialDatabase.getLayerNames();
				for (int i = 0; i < allTypeNames.length; i++) {
					// discard empty layers
					System.out.print("loading layer " + allTypeNames[i]);
					Layer layer = spatialDatabase.getLayer(allTypeNames[i]);
					if (!layer.getIndex().isEmpty()) {
						notEmptyTypes.add(allTypeNames[i]);
					}
				}
				typeNames = notEmptyTypes.toArray(new String[]{});
				tx.success();
			}
		}
		return typeNames;
    }
	
    /**
     * Return FeatureType of the given Layer.
     * FeatureTypes are cached in memory.
     */
    public SimpleFeatureType getSchema(String typeName) throws IOException {
        SimpleFeatureType result = simpleFeatureTypeIndex.get(typeName);
        if (result == null) {
            Layer layer = spatialDatabase.getLayer(typeName);
            if (layer == null) {
                throw new IOException("Layer not found: " + typeName);
            }

            result = Neo4jFeatureBuilder.getTypeFromLayer(layer);
            simpleFeatureTypeIndex.put(typeName, result);
        }

        return result;
    }
    
    /**
     * Return a FeatureSource implementation.
     * A FeatureSource can be used to retrieve Layer metadata, bounds and geometries.
     */
    @Override
    public SimpleFeatureSource getFeatureSource(String typeName) throws IOException {
    	SimpleFeatureSource result = featureSourceIndex.get(typeName);
    	if (result == null) {
        	final SimpleFeatureType featureType = getSchema(typeName);    		

        	if (getLockingManager() != null) {
        		System.out.println("getFeatureSource(" + typeName + ") - locking manager is present");
        		
            	result = new AbstractFeatureLocking(getSupportedHints()) {
            		public DataStore getDataStore() {
                        return Neo4jSpatialDataStore.this;
                    }

                    public void addFeatureListener(FeatureListener listener) {
                        listenerManager.addFeatureListener(this, listener);
                    }

                    public void removeFeatureListener(FeatureListener listener) {
                        listenerManager.removeFeatureListener(this, listener);
                    }

                    public SimpleFeatureType getSchema() {
                        return featureType;
                    }
                    
                	public ReferencedEnvelope getBounds() throws IOException {
                		return Neo4jSpatialDataStore.this.getBounds(featureType.getTypeName());
                	}
                    
                    public ResourceInfo getInfo() {
                        return Neo4jSpatialDataStore.this.getInfo(featureType.getTypeName());
                    }                
                };
            } else {  
        		System.out.println("getFeatureSource(" + typeName + ") - locking manager is NOT present");
        		
	        	result = new AbstractFeatureStore(getSupportedHints()) {
	        		public DataStore getDataStore() {
	        			return Neo4jSpatialDataStore.this;
	        		}

	        		public void addFeatureListener(FeatureListener listener) {
	        			listenerManager.addFeatureListener(this, listener);
	        		}

	        		public void removeFeatureListener(FeatureListener listener) {
	        			listenerManager.removeFeatureListener(this, listener);
	        		}

	        		public SimpleFeatureType getSchema() {
	        			return featureType;
	        		}
                
	        		public ReferencedEnvelope getBounds() throws IOException {
	        			return Neo4jSpatialDataStore.this.getBounds(featureType.getTypeName());
	        		}
                
	        		public ResourceInfo getInfo() {
	        			return Neo4jSpatialDataStore.this.getInfo(featureType.getTypeName());
	        		}                
	        	};
            }
        	
            featureSourceIndex.put(typeName, result);
    	}

    	return result;
    }

    /* public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriterAppend(String typeName, org.geotools.data.Transaction transaction) throws IOException {
		FeatureWriter<SimpleFeatureType, SimpleFeature> writer = getFeatureWriter(typeName, Filter.EXCLUDE, transaction);
		while (writer.hasNext()) {
			writer.next();
		}
		return writer;
    } */
        
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(String typeName, Filter filter, org.geotools.data.Transaction transaction) throws IOException {
		if (filter == null) {
			throw new NullPointerException("getFeatureReader requires Filter: did you mean Filter.INCLUDE?");
        }

		if (transaction == null) {
			throw new NullPointerException("getFeatureWriter requires Transaction: did you mean to use Transaction.AUTO_COMMIT?");
		}

		System.out.println("getFeatureWriter(" + typeName + "," + filter.getClass() + " " + filter + "," + transaction + ")");		
		
		FeatureWriter<SimpleFeatureType, SimpleFeature> writer;

		if (transaction == org.geotools.data.Transaction.AUTO_COMMIT) {
			try {
				writer = createFeatureWriter(typeName, filter, transaction);
			} catch (UnsupportedOperationException e) {
				// this is for backward compatibility
				try {
					writer = getFeatureWriter(typeName, transaction);
				} catch (UnsupportedOperationException eek) {
					throw e; // throw original - our fallback did not work
				}
			}
		} else {
			TransactionStateDiff state = state(transaction);
			if (state != null) {
				writer = state.writer(typeName, filter);
			} else {
				throw new UnsupportedOperationException("not implemented...");
			}
		}

		if (getLockingManager() != null) {
			writer = ((InProcessLockingManager) getLockingManager()).checkedWriter(writer, transaction);
		}

		if (filter != Filter.INCLUDE) {
			writer = new FilteringFeatureWriter(writer, filter);
		}

		return writer;
	}
    
	public ReferencedEnvelope getBounds(String typeName) {
    	ReferencedEnvelope result = boundsIndex.get(typeName);
		if (result == null) {
			Layer layer = spatialDatabase.getLayer(typeName);
			if (layer != null) {
				Envelope bbox = Utilities.fromNeo4jToJts(layer.getIndex().getBoundingBox());
				result = convertEnvelopeToRefEnvelope(typeName, bbox);
				boundsIndex.put(typeName, result);
			}
		}
		return result;
    }
    
	public SpatialDatabaseService getSpatialDatabaseService() {
		return spatialDatabase;
	}
	
	public Transaction beginTx() {
		return database.beginTx();
	}
	
    public void clearCache() {
    	typeNames = null;
    	simpleFeatureTypeIndex.clear();
    	crsIndex.clear();
    	styleIndex.clear();
    	boundsIndex.clear();
    	featureSourceIndex.clear();
    }	
		
	public void dispose() {
		database.shutdown();
		
		super.dispose();
	}
	
    
    // Private methods

    protected FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(String typeName, Query query) throws IOException {
    	System.out.println("getFeatureReader(" + typeName + "," + query.getFilter().getClass() + " " + query.getFilter() + ")");
    	
    	FeatureReader<SimpleFeatureType, SimpleFeature> reader = null;
		if (query != null && query.getTypeName() != null) {
			// use Filter to create optimized FeatureReader
			Filter filter = query.getFilter();
			reader = getFeatureReader(typeName, filter);
		}
		
		// default
		if (reader == null) {
			reader = super.getFeatureReader(typeName, query);
		}
		
		return reader;
	}
    
	/**
	 * Create an optimized FeatureReader for most of the uDig operations.
	 */
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(String typeName, Filter filter) throws IOException {
    	Layer layer = spatialDatabase.getLayer(typeName);
    	
    	Iterator<SpatialDatabaseRecord> records;
    	if (filter.equals(Filter.EXCLUDE)) {
    		// filter that excludes everything: create an empty FeatureReader
			records = null;
		} else if (filter instanceof FidFilterImpl) {
			// filter by Feature unique id
			List<SpatialDatabaseRecord> results = layer.getIndex().get(convertToGeomNodeIds((FidFilterImpl) filter));
			System.out.println("found results for FidFilter: " + results.size());
			records = results.iterator();
		} else {
			records = layer.getIndex().search(new SearchCQL(layer, filter));
		}
    	
		return new Neo4jSpatialFeatureReader(layer, getSchema(typeName), records);
    }
    
    protected ResourceInfo getInfo(String typeName) {
    	return new DefaultResourceInfo(typeName, getCRS(typeName), getBounds(typeName));
    }
    
    /**
     * Create a FeatureReader that returns all Feature in the given Layer
     */
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(String typeName) throws IOException {
    	System.out.println("getFeatureReader(" + typeName + ") SLOW QUERY :(");
		return getFeatureReader(typeName, new SearchAll());
	}
	
    private FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(String typeName, SearchFilter search) throws IOException {
    	Layer layer = spatialDatabase.getLayer(typeName);		
    	SearchRecords results = layer.getIndex().search(search);
    	return new Neo4jSpatialFeatureReader(layer, getSchema(typeName), results);
    }
		
    private ReferencedEnvelope convertEnvelopeToRefEnvelope(String typeName, Envelope bbox) {
    	return new ReferencedEnvelope(bbox, getCRS(typeName));
    }

    private CoordinateReferenceSystem getCRS(String typeName) {
    	CoordinateReferenceSystem result = crsIndex.get(typeName);
        if (result == null) {
            Layer layer = spatialDatabase.getLayer(typeName);
            result = layer.getCoordinateReferenceSystem();
            crsIndex.put(typeName, result);
        }
    	
    	return result;
    }

    public Style getStyle(String typeName) {
    	Style result = styleIndex.get(typeName);
        if (true || result == null) {
            Layer layer = spatialDatabase.getLayer(typeName);
            Object obj = layer.getStyle();
            if(obj instanceof Style) {
            	result = (Style)result;
            } else if (obj instanceof File || obj instanceof String) {
            	StyleFactory styleFactory = new StyleFactoryImpl();
            	SLDParser parser = new SLDParser(styleFactory);
            	try {
            		if(obj instanceof File) {
            			parser.setInput(new FileReader((File)obj));
            		}else{
            			parser.setInput(new StringReader(obj.toString()));
            		}
                	Style[] styles = parser.readXML();
					result = styles[0];
            	}
            	catch (Exception e) {
            	    System.err.println("Error loading style '"+obj+"': "+e.getMessage());
            	    e.printStackTrace(System.err);
            	}
            }
            styleIndex.put(typeName, result);
        }
    	
    	return result;
    }

    private Integer getGeometryType(String typeName) {
        Layer layer = spatialDatabase.getLayer(typeName);
        return layer.getGeometryType();
    }

	private EditableLayer getEditableLayer(String typeName) throws IOException {
        Layer layer = spatialDatabase.getLayer(typeName);
        if (layer == null) {
            throw new IOException("Layer not found: " + typeName);
        }
        
        if (!(layer instanceof EditableLayer)) {
            throw new IOException("Cannot create a FeatureWriter on a read-only layer: " + layer);
        }
        
        return (EditableLayer) layer;
	}

	private Set<Long> convertToGeomNodeIds(FidFilterImpl fidFilter) {
		Set<Long> nodeIds = new HashSet<Long>();
		
		Set<String> ids = fidFilter.getIDs();
		for (String id : ids) {
			if (newSimpleFeatures.containsKey(id)) {
				nodeIds.add(newSimpleFeatures.get(id));				
			} else {
				try {
					nodeIds.add(new Long(id));
				} catch (NumberFormatException e) {
					System.out.println("Neo4j Invalid FID: " + id);
				}
			}
		}		
		
		return nodeIds;
	}
	
	/**
	 * Try to create an optimized FeatureWriter for the given Filter.
	 */
    protected FeatureWriter<SimpleFeatureType, SimpleFeature> createFeatureWriter(String typeName, Filter filter, org.geotools.data.Transaction transaction) throws IOException {
    	FeatureReader<SimpleFeatureType, SimpleFeature> reader = getFeatureReader(typeName, filter);
    	if (reader == null) {
    		reader = getFeatureReader(typeName, new SearchAll());
    	}
    	
    	return new Neo4jSpatialFeatureWriter(listenerManager, transaction, getEditableLayer(typeName), reader);
    }
	
    protected FeatureWriter<SimpleFeatureType, SimpleFeature> createFeatureWriter(String typeName, org.geotools.data.Transaction transaction) throws IOException {
    	return new Neo4jSpatialFeatureWriter(listenerManager, transaction, getEditableLayer(typeName), getFeatureReader(typeName, new SearchAll()));
    }
    
	
	// Attributes
	
	private String[] typeNames;
	private Map<String,SimpleFeatureType> simpleFeatureTypeIndex = Collections.synchronizedMap(new HashMap<String,SimpleFeatureType>());
	private Map<String,CoordinateReferenceSystem> crsIndex = Collections.synchronizedMap(new HashMap<String,CoordinateReferenceSystem>());
	private Map<String, Style> styleIndex = Collections.synchronizedMap(new HashMap<String,Style>());
	private Map<String,ReferencedEnvelope> boundsIndex = Collections.synchronizedMap(new HashMap<String,ReferencedEnvelope>());
	private Map<String,SimpleFeatureSource> featureSourceIndex = Collections.synchronizedMap(new HashMap<String,SimpleFeatureSource>());
	private GraphDatabaseService database;
	private SpatialDatabaseService spatialDatabase;
	
	private Map<String,Long> newSimpleFeatures = new HashMap<String,Long>();
}