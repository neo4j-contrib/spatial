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
package org.neo4j.gis.spatial.geotools.data;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.geotools.data.AbstractDataStore;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureSource;
import org.geotools.data.ResourceInfo;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.BasicFeatureTypes;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.resources.Classes;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.Search;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.query.SearchAll;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.GeometryType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;


/**
 * @author Davide Savazzi
 */
public class Neo4jSpatialDataStore extends AbstractDataStore {

	// Constructor
	
	public Neo4jSpatialDataStore(GraphDatabaseService database) {
		// read only dataStore
		super(false);
		
		this.database = database;
        this.spatialDatabase = new SpatialDatabaseService(database);
	}
	
	
	// Public methods
		
	public String[] getTypeNames() throws IOException {
		if (typeNames == null) {
			Transaction tx = database.beginTx();
			try {
				typeNames = spatialDatabase.getLayerNames();
				tx.success();
			} finally {
				tx.finish();
			}
		}
		
		return typeNames;
	}	
	
	public SimpleFeatureType getSchema(String typeName) throws IOException {
		SimpleFeatureType result = simpleFeatureTypeIndex.get(typeName);
		if (result == null) {
			Transaction tx = database.beginTx();
			try {		
				Layer layer = spatialDatabase.getLayer(typeName);
				if (layer == null) {
					throw new IOException("Layer not found: " + typeName);
				}
				
				String[] extraPropertyNames = layer.getExtraPropertyNames();

				List<AttributeDescriptor> types = readAttributes(typeName);
            
				SimpleFeatureType parent = null;
				GeometryDescriptor geomDescriptor = (GeometryDescriptor) types.get(0);            
				Class<?> geomBinding = geomDescriptor.getType().getBinding();
				if ((geomBinding == Point.class) || (geomBinding == MultiPoint.class)) {
					parent = BasicFeatureTypes.POINT;
				} else if ((geomBinding == Polygon.class) || (geomBinding == MultiPolygon.class)) {
					parent = BasicFeatureTypes.POLYGON;
				} else if ((geomBinding == LineString.class) || (geomBinding == MultiLineString.class)) {
					parent = BasicFeatureTypes.LINE;
				}

				SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
				builder.setDefaultGeometry(geomDescriptor.getLocalName());
				builder.addAll(types);
				builder.setName(typeName);
				builder.setNamespaceURI(BasicFeatureTypes.DEFAULT_NAMESPACE);
				builder.setAbstract(false);
				if (parent != null) {
					builder.setSuperType(parent);
				}  
            
				result = builder.buildFeatureType();
				simpleFeatureTypeIndex.put(typeName, result);

				tx.success();
			} finally {
				tx.finish();
			}
		}
		
		return result;
	}
	    
    public FeatureSource<SimpleFeatureType, SimpleFeature> getFeatureSource(String typeName) throws IOException {
    	return new Neo4jSpatialFeatureSource(this, getSchema(typeName));
    }

    public ReferencedEnvelope getBounds(String typeName) {
		Envelope bbox = spatialDatabase.getLayer(typeName).getIndex().getLayerBoundingBox();
		return convertEnvelopeToRefEnvelope(typeName, bbox);
    }
    
    
    // Protected methods
    
    protected List<AttributeDescriptor> readAttributes(String typeName) throws IOException {
    	List<AttributeDescriptor> attributes = attributesIndex.get(typeName);
    	if (attributes == null) {	
	        // TODO
	        Class<?> geometryClass = MultiPolygon.class;
	            
	        AttributeTypeBuilder build = new AttributeTypeBuilder();
	        build.setName(Classes.getShortName(geometryClass));
	        build.setNillable(true);
	        build.setCRS(getCRS(typeName));
	        build.setBinding(geometryClass);
	
	        GeometryType geometryType = build.buildGeometryType();
	        
	        attributes = new ArrayList<AttributeDescriptor>();
	        attributes.add(build.buildDescriptor(BasicFeatureTypes.GEOMETRY_ATTRIBUTE_NAME, geometryType));

	        attributesIndex.put(typeName, attributes);
    	}
    	
        return attributes;
    }    
    
    protected ResourceInfo getInfo(String typeName) {
    	return new DefaultResourceInfo(typeName, getCRS(typeName), getBounds(typeName));
    }
    
	@Override
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(String typeName) throws IOException {
		Transaction tx = database.beginTx();
		try {				
			Layer layer = spatialDatabase.getLayer(typeName);		
			Search search = new SearchAll();
			layer.getIndex().executeSearch(search);
			Iterator<SpatialDatabaseRecord> results = search.getResults().iterator();

			tx.success();
			
			return new Neo4jSpatialFeatureReader(database, getSchema(typeName), results);
		} finally {
			tx.finish();
		}
	}

	
	// Private methods
	
    private ReferencedEnvelope convertEnvelopeToRefEnvelope(String typeName, Envelope bbox) {
    	return new ReferencedEnvelope(bbox, getCRS(typeName));
    }

    private CoordinateReferenceSystem getCRS(String typeName) {
    	CoordinateReferenceSystem result = crsIndex.get(typeName);
    	if (result == null) {
	    	Transaction tx = database.beginTx();
	    	try {
	    		Layer layer = spatialDatabase.getLayer(typeName);
	    		result = layer.getCoordinateReferenceSystem();
	    		
	    		tx.success();
	    		
	    		crsIndex.put(typeName, result);
	    	} finally {
	    		tx.finish();
	    	}
    	}
    	
    	return result;
    }
	
	
	// Attributes
	
	private String[] typeNames;
	private Map<String,SimpleFeatureType> simpleFeatureTypeIndex = Collections.synchronizedMap(new HashMap());
	private Map<String,List<AttributeDescriptor>> attributesIndex = Collections.synchronizedMap(new HashMap());
	private Map<String,CoordinateReferenceSystem> crsIndex = Collections.synchronizedMap(new HashMap());
	
	private GraphDatabaseService database;
	private SpatialDatabaseService spatialDatabase;
}