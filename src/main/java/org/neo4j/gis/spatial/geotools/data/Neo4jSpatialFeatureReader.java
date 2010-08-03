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
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.geotools.data.FeatureReader;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;


/**
 * @author Davide Savazzi
 */
public class Neo4jSpatialFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

	// Constructor
	
	public Neo4jSpatialFeatureReader(GraphDatabaseService database, Layer layer, SimpleFeatureType featureType, Iterator<SpatialDatabaseRecord> results) {
		this.database = database;
		this.layer = layer;
		this.extraPropertyNames = layer.getExtraPropertyNames();		
		this.featureType = featureType;
		this.builder = new SimpleFeatureBuilder(featureType);
		this.results = results;
	}
	
	
	// Public methods
	
	public SimpleFeatureType getFeatureType() {
		return featureType;
	}

	public boolean hasNext() throws IOException {
		return results.hasNext();
	}

	public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {
		SpatialDatabaseRecord record = results.next();

		Transaction tx = database.beginTx();
		try {				
	        builder.reset();
	        builder.set(FEATURE_PROP_GEOM, record.getGeometry());
	        
	        if (extraPropertyNames != null) {
		        for (int i = 0; i < extraPropertyNames.length; i++) {
		        	if (record.hasProperty(extraPropertyNames[i])) {
		        		builder.set(extraPropertyNames[i], record.getProperty(extraPropertyNames[i]));
		        	}
		        }
	        }
	        
	        SimpleFeature simpleFeature = builder.buildFeature(Long.toString(record.getId()));						
			
			tx.success();

			return simpleFeature;
		} finally {
			tx.finish();
		}
	}
	
	public void close() throws IOException {
		database = null;
		featureType = null;
		builder = null;
		results = null;
	}
	
	protected Layer getLayer() {
		return layer;
	}
	
	
	// Attributes
	
	private GraphDatabaseService database;
	private Layer layer;
	private SimpleFeatureType featureType;
    private SimpleFeatureBuilder builder;
	private Iterator<SpatialDatabaseRecord> results;
	private String[] extraPropertyNames;
	
	protected static final String FEATURE_PROP_GEOM = "the_geom";
}