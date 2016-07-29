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

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.geotools.data.FeatureReader;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.graphdb.Transaction;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;


/**
 * FeatureReader implementation.
 * Instances of this class are created by Neo4jSpatialDataStore.
 * 
 * @author Davide Savazzi, Andreas Wilhelm
 */
public class Neo4jSpatialFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {
	
    private static final Logger log = Logger.getLogger(Neo4jSpatialFeatureReader.class.getName());
	protected static final String FEATURE_PROP_GEOM = "the_geom";

	private Layer layer;
	private SimpleFeatureType featureType;
    private SimpleFeatureBuilder builder;
	private Iterator<SpatialDatabaseRecord> results;
	private String[] extraPropertyNames;
	
	/**
	 * 
	 * @param layer
	 * @param featureType
	 * @param results
	 */
	public Neo4jSpatialFeatureReader(Layer layer, SimpleFeatureType featureType, Iterator<SpatialDatabaseRecord> results) {
		
		this.layer = layer;
		this.extraPropertyNames = layer.getExtraPropertyNames();		
		this.featureType = featureType;
		this.builder = new SimpleFeatureBuilder(featureType);
		this.results = results;
	}
	
	/**
	 * 
	 * 
	 */
	public SimpleFeatureType getFeatureType() {
		return featureType;
	}

	/**
	 * 
	 */
	public boolean hasNext() throws IOException {
		if (results == null) return false;
		try (Transaction tx = layer.getSpatialDatabase().getDatabase().beginTx()) {
			boolean ans = results.hasNext();
			tx.success();
			return ans;
		}
	}

	/**
	 * 
	 */
	public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {
		if (results == null) return null;

		try (Transaction tx = layer.getSpatialDatabase().getDatabase().beginTx()) {
			SpatialDatabaseRecord record = results.next();
			if (record == null) return null;

			builder.reset();

			builder.set(FEATURE_PROP_GEOM, record.getGeometry());

			if (extraPropertyNames != null) {
				for (int i = 0; i < extraPropertyNames.length; i++) {
					if (record.hasProperty(extraPropertyNames[i])) {
						builder.set(extraPropertyNames[i], record.getProperty(extraPropertyNames[i]));
					}
				}
			}
			tx.success();

			return builder.buildFeature(record.getId());
		}
	}
	
	/**
	 * 
	 */
	public void close() throws IOException {
		log.debug("");
		featureType = null;
		builder = null;
		results = null;
	}
	
	/**
	 * 
	 * @return
	 */
	protected Layer getLayer() {
		return layer;
	}

}