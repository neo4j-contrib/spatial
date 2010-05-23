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

import org.geotools.data.AbstractFeatureSource;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureListener;
import org.geotools.data.ResourceInfo;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeatureType;


/**
 * TODO Query and Filter support
 * 
 * @author Davide Savazzi
 */
public class Neo4jSpatialFeatureSource extends AbstractFeatureSource {

	// Constructor
	
	public Neo4jSpatialFeatureSource(Neo4jSpatialDataStore dataStore, SimpleFeatureType featureType) {
		this.dataStore = dataStore;
		this.featureType = featureType;
	}
	
	
	// Public methods
	
	@Override
	public DataStore getDataStore() {
		return dataStore;
	}

	@Override
	public void addFeatureListener(FeatureListener listener) {
		this.dataStore.listenerManager.addFeatureListener(this, listener);
	}

	@Override
	public void removeFeatureListener(FeatureListener listener) {
		this.dataStore.listenerManager.removeFeatureListener(this, listener);
	}
	
	@Override
	public SimpleFeatureType getSchema() {
		return featureType;
	}

	public ReferencedEnvelope getBound() throws IOException {
		return dataStore.getBounds(featureType.getTypeName());
	}
    
    public ResourceInfo getInfo() {
        return dataStore.getInfo(featureType.getTypeName());
    }
    
    
	// Attributes

	private SimpleFeatureType featureType;
	private Neo4jSpatialDataStore dataStore;
}