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
import java.util.logging.Logger;

import org.geotools.data.FeatureListenerManager;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.graphdb.Transaction;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;


/**
 * FeatureWriter implementation.
 * Instances of this class are created by Neo4jSpatialDataStore.
 * 
 * @author Davide Savazzi
 */
public class Neo4jSpatialFeatureWriter implements FeatureWriter<SimpleFeatureType, SimpleFeature> {

	// Constructor
	
	protected Neo4jSpatialFeatureWriter(FeatureListenerManager listener, org.geotools.data.Transaction transaction, EditableLayer layer, FeatureReader<SimpleFeatureType, SimpleFeature> reader) {
		this.transaction = transaction;
		this.listener = listener;
		this.reader = reader;
		this.layer = layer;
		this.featureType = reader.getFeatureType();
	}
	
	
	// Public methods

	public SimpleFeatureType getFeatureType() {
        return featureType;
	}
	
	public boolean hasNext() throws IOException {
        if (closed) {
            throw new IOException("Feature writer is closed");
        }
        
        return reader != null && reader.hasNext();
	}

	public SimpleFeature next() throws IOException {
		if (closed) {
            throw new IOException("FeatureWriter has been closed");
        }

        SimpleFeatureType featureType = getFeatureType();

        if (hasNext()) {
            live = reader.next();
            current = SimpleFeatureBuilder.copy(live);
            LOGGER.finer("Calling next on writer");
        } else {
            // new content
            live = null;
            current = SimpleFeatureBuilder.template(featureType, null);
        }

        return current;
	}

	public void remove() throws IOException {
        if (closed) {
            throw new IOException("FeatureWriter has been closed");
        }

        if (current == null) {
            throw new IOException("No feature available to remove");
        }
        
        if (live != null) {
            LOGGER.fine("Removing " + live);
            
            Transaction tx = layer.getSpatialDatabase().getDatabase().beginTx();
            try {
            	layer.delete(Long.parseLong(live.getID()));  
            	tx.success();
            } finally {
            	tx.finish();
            }
            
            listener.fireFeaturesRemoved(featureType.getTypeName(), 
            		transaction, new ReferencedEnvelope(live.getBounds()), true);
        }
        
        live = null;
        current = null;
	}

	public void write() throws IOException {
        if (closed) {
            throw new IOException("FeatureWriter has been closed");
        }

        if (current == null) {
            throw new IOException("No feature available to write");
        }

        LOGGER.fine("Write called, live is " + live + " and cur is " + current);

        if (live != null) {
            if (!live.equals(current)) {
                LOGGER.fine("Updating " + current);
                Transaction tx = layer.getSpatialDatabase().getDatabase().beginTx();
                try {
                	layer.update(Long.parseLong(current.getID()), (Geometry) current.getDefaultGeometry());  
                	tx.success();
                } finally {
                	tx.finish();
                }
                
                listener.fireFeaturesChanged(featureType.getTypeName(), 
                		transaction, new ReferencedEnvelope(current.getBounds()), true);            

            }
        } else {
            LOGGER.fine("Inserting " + current);
            Transaction tx = layer.getSpatialDatabase().getDatabase().beginTx();
            try {
            	layer.add((Geometry) current.getDefaultGeometry());  
            	tx.success();
            } finally {
            	tx.finish();
            }
            
            listener.fireFeaturesAdded(featureType.getTypeName(), 
            		transaction, new ReferencedEnvelope(current.getBounds()), true);            
        }
        
        live = null;
        current = null;        
	}
	
	public void close() throws IOException {
        if (reader != null) reader.close();
        closed = true;
	}
	
	
	// Attributes
		
	// current for FeatureWriter
	private SimpleFeature live; 
	// copy of live returned to user
	private SimpleFeature current; 
	
	private FeatureListenerManager listener;
	private org.geotools.data.Transaction transaction;
	private SimpleFeatureType featureType;
	private FeatureReader<SimpleFeatureType, SimpleFeature> reader;	
	private EditableLayer layer;
	private boolean closed;
	
    private static final Logger LOGGER = org.geotools.util.logging.Logging.getLogger("org.neo4j.gis.spatial");
}