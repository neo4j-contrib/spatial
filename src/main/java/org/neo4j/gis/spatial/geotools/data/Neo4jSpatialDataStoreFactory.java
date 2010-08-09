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

import java.awt.RenderingHints.Key;
import java.io.File;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;

import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.util.KVP;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.EmbeddedGraphDatabase;


/**
 * @author Davide Savazzi
 */
public class Neo4jSpatialDataStoreFactory implements DataStoreFactorySpi {

	// Public methods
	
    /**
     * Takes a map of parameters which describes how to access a DataStore and
     * determines if it can be read by the Neo4jSpatialDataStore.
     * 
     * @param params A map of parameters describing the location of a datastore
     * @return true if params contains a url param which points to a file named 'neostore.id'
     */
    public boolean canProcess(Map<String,Serializable> params) {
        if (params == null) {
            return false;
        }    	

    	File neo4jDir = getNeo4jDir(params.get(URLP.key));
    	return neo4jDir != null && neo4jDir.isDirectory() && neo4jDir.canWrite();
    }
    
    public String getDataStoreUniqueIdentifier(Map<String,Serializable> params) {
		File neo4jDir = getNeo4jDir(params.get(URLP.key));
		return neo4jDir.getAbsolutePath();
    }

	public DataStore createDataStore(Map<String,Serializable> params) {
    	File neo4jDir = getNeo4jDir(params.get(URLP.key));
    	EmbeddedGraphDatabase db = null;
    	Neo4jSpatialDataStore neo4jSpatialDataStore = null;
		try {
			db = new EmbeddedGraphDatabase(neo4jDir.getAbsolutePath());
			neo4jSpatialDataStore  = new Neo4jSpatialDataStore(db);
    	} catch (TransactionFailureException tfe) {
    		tfe.printStackTrace();
    	}
		return neo4jSpatialDataStore;
	}
	
	public DataStore createNewDataStore(Map<String,Serializable> params) {
		return createDataStore(params);
	}

	public String getDisplayName() {
		return "Neo4j";
	}	
	
	public String getDescription() {
        return "Neo4j Graph Database";		
	}


	public Param[] getParametersInfo() {
        return new Param[] { URLP };
	}

	public boolean isAvailable() {
        try {
        	EmbeddedGraphDatabase.class.getName();
            GraphDatabaseService.class.getName();
        } catch (Exception e) {
        	log.error(e.getMessage(), e);
            return false;
        }

        return true;
	}
	
	public Map<Key, ?> getImplementationHints() {
        return Collections.EMPTY_MAP;		
	}	

	
	// Private methods
	
	private File getNeo4jDir(Object param) {
		if (param == null) return null;
		
		URL url;
		if (param instanceof URL) {
        	url = (URL) param;
		} else if (param instanceof String) {
			try {
				url = new URL((String) param);
			} catch (MalformedURLException e) {
				log.warn(e.getMessage(), e);
				return null;
			}
		} else {
			return null;
		}
		
		File neostoreId = new File(url.getPath());
        return neostoreId.getParentFile();		
	}
	
	
	// Attributes

    /**
     * url to the neostore.id file.
     */
    public static final Param URLP = new Param("url", URL.class,
            "url to a neostore.id file", true, null,
            new KVP(Param.EXT, "id"));
    
    private static final Logger log = Logger.getLogger("neo4j");
}