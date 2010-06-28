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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.util.KVP;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;


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
    public boolean canProcess(Map params) {
        if (params.containsKey(URLP.key)) {
        	URL url = (URL) params.get(Neo4jSpatialDataStoreFactory.URLP.key);
        	return url.getFile().endsWith("neostore.id");
        }
        
        return false;
    }
    
    public String getDataStoreUniqueIdentifier(Map params) {
		URL url = (URL) params.get(Neo4jSpatialDataStoreFactory.URLP.key);
		return url.getPath();    	
    }

	public DataStore createDataStore(Map params) throws IOException {
		URL url = (URL) params.get(Neo4jSpatialDataStoreFactory.URLP.key);
		File neostoreId = new File(url.getPath());
        GraphDatabaseService database = new EmbeddedGraphDatabase(neostoreId.getParent());
        return new Neo4jSpatialDataStore(database);
	}
	
	public DataStore createNewDataStore(Map params) throws IOException {
		// TODO
		throw new UnsupportedOperationException();
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
        	EmbeddedReadOnlyGraphDatabase.class.getName();
            GraphDatabaseService.class.getName();
        } catch (Exception e) {
        	e.printStackTrace();
            return false;
        }

        return true;
	}

    public Map getImplementationHints() {
        return Collections.EMPTY_MAP;
    }
    
	
	// Attributes

    /**
     * url to the neostore.id file.
     */
    public static final Param URLP = new Param("url", URL.class,
            "url to a neostore.id file", true, null,
            new KVP(Param.EXT, "id"));
}
