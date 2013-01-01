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
import java.io.IOException;
import java.util.Map;

import org.geotools.data.AbstractDataStoreFactory;
import org.geotools.data.DataStore;
import org.geotools.util.KVP;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * DataStoreFactorySpi implementation. It needs an "url" parameter containing a
 * path of a Neo4j neostore.id file.
 * 
 * @author Davide Savazzi, Andreas Wilhelm
 */
public class Neo4jSpatialDataStoreFactory extends AbstractDataStoreFactory
		implements org.geotools.data.DataStoreFactorySpi {
	
	/**
	 * url to the neostore.id file.
	 */
	public static final Param DIRECTORY = new Param("The directory path of the Neo4j database: ", File.class,
			"db", true);

	public static final Param DBTYPE = new Param("dbtype", String.class,
			"must be 'neo4j'", true, "neo4j", new KVP(Param.LEVEL, "program"));

	/**
	 * Creates a new instance of Neo4jSpatialDataStoreFactory
	 */
	public Neo4jSpatialDataStoreFactory() {
	}

	public boolean canProcess(Map params) {
	    String type = (String) params.get("dbtype");
	    if(type != null)
        {
            if (!(type.equalsIgnoreCase("neo4j"))) {
            	return false;
            } else {
            	return true;
            }
        }
	    return false;
	}

	
	public DataStore createDataStore(Map params) throws IOException {

		if (!canProcess(params)) {
			throw new IOException("The parameters map isn't correct!!");
		}
		
	    File neodir = (File) DIRECTORY.lookUp(params);

		GraphDatabaseService db = new EmbeddedGraphDatabase(neodir.getAbsolutePath());
		Neo4jSpatialDataStore dataStore = new Neo4jSpatialDataStore(db);

		return dataStore;
	}

	public DataStore createNewDataStore(Map params) throws IOException {
		throw new UnsupportedOperationException(
				"Neo4j Spatial cannot create a new database!");
	}

	
	/**
	 * 
	 */
	public String getDisplayName() {
		return "Neo4j";
	}


	/**
	 * 
	 */
	public String getDescription() {
		return "A datasource backed by a Neo4j Spatial datasource";
	}


	/**
	 * 
	 */
	public boolean isAvailable() {
		return true;
	}

	/*
	 * @see org.geotools.data.DataStoreFactorySpi#getParametersInfo()
	 */
	public Param[] getParametersInfo() {
		return new Param[] { DBTYPE, DIRECTORY };
	}

}