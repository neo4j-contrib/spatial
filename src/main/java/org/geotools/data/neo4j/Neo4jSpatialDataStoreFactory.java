/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Spatial.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.geotools.data.neo4j;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFactorySpi;
import org.geotools.util.KVP;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * DataStoreFactorySpi implementation. It needs an "url" parameter containing a
 * path of a Neo4j neostore-id file.
 */
public class Neo4jSpatialDataStoreFactory implements DataStoreFactorySpi {

	// TODO: This should change to Neo4j 4.x directory layout and possible multiple databases
	/**
	 * url to the neostore-id file.
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

	@Override
	public boolean canProcess(Map params) {
		String type = (String) params.get("dbtype");
		if (type != null) {
			return type.equalsIgnoreCase("neo4j");
		}
		return false;
	}

	@Override
	public DataStore createDataStore(Map<String, ?> params) throws IOException {

		if (!canProcess(params)) {
			throw new IOException("The parameters map isn't correct!!");
		}

		File neodir = (File) DIRECTORY.lookUp(params);

		DatabaseManagementService databases = new DatabaseManagementServiceBuilder(neodir.toPath()).build();
		GraphDatabaseService db = databases.database(DEFAULT_DATABASE_NAME);

		return new Neo4jSpatialDataStore(db);
	}

	@Override
	public DataStore createNewDataStore(Map params) {
		throw new UnsupportedOperationException("Neo4j Spatial cannot create a new database!");
	}

	@Override
	public String getDisplayName() {
		return "Neo4j";
	}

	@Override
	public String getDescription() {
		return "A datasource backed by a Neo4j Spatial database";
	}

	@Override
	public boolean isAvailable() {
		return true;
	}

	@Override
	public Param[] getParametersInfo() {
		return new Param[]{DBTYPE, DIRECTORY};
	}

}
