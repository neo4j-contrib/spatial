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
package org.neo4j.spatial.geotools.plugin;

import static org.geotools.api.data.Parameter.IS_PASSWORD;

import java.io.IOException;
import java.util.Map;
import org.geotools.api.data.DataAccessFactory;
import org.geotools.util.KVP;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

/**
 * DataAccessFactory implementation for Neo4j with a driver-based connection.
 */
public class Neo4jSpatialDataStoreFactory implements DataAccessFactory {

	public static final Param DBTYPE = new Param("dbtype", String.class,
			"must be 'neo4j-driver'", true, "neo4j-driver", new KVP(Param.LEVEL, "program"));

	public static final Param URI = new Param("uri", String.class,
			"URI for the Neo4j server", true, "bolt://localhost:7687");

	public static final Param DATABASE = new Param("database", String.class,
			"Neo4j database name", false, "neo4j");

	public static final Param USERNAME = new Param("username", String.class,
			"Username for Neo4j authentication", true, "neo4j");

	public static final Param PASSWORD = new Param("password", String.class,
			"Password for Neo4j authentication", true, null, Map.of(IS_PASSWORD, true));

	/**
	 * Creates a new instance of Neo4jSpatialDataStoreFactory
	 */
	public Neo4jSpatialDataStoreFactory() {
	}

	@Override
	public boolean canProcess(Map params) {
		String type = (String) params.get(DBTYPE.key);
		return DBTYPE.sample.equals(type);
	}

	@Override
	public Neo4jSpatialDataStore createDataStore(Map<String, ?> params) throws IOException {

		if (!canProcess(params)) {
			throw new IOException("The parameters map isn't correct");
		}

		String uri = (String) URI.lookUp(params);
		String username = (String) USERNAME.lookUp(params);
		String password = (String) PASSWORD.lookUp(params);
		String database = params.containsKey(DATABASE.key) ?
				(String) DATABASE.lookUp(params) : "neo4j";

		Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));

		return new Neo4jSpatialDataStore(driver, database);
	}

	@Override
	public String getDisplayName() {
		return "Neo4j";
	}

	@Override
	public String getDescription() {
		return "A datasource connecting a neo4j server that has the neo4j-spatial plugin installed";
	}

	@Override
	public boolean isAvailable() {
		return true;
	}

	@Override
	public Param[] getParametersInfo() {
		return new Param[]{DBTYPE, URI, DATABASE, USERNAME, PASSWORD};
	}

}
