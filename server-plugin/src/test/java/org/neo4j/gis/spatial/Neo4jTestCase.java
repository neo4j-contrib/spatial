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
package org.neo4j.gis.spatial;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilder;
import org.neo4j.harness.Neo4jBuilders;

/**
 * Base class for the meta model tests.
 */
public abstract class Neo4jTestCase {

	static final Map<Setting<?>, Object> NORMAL_CONFIG = new HashMap<>();
	static final List<Thread> closeThreads = new ArrayList<>();

	static {
		//NORMAL_CONFIG.put( GraphDatabaseSettings.nodestore_mapped_memory_size.name(), "50M" );
		//NORMAL_CONFIG.put( GraphDatabaseSettings.relationshipstore_mapped_memory_size.name(), "120M" );
		//NORMAL_CONFIG.put( GraphDatabaseSettings.nodestore_propertystore_mapped_memory_size.name(), "150M" );
		//NORMAL_CONFIG.put( GraphDatabaseSettings.strings_mapped_memory_size.name(), "200M" );
		//NORMAL_CONFIG.put( GraphDatabaseSettings.arrays_mapped_memory_size.name(), "0M" );
		NORMAL_CONFIG.put(GraphDatabaseSettings.pagecache_memory, 200000000L);
		NORMAL_CONFIG.put(GraphDatabaseInternalSettings.trace_cursors, true);
	}

	static final Map<Setting<?>, Object> LARGE_CONFIG = new HashMap<>();

	static {
		//LARGE_CONFIG.put( GraphDatabaseSettings.nodestore_mapped_memory_size.name(), "100M" );
		//LARGE_CONFIG.put( GraphDatabaseSettings.relationshipstore_mapped_memory_size.name(), "300M" );
		//LARGE_CONFIG.put( GraphDatabaseSettings.nodestore_propertystore_mapped_memory_size.name(), "400M" );
		//LARGE_CONFIG.put( GraphDatabaseSettings.strings_mapped_memory_size.name(), "800M" );
		//LARGE_CONFIG.put( GraphDatabaseSettings.arrays_mapped_memory_size.name(), "10M" );
		LARGE_CONFIG.put(GraphDatabaseSettings.pagecache_memory, 100000000L);
	}

	private DatabaseManagementService databases;
	private GraphDatabaseService graphDb;
	private Neo4j neo4j;
	protected Driver driver;

	/**
	 * Configurable options for text cases, with or without deleting the previous database, and with
	 * or without using the BatchInserter for higher creation speeds. Note that tests that need to
	 * delete nodes or use transactions should not use the BatchInserter.
	 */
	@SuppressWarnings("unchecked")
	@BeforeEach
	void setUpDatabase() throws Exception {
		Neo4jBuilder neo4jBuilder = Neo4jBuilders
				.newInProcessBuilder(getDbPath())
				.withConfig(GraphDatabaseSettings.procedure_unrestricted, List.of("spatial.*"))
				.withProcedure(SpatialProcedures.class)
				.withFunction(SpatialFunctions.class);

		String largeMode = System.getProperty("spatial.test.large");
		if (largeMode != null && largeMode.equalsIgnoreCase("true")) {
			LARGE_CONFIG.forEach((setting, o) -> neo4jBuilder.withConfig((Setting<Object>) setting, o));
		} else {
			NORMAL_CONFIG.forEach((setting, o) -> neo4jBuilder.withConfig((Setting<Object>) setting, o));
		}

		neo4j = neo4jBuilder.build();
		driver = GraphDatabase.driver(neo4j.boltURI().toString(), AuthTokens.basic("neo4j", ""));
	}

	@AfterAll
	static void afterAll() throws InterruptedException {
		for (Thread closeThread : closeThreads) {
			closeThread.join();
		}
	}

	@AfterEach
	public void tearDown() {
		driver.close();
		// defer cleanup so the test can run faster
		Thread closeThread = new Thread(() -> neo4j.close());
		closeThread.start();
		closeThreads.add(closeThread);
	}

	static Path getDbPath() {
		return Path.of("target", "neo4j-db");
	}

	void printDatabaseStats() {
		Neo4jTestUtils.printDatabaseStats(graphDb(), getDbPath().toFile());
	}

	protected GraphDatabaseService graphDb() {
		return neo4j.databaseManagementService().database(DEFAULT_DATABASE_NAME);
	}
}
