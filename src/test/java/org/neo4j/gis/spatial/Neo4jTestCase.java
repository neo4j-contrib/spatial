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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

/**
 * Base class for the meta model tests.
 */
public abstract class Neo4jTestCase {

	static final Map<Setting<?>, Object> NORMAL_CONFIG = new HashMap<>();

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

	private static final File basePath = new File("target/var");
	private static final Path dbPath = new File(basePath, "neo4j-db").toPath();
	private DatabaseManagementService databases;
	private GraphDatabaseService graphDb;

	private long storePrefix;

	@BeforeEach
	public void setUp() throws Exception {
		updateStorePrefix();
		setUp(true);
	}

	private void updateStorePrefix() {
		storePrefix++;
	}

	/**
	 * Configurable options for text cases, with or without deleting the previous database, and with
	 * or without using the BatchInserter for higher creation speeds. Note that tests that need to
	 * delete nodes or use transactions should not use the BatchInserter.
	 */
	protected void setUp(boolean deleteDb) throws Exception {
		shutdownDatabase(deleteDb);
		Map<Setting<?>, Object> config = NORMAL_CONFIG;
		String largeMode = System.getProperty("spatial.test.large");
		if (largeMode != null && largeMode.equalsIgnoreCase("true")) {
			config = LARGE_CONFIG;
		}
		databases = new TestDatabaseManagementServiceBuilder(getDbPath()).setConfig(config).build();
		graphDb = databases.database(DEFAULT_DATABASE_NAME);
		((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(GlobalProcedures.class)
				.registerProcedure(SpatialProcedures.class);
	}

	/**
	 * For test cases that want to control their own database access, we should
	 * shutdown the current one.
	 */
	private void shutdownDatabase(boolean deleteDb) {
		beforeShutdown();
		if (graphDb != null) {
			databases.shutdown();
			databases = null;
			graphDb = null;
		}
		if (deleteDb) {
			deleteDatabase();
		}
	}

	private static EphemeralFileSystemAbstraction fileSystem;

	@BeforeAll
	static void beforeAll() throws IOException {
		fileSystem = new EphemeralFileSystemAbstraction();
		fileSystem.mkdirs(new File("target").toPath());
	}

	@AfterAll
	static void afterAll() throws IOException {
		fileSystem.close();
	}

	@AfterEach
	public void tearDown() {
		shutdownDatabase(true);
	}

	private void beforeShutdown() {
	}

	static Path getNeoPath() {
		return dbPath.toAbsolutePath();
	}

	Path getDbPath() {
		return dbPath.toAbsolutePath().resolve("test-" + storePrefix);
	}

	private static void deleteDatabase() {
		try {
			FileUtils.deleteDirectory(getNeoPath());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static void deleteBaseDir() {
		deleteFileOrDirectory(basePath);
	}

	private static void deleteFileOrDirectory(File file) {
		if (!file.exists()) {
			return;
		}

		if (file.isDirectory()) {
			for (File child : Objects.requireNonNull(file.listFiles())) {
				deleteFileOrDirectory(child);
			}
		} else {
			//noinspection ResultOfMethodCallIgnored
			file.delete();
		}
	}

	void printDatabaseStats() {
		Neo4jTestUtils.printDatabaseStats(graphDb(), getDbPath().toFile());
	}

	protected GraphDatabaseService graphDb() {
		return graphDb;
	}
}
