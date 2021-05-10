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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

/**
 * Base class for the meta model tests.
 */
public abstract class Neo4jTestCase {
    static final Map<String, String> NORMAL_CONFIG = new HashMap<>();

    static {
        //NORMAL_CONFIG.put( GraphDatabaseSettings.nodestore_mapped_memory_size.name(), "50M" );
        //NORMAL_CONFIG.put( GraphDatabaseSettings.relationshipstore_mapped_memory_size.name(), "120M" );
        //NORMAL_CONFIG.put( GraphDatabaseSettings.nodestore_propertystore_mapped_memory_size.name(), "150M" );
        //NORMAL_CONFIG.put( GraphDatabaseSettings.strings_mapped_memory_size.name(), "200M" );
        //NORMAL_CONFIG.put( GraphDatabaseSettings.arrays_mapped_memory_size.name(), "0M" );
        NORMAL_CONFIG.put(GraphDatabaseSettings.pagecache_memory.name(), "200M");
    }

    static final Map<String, String> LARGE_CONFIG = new HashMap<>();

    static {
        //LARGE_CONFIG.put( GraphDatabaseSettings.nodestore_mapped_memory_size.name(), "100M" );
        //LARGE_CONFIG.put( GraphDatabaseSettings.relationshipstore_mapped_memory_size.name(), "300M" );
        //LARGE_CONFIG.put( GraphDatabaseSettings.nodestore_propertystore_mapped_memory_size.name(), "400M" );
        //LARGE_CONFIG.put( GraphDatabaseSettings.strings_mapped_memory_size.name(), "800M" );
        //LARGE_CONFIG.put( GraphDatabaseSettings.arrays_mapped_memory_size.name(), "10M" );
        LARGE_CONFIG.put(GraphDatabaseSettings.pagecache_memory.name(), "100M");
    }

    private static final File basePath = new File("target/var");
    private static final Path dbPath = new File(basePath, "neo4j-db").toPath();
    private DatabaseManagementService databases;
    private GraphDatabaseService graphDb;

    private long storePrefix;

    @Before
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
        DatabaseLayout layout = prepareLayout(true);
        Map<String, String> config = NORMAL_CONFIG;
        String largeMode = System.getProperty("spatial.test.large");
        if (largeMode != null && largeMode.equalsIgnoreCase("true")) {
            config = LARGE_CONFIG;
        }
        databases = new DatabaseManagementServiceBuilder(getDbPath()).setConfigRaw(config).build();
        graphDb = databases.database(DEFAULT_DATABASE_NAME);
        ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(GlobalProcedures.class).registerProcedure(SpatialProcedures.class);
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

    private DatabaseLayout prepareLayout(boolean delete) throws IOException {
        Neo4jLayout homeLayout = Neo4jLayout.of(dbPath);
        DatabaseLayout databaseLayout = homeLayout.databaseLayout(DEFAULT_DATABASE_NAME);
        if (delete) {
            FileUtils.deleteDirectory(databaseLayout.databaseDirectory());
            FileUtils.deleteDirectory(databaseLayout.getTransactionLogsDirectory());
        }
        return databaseLayout;
    }

    private Config makeConfig(Map<String, String> config) {
        Config.Builder builder = Config.newBuilder();
        builder.setRaw(NORMAL_CONFIG);
        return builder.build();
    }

    @Rule
    public EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Before
    public void before() throws Exception {
        fileSystemRule.get().mkdirs(new File("target").toPath());
    }

    @After
    public void tearDown() {
        shutdownDatabase(true);
    }

    private void beforeShutdown() {
    }

    Path getNeoPath() {
        return dbPath.toAbsolutePath();
    }

    Path getDbPath() {
        return dbPath.toAbsolutePath().resolve("test-" + storePrefix);
    }

    private void deleteDatabase() {
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
