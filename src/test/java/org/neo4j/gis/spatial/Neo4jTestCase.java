/*
 * Copyright (c) 2010-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

/**
 * Base class for the meta model tests.
 */
public abstract class Neo4jTestCase extends TestCase {
    static final Map<String, String> NORMAL_CONFIG = new HashMap<>();
    static {
        //NORMAL_CONFIG.put( GraphDatabaseSettings.nodestore_mapped_memory_size.name(), "50M" );
        //NORMAL_CONFIG.put( GraphDatabaseSettings.relationshipstore_mapped_memory_size.name(), "120M" );
        //NORMAL_CONFIG.put( GraphDatabaseSettings.nodestore_propertystore_mapped_memory_size.name(), "150M" );
        //NORMAL_CONFIG.put( GraphDatabaseSettings.strings_mapped_memory_size.name(), "200M" );
        //NORMAL_CONFIG.put( GraphDatabaseSettings.arrays_mapped_memory_size.name(), "0M" );
	NORMAL_CONFIG.put( GraphDatabaseSettings.pagecache_memory.name(), "200M" );
	NORMAL_CONFIG.put( GraphDatabaseSettings.batch_inserter_batch_size.name(), "2" );
        NORMAL_CONFIG.put( GraphDatabaseSettings.dump_configuration.name(), "false" );
    }
    static final Map<String, String> LARGE_CONFIG = new HashMap<>();
    static {
        //LARGE_CONFIG.put( GraphDatabaseSettings.nodestore_mapped_memory_size.name(), "100M" );
        //LARGE_CONFIG.put( GraphDatabaseSettings.relationshipstore_mapped_memory_size.name(), "300M" );
        //LARGE_CONFIG.put( GraphDatabaseSettings.nodestore_propertystore_mapped_memory_size.name(), "400M" );
        //LARGE_CONFIG.put( GraphDatabaseSettings.strings_mapped_memory_size.name(), "800M" );
        //LARGE_CONFIG.put( GraphDatabaseSettings.arrays_mapped_memory_size.name(), "10M" );
	LARGE_CONFIG.put( GraphDatabaseSettings.pagecache_memory.name(), "100M" );
	LARGE_CONFIG.put( GraphDatabaseSettings.batch_inserter_batch_size.name(), "2" );
        LARGE_CONFIG.put( GraphDatabaseSettings.dump_configuration.name(), "true" );
    }
    private static File basePath = new File("target/var");
    private static File dbPath = new File(basePath, "neo4j-db");
    private GraphDatabaseService graphDb;
    private Transaction tx;
    private BatchInserter batchInserter;

    private long storePrefix;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        updateStorePrefix();
        setUp(true, false, false);
    }

    private void updateStorePrefix()
    {
        storePrefix++;
    }

    /**
     * Configurable options for text cases, with or without deleting the previous database, and with
     * or without using the BatchInserter for higher creation speeds. Note that tests that need to
     * delete nodes or use transactions should not use the BatchInserter.
     */
    protected void setUp(boolean deleteDb, boolean useBatchInserter, boolean autoTx) throws Exception {
        super.setUp();
        reActivateDatabase(deleteDb, useBatchInserter, autoTx);
    }

    /**
     * For test cases that want to control their own database access, we should
     * shutdown the current one.
     */
    private void shutdownDatabase(boolean deleteDb) {
        if (tx != null) {
            tx.success();
            tx.close();
            tx = null;
        }
        beforeShutdown();
        if (graphDb != null) {
            graphDb.shutdown();
            graphDb = null;
        }
        if (batchInserter != null) {
            batchInserter.shutdown();
            batchInserter = null;
        }
        if (deleteDb) {
            deleteDatabase(true);
        }
    }

    /**
     * Some tests require switching between normal EmbeddedGraphDatabase and BatchInserter, so we
     * allow that with this method. We also allow deleting the previous database, if that is desired
     * (probably only the first time this is called).
     */
    void reActivateDatabase(boolean deleteDb, boolean useBatchInserter, boolean autoTx) throws Exception {
        shutdownDatabase( deleteDb );
        Map<String, String> config = NORMAL_CONFIG;
        String largeMode = System.getProperty("spatial.test.large");
        if (largeMode != null && largeMode.equalsIgnoreCase("true")) {
            config = LARGE_CONFIG;
        }
        if (useBatchInserter) {
            batchInserter = BatchInserters.inserter(getNeoPath(), config);
        } else {
            graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(getNeoPath()).setConfig(config).newGraphDatabase();
            ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(SpatialProcedures.class);
        }
        if (autoTx) {
            // with the batch inserter the tx is a dummy that simply succeeds all the time
            tx = graphDb.beginTx();
        }
    }
    
    @Rule
    public EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Before
    public void before() throws Exception
    {
        fileSystemRule.get().mkdirs( new File( "target" ) );
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        shutdownDatabase( true );
        super.tearDown();
    }

    private void beforeShutdown() {
    }

    File getNeoPath() {
        return new File(dbPath.getAbsolutePath(), Long.toString(storePrefix));
    }

    private void deleteDatabase(boolean synchronous) {
        if (synchronous)
        {
            try {
                FileUtils.deleteRecursively(getNeoPath());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        else
        {
            final File oldPath = getNeoPath();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        FileUtils.deleteRecursively(oldPath);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }

    static void deleteBaseDir()
    {
        deleteFileOrDirectory(basePath);
    }

    private static void deleteFileOrDirectory(File file) {
        if (!file.exists()) {
            return;
        }

        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                deleteFileOrDirectory(child);
            }
        } else {
            file.delete();
        }
    }

    void printDatabaseStats() {
        Neo4jTestUtils.printDatabaseStats(graphDb(), getNeoPath());
    }

    protected void restartTx() {
        restartTx(true);
    }

    private void restartTx(boolean success) {
        if (tx != null) {
            if (success) {
                tx.success();
            } else {
                tx.failure();
            }
            tx.close();
            tx = graphDb.beginTx();
        }
    }

    protected GraphDatabaseService graphDb() {
        return graphDb;
    }

    BatchInserter getBatchInserter() {
        return batchInserter;
    }

    protected boolean isUsingBatchInserter() {
        return batchInserter != null;
    }

}
