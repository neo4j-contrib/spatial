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
package org.neo4j.gis.spatial;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;


/**
 * Base class for the meta model tests.
 */
public abstract class Neo4jTestCase extends TestCase {
    public static final Map<String, String> NORMAL_CONFIG = new HashMap<String, String>();
    static {
        NORMAL_CONFIG.put( GraphDatabaseSettings.nodestore_mapped_memory_size.name(), "50M" );
        NORMAL_CONFIG.put( GraphDatabaseSettings.relationshipstore_mapped_memory_size.name(), "120M" );
        NORMAL_CONFIG.put( GraphDatabaseSettings.nodestore_propertystore_mapped_memory_size.name(), "150M" );
        NORMAL_CONFIG.put( GraphDatabaseSettings.strings_mapped_memory_size.name(), "200M" );
        NORMAL_CONFIG.put( GraphDatabaseSettings.arrays_mapped_memory_size.name(), "0M" );
        NORMAL_CONFIG.put( GraphDatabaseSettings.dump_configuration.name(), "false" );
    }
    protected static final Map<String, String> LARGE_CONFIG = new HashMap<String, String>();
    static {
        LARGE_CONFIG.put( GraphDatabaseSettings.nodestore_mapped_memory_size.name(), "100M" );
        LARGE_CONFIG.put( GraphDatabaseSettings.relationshipstore_mapped_memory_size.name(), "300M" );
        LARGE_CONFIG.put( GraphDatabaseSettings.nodestore_propertystore_mapped_memory_size.name(), "400M" );
        LARGE_CONFIG.put( GraphDatabaseSettings.strings_mapped_memory_size.name(), "800M" );
        LARGE_CONFIG.put( GraphDatabaseSettings.arrays_mapped_memory_size.name(), "10M" );
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
        updateStorePrefix();
        setUp(false, false, false);
    }

    protected void updateStorePrefix()
    {
        storePrefix++;
    }

    /**
     * Configurable options for text cases, with or without deleting the previous database, and with
     * or without using the BatchInserter for higher creation speeds. Note that tests that need to
     * delete nodes or use transactions should not use the BatchInserter.
     *
     * @param deleteDb
     * @param useBatchInserter
     * @throws Exception
     */
    protected void setUp(boolean deleteDb, boolean useBatchInserter, boolean autoTx) throws Exception {
        super.setUp();
        reActivateDatabase(deleteDb, useBatchInserter, autoTx);
    }

    /**
     * For test cases that want to control their own database access, we should
     * shutdown the current one.
     *
     * @param deleteDb
     */
    protected void shutdownDatabase(boolean deleteDb) {
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
     *
     * @param deleteDb
     * @param useBatchInserter
     * @throws Exception
     */
    protected void reActivateDatabase(boolean deleteDb, boolean useBatchInserter, boolean autoTx) throws Exception {
        shutdownDatabase( deleteDb );
        Map<String, String> config = NORMAL_CONFIG;
        String largeMode = System.getProperty("spatial.test.large");
        if (largeMode != null && largeMode.equalsIgnoreCase("true")) {
            config = LARGE_CONFIG;
        }
        if (useBatchInserter) {
            batchInserter = BatchInserters.inserter(getNeoPath().getAbsolutePath(), config);
	    //graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(getNeoPath().getAbsolutePath()).setConfig( config ).newGraphDatabase();
	    //graphDb = new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( getNeoPath().getAbsolutePath() );
        } else {
	    //graphDb = new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( getNeoPath().getAbsolutePath() );
	    graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(getNeoPath().getAbsolutePath()).setConfig( config ).newGraphDatabase();
        }
        if (autoTx) {
            // with the batch inserter the tx is a dummy that simply succeeds all the time
            tx = graphDb.beginTx();
        }
    }
    
    @Rule
    public EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    private EphemeralFileSystemAbstraction fileSystem;

    @Before
    public void before() throws Exception
    {
        fileSystem = fileSystemRule.get();
        fileSystem.mkdirs( new File( "target" ) );
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        shutdownDatabase( true );
        super.tearDown();
    }

    protected void beforeShutdown() {
    }

    protected File getNeoPath() {
        return new File(dbPath.getAbsolutePath(), Long.toString(storePrefix));
    }

    protected void deleteDatabase(boolean synchronous) {
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
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        FileUtils.deleteRecursively(getNeoPath());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }

    protected static void deleteBaseDir()
    {
        deleteFileOrDirectory(basePath);
    }

    protected static void deleteFileOrDirectory(File file) {
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

    protected long calculateDiskUsage(File file) {
        if(file.isDirectory()) {
            long count = 0;
            for(File sub:file.listFiles()) {
                count += calculateDiskUsage(sub);
            }
            return count;
        } else {
            return file.length();
        }
    }

    protected long databaseDiskUsage() {
        return calculateDiskUsage(getNeoPath());
    }

    protected void printDatabaseStats() {
        System.out.println("Database stats:");
        System.out.println("\tTotal disk usage: "+(databaseDiskUsage())/(1024.0*1024.0)+"MB");
        System.out.println("\tTotal # nodes:    "+nodeStore().getNumberOfIdsInUse());
        System.out.println("\tTotal # rels:     "+relationshipStore().getNumberOfIdsInUse());
        System.out.println("\tTotal # props:    "+propertyStore().getNumberOfIdsInUse());
    }
    protected void restartTx() {
        restartTx(true);
    }

    protected void restartTx(boolean success) {
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

    protected BatchInserter getBatchInserter() {
        return batchInserter;
    }

    protected boolean isUsingBatchInserter() {
        return batchInserter != null;
    }

    protected org.neo4j.kernel.impl.store.PropertyStore propertyStore()
    {
        NeoStore neoStore = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( NeoStoreProvider.class ).evaluate();
        return neoStore.getPropertyStore();
    }

    protected org.neo4j.kernel.impl.store.NodeStore nodeStore()
    {
        NeoStore neoStore = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( NeoStoreProvider.class ).evaluate();
        return neoStore.getNodeStore();
    }

    protected org.neo4j.kernel.impl.store.RelationshipStore relationshipStore()
    {
        NeoStore neoStore = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( NeoStoreProvider.class ).evaluate();
        return neoStore.getRelationshipStore();
    }

    protected <T> void assertCollection(Collection<T> collection, T... expectedItems) {
        String collectionString = join(", ", collection.toArray());
        assertEquals(collectionString, expectedItems.length, collection.size());
        for (T item : expectedItems) {
            assertTrue(collection.contains(item));
        }
    }

    protected <T> Collection<T> asCollection(Iterable<T> iterable) {
        List<T> list = new ArrayList<T>();
        for (T item : iterable) {
            list.add(item);
        }
        return list;
    }

    protected <T> String join(String delimiter, T... items) {
        StringBuffer buffer = new StringBuffer();
        for (T item : items) {
            if (buffer.length() > 0) {
                buffer.append(delimiter);
            }
            buffer.append(item.toString());
        }
        return buffer.toString();
    }

    protected <T> int countIterable(Iterable<T> iterable) {
        int counter = 0;
        Iterator<T> itr = iterable.iterator();
        while (itr.hasNext()) {
            itr.next();
            counter++;
        }
        return counter;
    }
    
	protected void debugIndexTree(RTreeIndex index) {
        try (Transaction tx = graphDb().beginTx()) {
            printTree(index.getIndexRoot(), 0);
            tx.success();
        }
	}
	
	private static String arrayString(double[] test) {
		StringBuffer sb = new StringBuffer();
		for (double d : test) {
			addToArrayString(sb, d);
		}
		sb.append("]");
		return sb.toString();
	}	
	
	private static void addToArrayString(StringBuffer sb, Object obj) {
		if (sb.length() == 0) {
			sb.append("[");
		} else {
			sb.append(",");
		}
		sb.append(obj);
	}
	
	private void printTree(Node root, int depth) {
		StringBuffer tab = new StringBuffer();
		for (int i = 0; i < depth; i++) {
			tab.append("  ");
		}
		
		if (root.hasProperty(Constants.PROP_BBOX)) {
			System.out.println(tab.toString() + "INDEX: " + root + " BBOX[" + arrayString((double[]) root.getProperty(Constants.PROP_BBOX)) + "]");
		} else {
			System.out.println(tab.toString() + "INDEX: " + root);
		}
		
		StringBuffer data = new StringBuffer();
		for (Relationship rel : root.getRelationships(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING)) {
			if (data.length() > 0) {
				data.append(", ");
			} else {
				data.append("DATA: ");
			}
			data.append(rel.getEndNode().toString());
		}
		
		if (data.length() > 0) {
			System.out.println("  " + tab + data);
		}
		
		for (Relationship rel : root.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
			printTree(rel.getEndNode(), depth + 1);
		}
	}    
}