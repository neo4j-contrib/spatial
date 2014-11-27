package org.neo4j.gis.spatial;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import org.neo4j.io.fs.FileUtils;

public class TestReadOnlyTransactions {
    private GraphDatabaseService graphDb;
    private static File basePath = new File("target/var");
    private static File dbPath = new File(basePath, "neo4j-db");

    private long storePrefix;
    
    private static long n1Id = 0L;
    private static long n2Id = 0L;

    @Before
    public void setUp() throws Exception {
        storePrefix++;
    	graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(getNeoPath().getAbsolutePath()).newGraphDatabase();
    	buildDataModel();
    }

    @After
    public void tearDown() throws Exception {
        graphDb.shutdown();
		deleteDatabase();
    }
	
    private void buildDataModel() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = graphDb.createNode();
			n1.setProperty("name", "n1");
			Node n2 = graphDb.createNode();
			n2.setProperty("name", "n2");
			n1.createRelationshipTo(n2, DynamicRelationshipType.withName("LIKES"));
			n1Id = n1.getId();
			n2Id = n2.getId();
			tx.success();
		}
    }
    
    protected File getNeoPath() {
        return new File(dbPath.getAbsolutePath(), Long.toString(storePrefix));
    }

    private void deleteDatabase() {
        try {
            FileUtils.deleteRecursively(getNeoPath());
        } catch (IOException e) {
            System.out.println("Failed to delete database: "+e);
            e.printStackTrace();
        }
    }

	private void readNames() {
		Node n1 = graphDb.getNodeById(n1Id);
		Node n2 = graphDb.getNodeById(n2Id);
		String n1Name = (String) n1.getProperty("name");
		String n2Name = (String) n2.getProperty("name");
		System.out.println("First node: " + n1Name);
		System.out.println("Second node: " + n2Name);
		assertEquals("Name does not match", n1Name, "n1");
		assertEquals("Name does not match", n2Name, "n2");
	}

	private void readNamesWithNestedTransaction(boolean outer, boolean inner) {
		try (Transaction tx_outer = graphDb.beginTx()) {
			try (Transaction tx_inner = graphDb.beginTx()) {
				readNames();
				if (inner) {
					tx_inner.success();
				}
			}
			if (outer) {
				tx_outer.success();
			}
		}
	}

	@Test
	public void testNormalTransaction() {
		try (Transaction tx = graphDb.beginTx()) {
			readNames();
		}
	}

	@Test
	public void testNestedTransactionFF() {
		readNamesWithNestedTransaction(false, false);
	}

	@Test
	public void testNestedTransactionSS() {
		readNamesWithNestedTransaction(true, true);
	}

	@Test
	public void testNestedTransactionFS() {
		readNamesWithNestedTransaction(false, true);
	}

	@Test
	public void testNestedTransactionSF() {
		try {
			readNamesWithNestedTransaction(true, false);
		} catch (Exception e) {
			assertEquals("Expected transaction failure from RollbackException",
				"Transaction rolled back even if marked as successful", e.getCause().getMessage());
		}
	}
}
