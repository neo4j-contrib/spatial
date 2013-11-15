package org.neo4j.gis.spatial;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 * @author Michael Hunger @since 24.10.13
 */
public class TryWithResourceTest {

    public static final String MESSAGE = "I want to see this";

    @Test
    public void testSuppressedException() throws Exception {
        try {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try (Transaction tx = db.beginTx()) {
            Node n = db.createNode();
            try (Transaction tx2 = db.beginTx()) {
                n.setProperty("foo","bar");
                if (true) throw new Exception(MESSAGE);
                tx2.success();
            }
            tx.success();
        }
        } catch(Exception e) {
            assertEquals(MESSAGE,e.getMessage());
        }
    }

    @Test
    public void testSuppressedExceptionTopLevel() throws Exception {
        try {
            GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
            try (Transaction tx = db.beginTx()) {
                Node n = db.createNode();
                n.setProperty("foo", "bar");
                if (true) throw new Exception(MESSAGE);
                tx.success();
            }
        } catch (Exception e) {
            assertEquals(MESSAGE, e.getMessage());
        }
    }
}
