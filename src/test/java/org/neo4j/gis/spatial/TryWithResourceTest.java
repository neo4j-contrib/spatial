package org.neo4j.gis.spatial;

import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@SuppressWarnings("ConstantConditions")
public class TryWithResourceTest {

    public static final String MESSAGE = "I want to see this";

    @Test
    public void testSuppressedException() {
        try {
            DatabaseManagementService databases = new TestDatabaseManagementServiceBuilder(new File("target/resource").toPath()).impermanent().build();
            GraphDatabaseService db = databases.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = db.beginTx()) {
                Node n = tx.createNode();
                try (Transaction tx2 = db.beginTx()) {
                    n.setProperty("foo", "bar");
                    if (true) throw new Exception(MESSAGE);
                    tx2.commit();
                }
                tx.commit();
            } finally {
                databases.shutdown();
            }
        } catch (Exception e) {
            assertEquals(MESSAGE, e.getMessage());
        }
    }

    @Test
    public void testSuppressedExceptionTopLevel() {
        try {
            DatabaseManagementService databases = new TestDatabaseManagementServiceBuilder(new File("target/resource").toPath()).impermanent().build();
            GraphDatabaseService db = databases.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = db.beginTx()) {
                Node n = tx.createNode();
                n.setProperty("foo", "bar");
                if (true) throw new Exception(MESSAGE);
                tx.commit();
            } finally {
                databases.shutdown();
            }
        } catch (Exception e) {
            assertEquals(MESSAGE, e.getMessage());
        }
    }
}
