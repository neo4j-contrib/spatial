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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

/**
 * This test was written to test the subtle behavior of nested transactions in the Neo4j 1.x-3.x code.
 * However, in Neo4j 4.0 transaction nesting was removed, and it is no longer possible to create inner transactions.
 * Entities passed around can also no longer be used to obtain access to the database API's to make new transactions.
 * Instead all transactions need to be passed down the call stack to all places they are needed.
 * <p>
 * // TODO: Consider deleting this test as it probably no longer makes sense in Neo4j 4.0
 */
public class TestReadOnlyTransactions {

	private DatabaseManagementService databases;
	private GraphDatabaseService graph;
	private static final Path basePath = new File("target/var").toPath();
	private static final String dbPrefix = "neo4j-db";

	private static long storePrefix;

	private static String n1Id;
	private static String n2Id;

	@BeforeEach
	public void setUp() throws Exception {
		storePrefix++;
		System.out.println("Creating store at " + dbPrefix + storePrefix);
		this.databases = new TestDatabaseManagementServiceBuilder(
				basePath.resolve(dbPrefix + storePrefix)).impermanent().build();
		this.graph = databases.database(DEFAULT_DATABASE_NAME);
		buildDataModel();
	}

	@AfterEach
	public void tearDown() {
		databases.shutdown();
		try {
			FileUtils.deleteDirectory(basePath);
		} catch (IOException e) {
			System.out.println("Failed to delete database: " + e);
			e.printStackTrace();
		}
	}

	private void buildDataModel() {
		try (Transaction tx = graph.beginTx()) {
			Node n1 = tx.createNode();
			n1.setProperty("name", "n1");
			Node n2 = tx.createNode();
			n2.setProperty("name", "n2");
			n1.createRelationshipTo(n2, RelationshipType.withName("LIKES"));
			n1Id = n1.getElementId();
			n2Id = n2.getElementId();
			tx.commit();
		}
	}

	private static void readNames(Transaction tx) {
		Node n1 = tx.getNodeByElementId(n1Id);
		Node n2 = tx.getNodeByElementId(n2Id);
		String n1Name = (String) n1.getProperty("name");
		String n2Name = (String) n2.getProperty("name");
		System.out.println("First node: " + n1Name);
		System.out.println("Second node: " + n2Name);
		assertEquals("n1", n1Name, "Name does not match");
		assertEquals("n2", n2Name, "Name does not match");
	}

	private void readNamesWithNestedTransaction(boolean outer, boolean inner) {
		try (Transaction tx_outer = graph.beginTx()) {
			try (Transaction tx_inner = graph.beginTx()) {
				readNames(tx_inner);
				if (inner) {
					tx_inner.commit();
				}
			}
			if (outer) {
				tx_outer.commit();
			}
		}
	}

	@Test
	public void testNormalTransaction() {
		try (Transaction tx = graph.beginTx()) {
			readNames(tx);
			tx.commit();
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
			assertEquals("Transaction rolled back even if marked as successful", e.getCause().getMessage(),
					"Expected transaction failure from RollbackException");
		}
	}
}
