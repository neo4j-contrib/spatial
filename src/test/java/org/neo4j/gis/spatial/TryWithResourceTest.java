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
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

@SuppressWarnings("ConstantConditions")
public class TryWithResourceTest {

	public static final String MESSAGE = "I want to see this";

	@Test
	public void testSuppressedException() {
		try {
			DatabaseManagementService databases = new TestDatabaseManagementServiceBuilder(
					new File("target/resource").toPath()).impermanent().build();
			GraphDatabaseService db = databases.database(DEFAULT_DATABASE_NAME);
			try (Transaction tx = db.beginTx()) {
				Node n = tx.createNode();
				try (Transaction tx2 = db.beginTx()) {
					n.setProperty("foo", "bar");
					if (true) {
						throw new Exception(MESSAGE);
					}
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
			DatabaseManagementService databases = new TestDatabaseManagementServiceBuilder(
					new File("target/resource").toPath()).impermanent().build();
			GraphDatabaseService db = databases.database(DEFAULT_DATABASE_NAME);
			try (Transaction tx = db.beginTx()) {
				Node n = tx.createNode();
				n.setProperty("foo", "bar");
				if (true) {
					throw new Exception(MESSAGE);
				}
				tx.commit();
			} finally {
				databases.shutdown();
			}
		} catch (Exception e) {
			assertEquals(MESSAGE, e.getMessage());
		}
	}
}
