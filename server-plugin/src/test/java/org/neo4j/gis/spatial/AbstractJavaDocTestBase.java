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

import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.doc.tools.JavaTestDocsGenerator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.GraphDatabaseServiceCleaner;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.TestData;

/**
 * This class was copied from the class of the same name in neo4j-examples, in order to reduce the dependency chain
 */
public abstract class AbstractJavaDocTestBase implements GraphHolder {

	@RegisterExtension
	public TestData<Map<String, Node>> data = TestData.producedThrough(GraphDescription.createGraphFor(this));
	@RegisterExtension
	public TestData<JavaTestDocsGenerator> gen = TestData.producedThrough(JavaTestDocsGenerator.PRODUCER);
	protected static DatabaseManagementService databases;
	protected static GraphDatabaseService db;

	@AfterAll
	public static void shutdownDb() {
		try {
			if (databases != null) {
				databases.shutdown();
			}
		} finally {
			databases = null;
			db = null;
		}

	}

	@Override
	public GraphDatabaseService graphdb() {
		return db;
	}

	@BeforeEach
	public void setUp() {
		GraphDatabaseService graphdb = this.graphdb();
		GraphDatabaseServiceCleaner.cleanDatabaseContent(graphdb);
		this.gen.get().setGraph(graphdb);
	}

	@AfterEach
	public void doc() {
		this.gen.get().document("target/docs/dev", "examples");
	}
}
