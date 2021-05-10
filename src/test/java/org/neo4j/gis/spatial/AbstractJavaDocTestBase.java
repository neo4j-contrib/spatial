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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.doc.tools.JavaTestDocsGenerator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.GraphDatabaseServiceCleaner;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.TestData;

import java.util.Map;

/**
 * This class was copied from the class of the same name in neo4j-examples, in order to reduce the dependency chain
 */
public abstract class AbstractJavaDocTestBase implements GraphHolder {
    @Rule
    public TestData<JavaTestDocsGenerator> gen;
    @Rule
    public TestData<Map<String, Node>> data;
    protected static DatabaseManagementService databases;
    protected static GraphDatabaseService db;

    public AbstractJavaDocTestBase() {
        this.gen = TestData.producedThrough(JavaTestDocsGenerator.PRODUCER);
        this.data = TestData.producedThrough(GraphDescription.createGraphFor(this));
    }

    @AfterClass
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

    public GraphDatabaseService graphdb() {
        return db;
    }

    @Before
    public void setUp() {
        GraphDatabaseService graphdb = this.graphdb();
        GraphDatabaseServiceCleaner.cleanDatabaseContent(graphdb);
        this.gen.get().setGraph(graphdb);
    }

    @After
    public void doc() {
        this.gen.get().document("target/docs/dev", "examples");
    }
}
