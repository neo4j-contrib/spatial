/**
 * Copyright (c) 2010-2016 "Neo Technology,"
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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.neo4j.cypher.internal.compiler.v3_2.prettifier.Prettifier;
import org.neo4j.doc.tools.JavaTestDocsGenerator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.*;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

import java.util.Map;

/**
 * This class was copied from the class of the same name in neo4j-examples, in order to reduce the dependency chain
 */
public abstract class AbstractJavaDocTestBase implements GraphHolder {
    @Rule
    public TestData<JavaTestDocsGenerator> gen;
    @Rule
    public TestData<Map<String, Node>> data;
    protected static GraphDatabaseService db;

    public AbstractJavaDocTestBase() {
        this.gen = TestData.producedThrough(JavaTestDocsGenerator.PRODUCER);
        this.data = TestData.producedThrough(GraphDescription.createGraphFor(this, true));
    }

    @AfterClass
    public static void shutdownDb() {
        try {
            if(db != null) {
                db.shutdown();
            }
        } finally {
            db = null;
        }

    }

    public GraphDatabaseService graphdb() {
        return db;
    }

    protected String createCypherSnippet(String cypherQuery) {
        String snippet = Prettifier.apply(cypherQuery);
        return AsciidocHelper.createAsciiDocSnippet("cypher", snippet);
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
