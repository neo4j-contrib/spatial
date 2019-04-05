/**
 * Copyright (c) 2002-2013 "Neo Technology," Network Engine for Objects in Lund
 * AB [http://neotechnology.com]
 *
 * This file is part of Neo4j Spatial.
 *
 * Neo4j is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.rtree;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.geotools.data.neo4j.Neo4jFeatureBuilder;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class RTreeTests {

    private GraphDatabaseService db;
    private TestRTreeIndex rtree;
    private RTreeImageExporter imageExporter;

    @Before
    public void setup() {
        this.db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try (Transaction tx = db.beginTx()) {
            this.rtree = new TestRTreeIndex(this.db);
            tx.success();
        }
        Coordinate min = new Coordinate(0.0, 0.0);
        Coordinate max = new Coordinate(1.0, 1.0);
        SimpleFeatureType featureType = Neo4jFeatureBuilder.getType("test", Constants.GTYPE_POINT, null, new String[]{});
        imageExporter = new RTreeImageExporter(new GeometryFactory(), new SimplePointEncoder(), null, featureType, rtree, min, max);
    }

    @Test
    public void shouldMergeTwoPartiallyOverlappingTrees() throws IOException {
        RTreeIndex.NodeWithEnvelope rootLeft;
        RTreeIndex.NodeWithEnvelope rootRight;
        try (Transaction tx = db.beginTx()) {
            rootLeft = createSimpleRTree(0.01, 0.81, 5);
            tx.success();
        }
        try (Transaction tx = db.beginTx()) {
            rootRight = createSimpleRTree(0.19, 0.99, 5);
            tx.success();
        }
        System.out.println("Created two trees");
        try (Transaction tx = db.beginTx()) {
            imageExporter.saveRTreeLayers(new File("target/rtree-test/rtree-left.png"), rootLeft.node, 7);
            imageExporter.saveRTreeLayers(new File("target/rtree-test/rtree-right.png"), rootRight.node, 7);
            tx.success();
        }
        try (Transaction tx = db.beginTx()) {
            rtree.mergeTwoTrees(rootLeft, rootRight);
            tx.success();
        }
        System.out.println("Merged two trees");
        try (Transaction tx = db.beginTx()) {
            imageExporter.saveRTreeLayers(new File("target/rtree-test/rtree-merged.png"), rootLeft.node, 7);
            tx.success();
        }
    }

    private RTreeIndex.NodeWithEnvelope createSimpleRTree(double minx, double maxx, int depth) {
        double[] min = new double[]{minx, minx};
        double[] max = new double[]{maxx, maxx};
        try (Transaction tx = db.beginTx()) {
            RTreeIndex.NodeWithEnvelope rootNode = new RTreeIndex.NodeWithEnvelope(db.createNode(), new Envelope(min, max));
            rtree.setIndexNodeEnvelope(rootNode);
            ArrayList<RTreeIndex.NodeWithEnvelope> parents = new ArrayList<>();
            ArrayList<RTreeIndex.NodeWithEnvelope> children = new ArrayList<>();
            parents.add(rootNode);
            for (int i = 1; i < depth; i++) {
                for (RTreeIndex.NodeWithEnvelope parent : parents) {
                    Envelope[] envs = new Envelope[]{
                            makeEnvelope(parent.envelope, 0.5, 0.0, 0.0),
                            makeEnvelope(parent.envelope, 0.5, 1.0, 0.0),
                            makeEnvelope(parent.envelope, 0.5, 1.0, 1.0),
                            makeEnvelope(parent.envelope, 0.5, 0.0, 1.0)
                    };
                    for (Envelope env : envs) {
                        RTreeIndex.NodeWithEnvelope child = rtree.makeChildIndexNode(parent, env);
                        children.add(child);
                    }
                }
                parents.clear();
                parents.addAll(children);
                children.clear();
            }
            tx.success();
            return rootNode;
        }
    }

    Envelope makeEnvelope(Envelope parent, double scaleFactor, double offsetX, double offsetY) {
        Envelope env = new Envelope(parent);
        env.scaleBy(0.5);
        env.shiftBy(offsetX * env.getWidth(0), 0);
        env.shiftBy(offsetY * env.getWidth(1), 1);
        return env;
    }
}
