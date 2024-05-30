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
package org.neo4j.gis.spatial.rtree;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.neo4j.Neo4jFeatureBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class RTreeTests {

	private static final boolean exportImages = false;    // TODO: This can be enabled once we port to newer GeoTools that works with Java11
	private DatabaseManagementService databases;
	private GraphDatabaseService db;
	private TestRTreeIndex rtree;
	private RTreeImageExporter imageExporter;

	@BeforeEach
	public void setup() {
		databases = new TestDatabaseManagementServiceBuilder(Path.of("target", "rtree")).impermanent().build();
		db = databases.database(DEFAULT_DATABASE_NAME);
		try (Transaction tx = db.beginTx()) {
			this.rtree = new TestRTreeIndex(tx);
			tx.commit();
		}
		if (exportImages) {
			SimpleFeatureType featureType = Neo4jFeatureBuilder.getType("test", Constants.GTYPE_POINT, null,
					new String[]{});
			imageExporter = new RTreeImageExporter(new GeometryFactory(), new SimplePointEncoder(), null, featureType,
					rtree);
			try (Transaction tx = db.beginTx()) {
				imageExporter.initialize(tx, new Coordinate(0.0, 0.0), new Coordinate(1.0, 1.0));
				tx.commit();
			}
		}
	}

	@AfterEach
	public void teardown() {
		databases.shutdown();
	}

	@Test
	public void shouldMergeTwoPartiallyOverlappingTrees() throws IOException {
		RTreeIndex.NodeWithEnvelope rootLeft;
		RTreeIndex.NodeWithEnvelope rootRight;
		try (Transaction tx = db.beginTx()) {
			rootLeft = createSimpleRTree(0.01, 0.81, 5);
			tx.commit();
		}
		try (Transaction tx = db.beginTx()) {
			rootRight = createSimpleRTree(0.19, 0.99, 5);
			tx.commit();
		}
		System.out.println("Created two trees");
		if (exportImages) {
			try (Transaction tx = db.beginTx()) {
				imageExporter.saveRTreeLayers(tx, new File("target/rtree-test/rtree-left.png"), rootLeft.node, 7);
				imageExporter.saveRTreeLayers(tx, new File("target/rtree-test/rtree-right.png"), rootRight.node, 7);
				tx.commit();
			}
		}
		try (Transaction tx = db.beginTx()) {
			rtree.mergeTwoTrees(tx, rootLeft.refresh(tx), rootRight.refresh(tx));
			tx.commit();
		}
		System.out.println("Merged two trees");
		if (exportImages) {
			try (Transaction tx = db.beginTx()) {
				imageExporter.saveRTreeLayers(tx, new File("target/rtree-test/rtree-merged.png"), rootLeft.node, 7);
				tx.commit();
			}
		}
	}

	@SuppressWarnings("SameParameterValue")
	private RTreeIndex.NodeWithEnvelope createSimpleRTree(double minx, double maxx, int depth) {
		double[] min = new double[]{minx, minx};
		double[] max = new double[]{maxx, maxx};
		try (Transaction tx = db.beginTx()) {
			RTreeIndex.NodeWithEnvelope rootNode = new RTreeIndex.NodeWithEnvelope(tx.createNode(),
					new Envelope(min, max));
			TestRTreeIndex.setIndexNodeEnvelope(rootNode);
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
						RTreeIndex.NodeWithEnvelope child = TestRTreeIndex.makeChildIndexNode(tx, parent, env);
						children.add(child);
					}
				}
				parents.clear();
				parents.addAll(children);
				children.clear();
			}
			tx.commit();
			return rootNode;
		}
	}

	@SuppressWarnings("SameParameterValue")
	static Envelope makeEnvelope(Envelope parent, double scaleFactor, double offsetX, double offsetY) {
		Envelope env = new Envelope(parent);
		env.scaleBy(scaleFactor);
		env.shiftBy(offsetX * env.getWidth(0), 0);
		env.shiftBy(offsetY * env.getWidth(1), 1);
		return env;
	}
}
