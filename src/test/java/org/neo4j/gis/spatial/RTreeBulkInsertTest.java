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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.gis.spatial.rtree.RTreeIndex.DEFAULT_MAX_NODE_REFERENCES;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.commons.io.FileUtils;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.data.neo4j.Neo4jFeatureBuilder;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.index.ExplicitIndexBackedMonitor;
import org.neo4j.gis.spatial.index.ExplicitIndexBackedPointIndex;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.index.LayerGeohashPointIndex;
import org.neo4j.gis.spatial.index.LayerHilbertPointIndex;
import org.neo4j.gis.spatial.index.LayerIndexReader;
import org.neo4j.gis.spatial.index.LayerZOrderPointIndex;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.RTreeImageExporter;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.gis.spatial.rtree.RTreeMonitor;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.gis.spatial.rtree.TreeMonitor;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class RTreeBulkInsertTest {

	private DatabaseManagementService databases;
	private GraphDatabaseService db;
	private final File storeDir = new File("target/store").getAbsoluteFile();

	// While the current lucene index implmentation is so slow (16n/s) we disable all benchmarks for lucene backed indexes
	private static final boolean enableLucene = false;

	@BeforeEach
	public void before() throws IOException {
		restart();
	}

	@AfterEach
	public void after() {
		doCleanShutdown();
	}

	@Disabled
	@Test
	public void shouldDeleteRecursiveTree() {
		int depth = 5;
		int width = 2;
		//Create nodes
		ArrayList<ArrayList<Node>> nodes = new ArrayList<>();
		try (Transaction tx = db.beginTx()) {
			nodes.add(new ArrayList<>());
			nodes.get(0).add(tx.createNode());
			nodes.get(0).get(0).setProperty("name", "0-0");

			for (int i = 1; i < depth; i++) {
				ArrayList<Node> children = new ArrayList<>();
				nodes.add(children);
				for (Node parent : nodes.get(i - 1)) {
					for (int j = 0; j < width; j++) {
						Node node = tx.createNode();
						node.setProperty("name", i + "-" + j);
						parent.createRelationshipTo(node, RTreeRelationshipTypes.RTREE_CHILD);
						children.add(node);
					}
				}
			}
			debugRest();
			//Disconact leafs
			ArrayList<Node> leaves = nodes.get(nodes.size() - 1);
			for (Node leaf : leaves) {
				leaf.getSingleRelationship(RTreeRelationshipTypes
						.RTREE_CHILD, Direction.INCOMING).delete();

			}

			deleteRecursivelySubtree(nodes.get(0).get(0), null);
			System.out.println("Leaf");
			nodes.get(nodes.size() - 1).forEach(System.out::println);
			tx.commit();
		}
		debugRest();
	}

	private void debugRest() {
		try (Transaction tx = db.beginTx()) {
			Result result = tx.execute("MATCH (n) OPTIONAL MATCH (n)-[r]-() RETURN elementId(n), n.name, count(r)");
			while (result.hasNext()) {
				System.out.println(result.next());
			}
			tx.commit();
		}
	}

	private static void deleteRecursivelySubtree(Node node, Relationship incoming) {
		for (Relationship relationship : node.getRelationships(Direction.OUTGOING,
				RTreeRelationshipTypes.RTREE_CHILD)) {
			deleteRecursivelySubtree(relationship.getEndNode(), relationship);
		}
		if (incoming != null) {
			incoming.delete();
		}
//        Iterator<Relationship> itr = node.getRelationships().iterator();
//        while(itr.hasNext()){
//            itr.next().delete();
//        }
		System.out.println(node.getElementId());
		node.delete();
	}

	SpatialDatabaseService spatial() {
		return new SpatialDatabaseService(new IndexManager((GraphDatabaseAPI) db, SecurityContext.AUTH_DISABLED));
	}

	private EditableLayer getOrCreateSimplePointLayer(String name, String index, String xProperty, String yProperty) {
		CoordinateReferenceSystem crs = DefaultEngineeringCRS.GENERIC_2D;
		try (Transaction tx = db.beginTx()) {
			SpatialDatabaseService sdbs = spatial();
			EditableLayer layer = sdbs.getOrCreateSimplePointLayer(tx, name, index, xProperty, yProperty, null);
			layer.setCoordinateReferenceSystem(tx, crs);
			tx.commit();
			return layer;
		}
	}

	@Disabled
	@Test
	public void shouldInsertSimpleRTree() {
		int width = 20;
		EditableLayer layer = getOrCreateSimplePointLayer("Coordinates", "rtree", "lon", "lat");
		List<String> nodes = new ArrayList<>();
		try (Transaction tx = db.beginTx()) {
			for (int i = 0; i < width; i++) {
				Node node = tx.createNode();
				node.addLabel(Label.label("Coordinates"));
				node.setProperty("lat", i);
				node.setProperty("lon", 0);
				nodes.add(node.getElementId());
				node.toString();
			}
			tx.commit();
		}
//        java.util.Collections.shuffle( nodes,new Random( 1 ) );
		TreeMonitor monitor = new RTreeMonitor();
		layer.getIndex().addMonitor(monitor);
		long start = System.currentTimeMillis();

		List<String> list1 = nodes.subList(0, nodes.size() / 2 + 8);
		List<String> list2 = nodes.subList(list1.size(), nodes.size());
		System.out.println(list1);
		System.out.println(list2);
		try (Transaction tx = db.beginTx()) {
			layer.addAll(tx, idsToNodes(tx, list1));
			tx.commit();
		}
		Neo4jTestUtils.debugIndexTree(db, "Coordinates");
		//TODO add this part to the test
//        try (Transaction tx = db.beginTx()) {
//            layer.addAll(list2);
//            tx.commit();
//        }

		System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + (width * width)
				+ " nodes to RTree in bulk");

//        queryRTree(layer);
//        verifyTreeStructure(layer);
		Neo4jTestUtils.debugIndexTree(db, "Coordinates");

	}

	private static List<Node> idsToNodes(Transaction tx, List<String> nodeIds) {
		return nodeIds.stream().map(tx::getNodeByElementId).collect(Collectors.toList());
	}

	private static class IndexTestConfig {

		String name;
		int width;
		Coordinate searchMin;
		Coordinate searchMax;
		long totalCount;
		long expectedCount;
		long expectedGeometries;

		/*
		 * Collection of test settings to perform assertions on.
		 * Note that due to some crazy GIS spec points on polygon edges are considered to be contained,
		 * unless the polygon is a rectangle, in which case they are not contained, leading to
		 * different numbers for expectedGeometries and expectedCount,
		 * See https://github.com/locationtech/jts/blob/master/modules/core/src/main/java/org/locationtech/jts/operation/predicate/RectangleContains.java#L70
		 */
		public IndexTestConfig(String name, int width, Coordinate searchMin, Coordinate searchMax, long expectedCount,
				long expectedGeometries) {
			this.name = name;
			this.width = width;
			this.searchMin = searchMin;
			this.searchMax = searchMax;
			this.expectedCount = expectedCount;
			this.expectedGeometries = expectedGeometries;
			this.totalCount = (long) width * width;
		}
	}

	private static final Map<String, IndexTestConfig> testConfigs = new HashMap<>();

	static {
		Coordinate searchMin = new Coordinate(0.5, 0.5);
		Coordinate searchMax = new Coordinate(0.52, 0.52);
		addTestConfig(new IndexTestConfig("very_small", 100, searchMin, searchMax, 9, 1));
		addTestConfig(new IndexTestConfig("small", 250, searchMin, searchMax, 35, 16));
		addTestConfig(new IndexTestConfig("medium", 500, searchMin, searchMax, 121, 81));
		addTestConfig(new IndexTestConfig("large", 750, searchMin, searchMax, 256, 196));
	}

	private static void addTestConfig(IndexTestConfig config) {
		testConfigs.put(config.name, config);
	}

	private interface IndexMaker {

		EditableLayer setupLayer(Transaction tx);

		List<String> nodes();

		TestStats initStats(int blockSize);

		TimedLogger initLogger();

		IndexTestConfig getConfig();

		void verifyStructure();
	}

	private class GeohashIndexMaker implements IndexMaker {

		private final String name;
		private final String insertMode;
		private final IndexTestConfig config;
		private List<String> nodes;
		private EditableLayer layer;

		private GeohashIndexMaker(String name, String insertMode, IndexTestConfig config) {
			this.name = name;
			this.insertMode = insertMode;
			this.config = config;
		}

		@Override
		public EditableLayer setupLayer(Transaction tx) {
			this.nodes = setup(name, "geohash", config.width);
			this.layer = (EditableLayer) spatial().getLayer(tx, "Coordinates");
			return layer;
		}

		@Override
		public List<String> nodes() {
			return nodes;
		}

		@Override
		public TestStats initStats(int blockSize) {
			return new TestStats(config, insertMode, "Geohash", blockSize, -1);
		}

		@Override
		public TimedLogger initLogger() {
			return new TimedLogger(
					"Inserting " + config.totalCount + " nodes into Geohash using " + insertMode + " insert",
					config.totalCount);
		}

		@Override
		public IndexTestConfig getConfig() {
			return config;
		}

		@Override
		public void verifyStructure() {
			verifyGeohashIndex(layer);
		}
	}

	private class ZOrderIndexMaker implements IndexMaker {

		private final String name;
		private final String insertMode;
		private final IndexTestConfig config;
		private List<String> nodes;
		private EditableLayer layer;

		private ZOrderIndexMaker(String name, String insertMode, IndexTestConfig config) {
			this.name = name;
			this.insertMode = insertMode;
			this.config = config;
		}

		@Override
		public EditableLayer setupLayer(Transaction tx) {
			this.nodes = setup(name, "zorder", config.width);
			this.layer = (EditableLayer) spatial().getLayer(tx, "Coordinates");
			return layer;
		}

		@Override
		public List<String> nodes() {
			return nodes;
		}

		@Override
		public TestStats initStats(int blockSize) {
			return new TestStats(config, insertMode, "Z-Order", blockSize, -1);
		}

		@Override
		public TimedLogger initLogger() {
			return new TimedLogger(
					"Inserting " + config.totalCount + " nodes into Z-Order using " + insertMode + " insert",
					config.totalCount);
		}

		@Override
		public IndexTestConfig getConfig() {
			return config;
		}

		@Override
		public void verifyStructure() {
			verifyZOrderIndex(layer);
		}
	}

	private class HilbertIndexMaker implements IndexMaker {

		private final String name;
		private final String insertMode;
		private final IndexTestConfig config;
		private List<String> nodes;
		private EditableLayer layer;

		private HilbertIndexMaker(String name, String insertMode, IndexTestConfig config) {
			this.name = name;
			this.insertMode = insertMode;
			this.config = config;
		}

		@Override
		public EditableLayer setupLayer(Transaction tx) {
			this.nodes = setup(name, "hilbert", config.width);
			this.layer = (EditableLayer) spatial().getLayer(tx, "Coordinates");
			return layer;
		}

		@Override
		public List<String> nodes() {
			return nodes;
		}

		@Override
		public TestStats initStats(int blockSize) {
			return new TestStats(config, insertMode, "Hilbert", blockSize, -1);
		}

		@Override
		public TimedLogger initLogger() {
			return new TimedLogger(
					"Inserting " + config.totalCount + " nodes into Hilbert using " + insertMode + " insert",
					config.totalCount);
		}

		@Override
		public IndexTestConfig getConfig() {
			return config;
		}

		@Override
		public void verifyStructure() {
			verifyHilbertIndex(layer);
		}
	}

	private class RTreeIndexMaker implements IndexMaker {

		private final SpatialDatabaseService spatial = spatial();
		private final String splitMode;
		private final String insertMode;
		private final boolean shouldMergeTrees;
		private final int maxNodeReferences;
		private final IndexTestConfig config;
		private final String name;
		private EditableLayer layer;
		private TestStats stats;
		private List<String> nodes;

		private RTreeIndexMaker(String name, String splitMode, String insertMode, int maxNodeReferences,
				IndexTestConfig config) {
			this(name, splitMode, insertMode, maxNodeReferences, config, false);
		}

		private RTreeIndexMaker(String name, String splitMode, String insertMode, int maxNodeReferences,
				IndexTestConfig config, boolean shouldMergeTrees) {
			this.name = name;
			this.splitMode = splitMode;
			this.insertMode = insertMode;
			this.shouldMergeTrees = shouldMergeTrees;
			this.maxNodeReferences = maxNodeReferences;
			this.config = config;
		}

		@Override
		public EditableLayer setupLayer(Transaction tx) {
			this.nodes = setup(name, "rtree", config.width);
			this.layer = (EditableLayer) spatial.getLayer(tx, name);
			layer.getIndex().configure(Map.of(
					RTreeIndex.KEY_SPLIT, splitMode,
					RTreeIndex.KEY_MAX_NODE_REFERENCES, maxNodeReferences,
					RTreeIndex.KEY_SHOULD_MERGE_TREES, shouldMergeTrees)
			);
			return layer;
		}

		@Override
		public List<String> nodes() {
			return nodes;
		}

		@Override
		public TestStats initStats(int blockSize) {
			this.stats = new TestStats(config, insertMode, splitMode, blockSize, maxNodeReferences);
			return this.stats;
		}

		@Override
		public TimedLogger initLogger() {
			return new TimedLogger(
					"Inserting " + config.totalCount + " nodes into RTree using " + insertMode + " insert and "
							+ splitMode + " split with " + maxNodeReferences + " maxNodeReferences", config.totalCount);
		}

		@Override
		public IndexTestConfig getConfig() {
			return config;
		}

		@Override
		public void verifyStructure() {
			verifyTreeStructure(layer, splitMode, stats);
		}

	}

	/*
	 * Very small model 100*100 nodes
	 */

	@Test
	public void shouldInsertManyNodesIndividuallyWithGeohash_very_small() {
		insertManyNodesIndividually(new GeohashIndexMaker("Coordinates", "Single", testConfigs.get("very_small")),
				5000);
	}

	@Test
	public void shouldInsertManyNodesInBulkWithGeohash_very_small() {
		insertManyNodesInBulk(new GeohashIndexMaker("Coordinates", "Bulk", testConfigs.get("very_small")), 5000);
	}

	@Test
	public void shouldInsertManyNodesIndividuallyWithZOrder_very_small() {
		insertManyNodesIndividually(new ZOrderIndexMaker("Coordinates", "Single", testConfigs.get("very_small")), 5000);
	}

	@Test
	public void shouldInsertManyNodesInBulkWithZOrder_very_small() {
		insertManyNodesInBulk(new ZOrderIndexMaker("Coordinates", "Bulk", testConfigs.get("very_small")), 5000);
	}

	@Test
	public void shouldInsertManyNodesIndividuallyWithHilbert_very_small() {
		insertManyNodesIndividually(new HilbertIndexMaker("Coordinates", "Single", testConfigs.get("very_small")),
				5000);
	}

	@Test
	public void shouldInsertManyNodesInBulkWithHilbert_very_small() {
		insertManyNodesInBulk(new HilbertIndexMaker("Coordinates", "Bulk", testConfigs.get("very_small")), 5000);
	}

	@Test
	public void shouldInsertManyNodesIndividuallyWithQuadraticSplit_very_small_10() {
		insertManyNodesIndividually(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("very_small"));
	}

	@Test
	public void shouldInsertManyNodesIndividuallyGreenesSplit_very_small_10() {
		insertManyNodesIndividually(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("very_small"));
	}

	@Test
	public void shouldInsertManyNodesInBulkWithQuadraticSplit_very_small_10() {
		insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("very_small"));
	}

	@Test
	public void shouldInsertManyNodesInBulkWithGreenesSplit_very_small_10() {
		insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("very_small"));
	}

	/*
	 * Small model 250*250 nodes
	 */

	@Test
	public void shouldInsertManyNodesIndividuallyWithGeohash_small() {
		insertManyNodesIndividually(new GeohashIndexMaker("Coordinates", "Single", testConfigs.get("small")), 5000);
	}

	@Test
	public void shouldInsertManyNodesInBulkWithGeohash_small() {
		insertManyNodesInBulk(new GeohashIndexMaker("Coordinates", "Bulk", testConfigs.get("small")), 5000);
	}

	@Test
	public void shouldInsertManyNodesIndividuallyWithZOrder_small() {
		insertManyNodesIndividually(new ZOrderIndexMaker("Coordinates", "Single", testConfigs.get("small")), 5000);
	}

	@Test
	public void shouldInsertManyNodesInBulkWithZOrder_small() {
		insertManyNodesInBulk(new ZOrderIndexMaker("Coordinates", "Bulk", testConfigs.get("small")), 5000);
	}

	@Test
	public void shouldInsertManyNodesIndividuallyWithHilbert_small() {
		insertManyNodesIndividually(new HilbertIndexMaker("Coordinates", "Single", testConfigs.get("small")), 5000);
	}

	@Test
	public void shouldInsertManyNodesInBulkWithHilbert_small() {
		insertManyNodesInBulk(new HilbertIndexMaker("Coordinates", "Bulk", testConfigs.get("small")), 5000);
	}

	@Disabled // takes too long, change to @Test when benchmarking
	@Test
	public void shouldInsertManyNodesIndividuallyWithQuadraticSplit_small_10() {
		insertManyNodesIndividually(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("small"));
	}

	@Disabled // takes too long, change to @Test when benchmarking
	@Test
	public void shouldInsertManyNodesIndividuallyGreenesSplit_small_10() {
		insertManyNodesIndividually(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("small"));
	}

	@Test
	public void shouldInsertManyNodesInBulkWithQuadraticSplit_small_10() {
		insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("small"));
	}

	@Test
	public void shouldInsertManyNodesInBulkWithGreenesSplit_small_10() {
		insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("small"));
	}

	/*
	 * Small model 250*250 nodes (shallow tree)
	 */

	@Disabled // takes too long, change to @Test when benchmarking
	@Test
	public void shouldInsertManyNodesIndividuallyWithQuadraticSplit_small_100() {
		insertManyNodesIndividually(RTreeIndex.QUADRATIC_SPLIT, 5000, 100, testConfigs.get("small"));
	}

	@Disabled // takes too long, change to @Test when benchmarking
	@Test
	public void shouldInsertManyNodesIndividuallyGreenesSplit_small_100() {
		insertManyNodesIndividually(RTreeIndex.GREENES_SPLIT, 5000, 100, testConfigs.get("small"));
	}

	@Test
	public void shouldInsertManyNodesInBulkWithQuadraticSplit_small_100() {
		insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 100, testConfigs.get("small"));
	}

	@Test
	public void shouldInsertManyNodesInBulkWithGreenesSplit_small_100() {
		insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 100, testConfigs.get("small"));
	}

	/*
	 * Medium model 500*500 nodes (deep tree - factor 10)
	 */

	@Test
	public void shouldInsertManyNodesIndividuallyWithGeohash_medium() {
		insertManyNodesIndividually(new GeohashIndexMaker("Coordinates", "Single", testConfigs.get("medium")), 5000);
	}

	@Test
	public void shouldInsertManyNodesInBulkWithGeohash_medium() {
		insertManyNodesInBulk(new GeohashIndexMaker("Coordinates", "Bulk", testConfigs.get("medium")), 5000);
	}

	@Test
	public void shouldInsertManyNodesIndividuallyWithZOrder_medium() {
		insertManyNodesIndividually(new ZOrderIndexMaker("Coordinates", "Single", testConfigs.get("medium")), 5000);
	}

	@Test
	public void shouldInsertManyNodesInBulkWithZOrder_medium() {
		insertManyNodesInBulk(new ZOrderIndexMaker("Coordinates", "Bulk", testConfigs.get("medium")), 5000);
	}

	@Test
	public void shouldInsertManyNodesIndividuallyWithHilbert_medium() {
		insertManyNodesIndividually(new HilbertIndexMaker("Coordinates", "Single", testConfigs.get("medium")), 5000);
	}

	@Test
	public void shouldInsertManyNodesInBulkWithHilbert_medium() {
		insertManyNodesInBulk(new HilbertIndexMaker("Coordinates", "Bulk", testConfigs.get("medium")), 5000);
	}

	@Disabled
	@Test
	public void shouldInsertManyNodesIndividuallyWithQuadraticSplit_medium_10() {
		insertManyNodesIndividually(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("medium"));
	}

	@Disabled
	@Test
	public void shouldInsertManyNodesIndividuallyGreenesSplit_medium_10() {
		insertManyNodesIndividually(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("medium"));
	}

	@Test
	public void shouldInsertManyNodesInBulkWithQuadraticSplit_medium_10() {
		insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("medium"));
	}

	@Test
	public void shouldInsertManyNodesInBulkWithGreenesSplit_medium_10() {
		insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("medium"));
	}

	@Disabled
	@Test
	public void shouldInsertManyNodesInBulkWithQuadraticSplit_medium_10_merge() {
		insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("medium"), true);
	}

	@Disabled
	@Test
	public void shouldInsertManyNodesInBulkWithGreenesSplit_medium_10_merge() {
		insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("medium"), true);
	}

	/*
	 * Medium model 500*500 nodes (shallow tree - factor 100)
	 */

	@Disabled
	@Test
	public void shouldInsertManyNodesIndividuallyWithQuadraticSplit_medium_100() {
		insertManyNodesIndividually(RTreeIndex.QUADRATIC_SPLIT, 5000, 100, testConfigs.get("medium"));
	}

	@Disabled
	@Test
	public void shouldInsertManyNodesIndividuallyGreenesSplit_medium_100() {
		insertManyNodesIndividually(RTreeIndex.GREENES_SPLIT, 5000, 100, testConfigs.get("medium"));
	}

	@Disabled // takes too long, change to @Test when benchmarking
	@Test
	public void shouldInsertManyNodesInBulkWithQuadraticSplit_medium_100() {
		insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 100, testConfigs.get("medium"));
	}

	@Test
	public void shouldInsertManyNodesInBulkWithGreenesSplit_medium_100() {
		insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 100, testConfigs.get("medium"));
	}

	@Disabled
	@Test
	public void shouldInsertManyNodesInBulkWithQuadraticSplit_medium_100_merge() {
		insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 100, testConfigs.get("medium"), true);
	}

	@Disabled
	@Test
	public void shouldInsertManyNodesInBulkWithGreenesSplit_medium_100_merge() {
		insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 100, testConfigs.get("medium"), true);
	}

	/*
	 * Large model 750*750 nodes (only test bulk insert, 100 and 10, green and quadratic)
	 */

	@Test
	public void shouldInsertManyNodesIndividuallyWithGeohash_large() {
		insertManyNodesIndividually(new GeohashIndexMaker("Coordinates", "Single", testConfigs.get("large")), 5000);
	}

	@Test
	public void shouldInsertManyNodesInBulkWithGeohash_large() {
		insertManyNodesInBulk(new GeohashIndexMaker("Coordinates", "Bulk", testConfigs.get("large")), 5000);
	}

	@Test
	public void shouldInsertManyNodesIndividuallyWithZOrder_large() {
		insertManyNodesIndividually(new ZOrderIndexMaker("Coordinates", "Single", testConfigs.get("large")), 5000);
	}

	@Test
	public void shouldInsertManyNodesInBulkWithZOrder_large() {
		insertManyNodesInBulk(new ZOrderIndexMaker("Coordinates", "Bulk", testConfigs.get("large")), 5000);
	}

	@Test
	public void shouldInsertManyNodesIndividuallyWithHilbert_large() {
		insertManyNodesIndividually(new HilbertIndexMaker("Coordinates", "Single", testConfigs.get("large")), 5000);
	}

	@Test
	public void shouldInsertManyNodesInBulkWithHilbert_large() {
		insertManyNodesInBulk(new HilbertIndexMaker("Coordinates", "Bulk", testConfigs.get("large")), 5000);
	}

	@Disabled // takes too long, change to @Test when benchmarking
	@Test
	public void shouldInsertManyNodesInBulkWithQuadraticSplit_large_10() {
		insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("large"));
	}

	@Disabled // takes too long, change to @Test when benchmarking
	@Test
	public void shouldInsertManyNodesInBulkWithGreenesSplit_large_10() {
		insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("large"));
	}

	@Disabled // takes too long, change to @Test when benchmarking
	@Test
	public void shouldInsertManyNodesInBulkWithQuadraticSplit_large_100() {
		insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 100, testConfigs.get("large"));
	}

	@Disabled // takes too long, change to @Test when benchmarking
	@Test
	public void shouldInsertManyNodesInBulkWithGreenesSplit_large_100() {
		insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 100, testConfigs.get("large"));
	}

	/*
	 * Private methods used by the above tests
	 */

	class TreePrintingMonitor extends RTreeMonitor {

		private final RTreeImageExporter imageExporter;
		private final String splitMode;
		private final String insertMode;
		private final HashMap<String, Integer> called = new HashMap<>();

		TreePrintingMonitor(RTreeImageExporter imageExporter, String insertMode, String splitMode) {
			this.imageExporter = imageExporter;
			this.splitMode = splitMode;
			this.insertMode = insertMode;
		}

		private Integer getCalled(String key) {
			if (!called.containsKey(key)) {
				called.put(key, 0);
			}
			return called.get(key);
		}

		@Override
		public void addNbrRebuilt(RTreeIndex rtree, Transaction tx) {
			super.addNbrRebuilt(rtree, tx);
			printRTreeImage("rebuilt", rtree.getIndexRoot(tx), new ArrayList<>());
		}

		@Override
		public void addSplit(Node indexNode) {
			super.addSplit(indexNode);
//            printRTreeImage("split", indexNode, new ArrayList<>());
		}

		@Override
		public void beforeMergeTree(Node indexNode, List<RTreeIndex.NodeWithEnvelope> right) {
			super.beforeMergeTree(indexNode, right);

			printRTreeImage("before-merge", indexNode,
					right.stream().map(e -> e.envelope).collect(Collectors.toList()));

		}

		@Override
		public void afterMergeTree(Node indexNode) {
			super.afterMergeTree(indexNode);
			printRTreeImage("after-merge", indexNode, new ArrayList<>());
		}


		private void printRTreeImage(String context, Node rootNode, List<Envelope> envelopes) {
			try (Transaction tx = db.beginTx()) {
				int count = getCalled(context);
				imageExporter.saveRTreeLayers(tx,
						new File("rtree-" + insertMode + "-" + splitMode + "/debug-" + context + "/rtree-" + count
								+ ".png"),
						rootNode, envelopes, 7);
				called.put(context, count + 1);
				tx.commit();
			} catch (IOException e) {
				System.out.println("Failed to print RTree to disk: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	private void insertManyNodesIndividually(String splitMode, int blockSize, int maxNodeReferences,
			IndexTestConfig config) {
		insertManyNodesIndividually(new RTreeIndexMaker("Coordinates", splitMode, "Single", maxNodeReferences, config),
				blockSize);
	}

	private EditableLayer setupLayer(IndexMaker indexMaker) {
		try (Transaction tx = db.beginTx()) {
			EditableLayer layer = indexMaker.setupLayer(tx);
			tx.commit();
			return layer;
		}
	}

	private void insertManyNodesIndividually(IndexMaker indexMaker, int blockSize) {
		if (enableLucene || indexMaker instanceof RTreeIndexMaker) {
			TestStats stats = indexMaker.initStats(blockSize);
			EditableLayer layer = setupLayer(indexMaker);
			List<String> nodes = indexMaker.nodes();
			TreeMonitor monitor = new RTreeMonitor();
			layer.getIndex().addMonitor(monitor);
			TimedLogger log = indexMaker.initLogger();
			IndexTestConfig config = indexMaker.getConfig();
			long start = System.currentTimeMillis();
			for (int i = 0; i < config.totalCount / blockSize; i++) {
				List<String> slice = nodes.subList(i * blockSize, i * blockSize + blockSize);
				try (Transaction tx = db.beginTx()) {
					for (String node : slice) {
						layer.add(tx, tx.getNodeByElementId(node));
					}
					tx.commit();
				}
				log.log("Splits: " + monitor.getNbrSplit(), (long) (i + 1) * blockSize);
			}
			System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + config.totalCount
					+ " nodes to RTree in bulk");
			stats.setInsertTime(start);
			stats.put("Insert Splits", monitor.getNbrSplit());

			queryRTree(layer, monitor, stats);
			indexMaker.verifyStructure();
		}
	}

	/*
	 * Run this manually to generate images of RTree that can be used for animation.
	 * ffmpeg -f image2 -r 12 -i rtree-single/rtree-%d.png -r 12 -s 1280x960 rtree-single2_12fps.mp4
	 */
	@Disabled
	@Test
	public void shouldInsertManyNodesIndividuallyAndGenerateImagesForAnimation() throws IOException {
		IndexTestConfig config = testConfigs.get("medium");
		int blockSize = 5;
		int maxBlockSize = 1000;
		int maxNodeReferences = 10;
		String splitMode = RTreeIndex.GREENES_SPLIT;
		IndexMaker indexMaker = new RTreeIndexMaker("Coordinates", splitMode, "Single", maxNodeReferences, config);
		TestStats stats = indexMaker.initStats(blockSize);
		EditableLayer layer = setupLayer(indexMaker);
		List<String> nodes = indexMaker.nodes();

		RTreeIndex rtree = (RTreeIndex) layer.getIndex();
		RTreeImageExporter imageExporter;
		try (Transaction tx = db.beginTx()) {
			SimpleFeatureType featureType = Neo4jFeatureBuilder.getTypeFromLayer(tx, layer);
			imageExporter = new RTreeImageExporter(layer.getGeometryFactory(), layer.getGeometryEncoder(),
					layer.getCoordinateReferenceSystem(tx), featureType, rtree);
			imageExporter.initialize(tx, new Coordinate(0.0, 0.0), new Coordinate(1.0, 1.0));
			tx.commit();
		}

		TreeMonitor monitor = new TreePrintingMonitor(imageExporter, "single", splitMode);
		layer.getIndex().addMonitor(monitor);
		TimedLogger log = indexMaker.initLogger();
		long start = System.currentTimeMillis();
		int prevBlock = 0;
		int i = 0;
		int currBlock = 1;
		while (currBlock < nodes.size()) {
			List<String> slice = nodes.subList(prevBlock, currBlock);
			try (Transaction tx = db.beginTx()) {
				for (String node : slice) {
					layer.add(tx, tx.getNodeByElementId(node));
				}
				tx.commit();
			}
			log.log("Splits: " + monitor.getNbrSplit(), currBlock);
			try (Transaction tx = db.beginTx()) {
				imageExporter.saveRTreeLayers(tx, new File("rtree-single-" + splitMode + "/rtree-" + i + ".png"), 7);
				tx.commit();
			}
			i++;
			prevBlock = currBlock;
			currBlock += Math.min(blockSize, maxBlockSize);
			blockSize *= 1.33;
		}
		System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + config.totalCount
				+ " nodes to RTree in bulk");
		stats.setInsertTime(start);
		stats.put("Insert Splits", monitor.getNbrSplit());

		monitor.reset();
		List<Node> found = queryRTree(layer, monitor, stats, false);
		verifyTreeStructure(layer, splitMode, stats);
		try (Transaction tx = db.beginTx()) {
			imageExporter.saveRTreeLayers(tx, new File("rtree-single-" + splitMode + "/rtree.png"), 7, monitor, found,
					config.searchMin, config.searchMax);
			tx.commit();
		}
	}

	private void insertManyNodesInBulk(String splitMode, int blockSize, int maxNodeReferences, IndexTestConfig config) {
		insertManyNodesInBulk(new RTreeIndexMaker("Coordinates", splitMode, "Bulk", maxNodeReferences, config, false),
				blockSize);
	}

	private void insertManyNodesInBulk(String splitMode, int blockSize, int maxNodeReferences, IndexTestConfig config,
			boolean shouldMergeTrees) {
		insertManyNodesInBulk(
				new RTreeIndexMaker("Coordinates", splitMode, "Bulk", maxNodeReferences, config, shouldMergeTrees),
				blockSize);
	}

	private void insertManyNodesInBulk(IndexMaker indexMaker, int blockSize) {
		if (enableLucene || indexMaker instanceof RTreeIndexMaker) {
			TestStats stats = indexMaker.initStats(blockSize);
			EditableLayer layer = setupLayer(indexMaker);
			List<String> nodes = indexMaker.nodes();
			RTreeMonitor monitor = new RTreeMonitor();
			layer.getIndex().addMonitor(monitor);
			TimedLogger log = indexMaker.initLogger();
			long start = System.currentTimeMillis();
			for (int i = 0; i < indexMaker.getConfig().totalCount / blockSize; i++) {
				List<String> slice = nodes.subList(i * blockSize, i * blockSize + blockSize);
				long startIndexing = System.currentTimeMillis();
				try (Transaction tx = db.beginTx()) {
					layer.addAll(tx, idsToNodes(tx, slice));
					tx.commit();
				}
				log.log(startIndexing,
						"Rebuilt: " + monitor.getNbrRebuilt() + ", Splits: " + monitor.getNbrSplit() + ", Cases "
								+ monitor.getCaseCounts(), (long) (i + 1) * blockSize);
			}
			System.out.println(
					"Took " + (System.currentTimeMillis() - start) + "ms to add " + indexMaker.getConfig().totalCount
							+ " nodes to RTree in bulk");
			stats.setInsertTime(start);
			stats.put("Insert Splits", monitor.getNbrSplit());

			monitor.reset();
			queryRTree(layer, monitor, stats);
			indexMaker.verifyStructure();
//        debugIndexTree((RTreeIndex) layer.getIndex());
		}
	}

	/*
	 * Run this manually to generate images of RTree that can be used for animation.
	 * ffmpeg -f image2 -r 12 -i rtree-single/rtree-%d.png -r 12 -s 1280x960 rtree-single2_12fps.mp4
	 */
	@Disabled
	@Test
	public void shouldInsertManyNodesInBulkAndGenerateImagesForAnimation() throws IOException {
		IndexTestConfig config = testConfigs.get("medium");
		int blockSize = 1000;
		int maxNodeReferences = 10;
		String splitMode = RTreeIndex.GREENES_SPLIT;
		IndexMaker indexMaker = new RTreeIndexMaker("Coordinates", splitMode, "Bulk", maxNodeReferences, config);
		EditableLayer layer = setupLayer(indexMaker);
		List<String> nodes = indexMaker.nodes();
		TestStats stats = indexMaker.initStats(blockSize);

		RTreeIndex rtree = (RTreeIndex) layer.getIndex();
		RTreeImageExporter imageExporter;
		try (Transaction tx = db.beginTx()) {
			SimpleFeatureType featureType = Neo4jFeatureBuilder.getTypeFromLayer(tx, layer);
			imageExporter = new RTreeImageExporter(layer.getGeometryFactory(), layer.getGeometryEncoder(),
					layer.getCoordinateReferenceSystem(tx), featureType, rtree);
			imageExporter.initialize(tx, new Coordinate(0.0, 0.0), new Coordinate(1.0, 1.0));
			tx.commit();
		}

		TreeMonitor monitor = new TreePrintingMonitor(imageExporter, "bulk", splitMode);
		layer.getIndex().addMonitor(monitor);
		TimedLogger log = indexMaker.initLogger();
		long start = System.currentTimeMillis();
		for (int i = 0; i < config.totalCount / blockSize; i++) {
			List<String> slice = nodes.subList(i * blockSize, i * blockSize + blockSize);
			long startIndexing = System.currentTimeMillis();
			try (Transaction tx = db.beginTx()) {
				layer.addAll(tx, idsToNodes(tx, slice));
				tx.commit();
			}
			log.log(startIndexing,
					"Rebuilt: " + monitor.getNbrRebuilt() + ", Splits: " + monitor.getNbrSplit() + ", Cases "
							+ monitor.getCaseCounts(), (long) (i + 1) * blockSize);
			try (Transaction tx = db.beginTx()) {
				imageExporter.saveRTreeLayers(tx, new File("rtree-bulk-" + splitMode + "/rtree-" + i + ".png"), 7);
				tx.commit();
			}
		}
		System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + config.totalCount
				+ " nodes to RTree in bulk");
		stats.setInsertTime(start);
		stats.put("Insert Splits", monitor.getNbrSplit());

		monitor.reset();
		List<Node> found = queryRTree(layer, monitor, stats, false);
		indexMaker.verifyStructure();
		try (Transaction tx = db.beginTx()) {
			imageExporter.saveRTreeLayers(tx, new File("rtree-bulk-" + splitMode + "/rtree.png"), 7, monitor, found,
					config.searchMin, config.searchMax);
			tx.commit();
		}
//        debugIndexTree((RTreeIndex) layer.getIndex());
	}

	@Disabled
	@Test
	public void shouldAccessIndexAfterBulkInsertion() {
		EditableLayer layer = getOrCreateSimplePointLayer("Coordinates", "rtree", "lon", "lat");

		final long numNodes = 100000;
		Random rand = new Random();

		System.out.println("Bulk inserting " + numNodes + " nodes");
		long start = System.currentTimeMillis();
		try (Transaction tx = db.beginTx()) {
			List<Node> coordinateNodes = new ArrayList<>();
			for (int i = 0; i < numNodes; i++) {
				Node node = tx.createNode();
				node.addLabel(Label.label("Coordinates"));
				node.setProperty("lat", rand.nextDouble());
				node.setProperty("lon", rand.nextDouble());
				coordinateNodes.add(node);
			}
			layer.addAll(tx, coordinateNodes);
			tx.commit();
		}
		System.out.println("\t" + (System.currentTimeMillis() - start) + "ms");

		System.out.println("Searching with spatial.withinDistance");
		start = System.currentTimeMillis();
		try (Transaction tx = db.beginTx()) { // 'points',{longitude:15.0,latitude:60.0},100
			Result result = tx.execute(
					"CALL spatial.withinDistance('Coordinates',{longitude:0.5, latitude:0.5},1000.0) yield node as malmo");
			int i = 0;
			ResourceIterator<Node> thing = result.columnAs("malmo");
			while (thing.hasNext()) {
				assertNotNull(thing.next());
				i++;
			}
			assertEquals(i, numNodes);
			tx.commit();
		}
		System.out.println("\t" + (System.currentTimeMillis() - start) + "ms");

		System.out.println("Searching with spatial.withinDistance and Cypher count");
		start = System.currentTimeMillis();
		try (Transaction tx = db.beginTx()) {
			String cypher =
					"CALL spatial.withinDistance('Coordinates',{longitude:0.5, latitude:0.5},1000.0) yield node\n" +
							"RETURN COUNT(node) as count";
			Result result = tx.execute(cypher);
//           System.out.println(result.columns().toString());
			Object obj = result.columnAs("count").next();
			assertInstanceOf(Long.class, obj);
			assertEquals((long) ((Long) obj), numNodes);
			tx.commit();
		}
		System.out.println("\t" + (System.currentTimeMillis() - start) + "ms");

		System.out.println("Searching with pure Cypher");
		start = System.currentTimeMillis();
		try (Transaction tx = db.beginTx()) {
			String cypher = "MATCH ()-[:RTREE_ROOT]->(n)\n" +
					"MATCH (n)-[:RTREE_CHILD*]->(m)-[:RTREE_REFERENCE]->(p)\n" +
					"RETURN COUNT(p) as count";
			Result result = tx.execute(cypher);
//           System.out.println(result.columns().toString());
			Object obj = result.columnAs("count").next();
			assertInstanceOf(Long.class, obj);
			assertEquals((long) ((Long) obj), numNodes);
			tx.commit();
		}
		System.out.println("\t" + (System.currentTimeMillis() - start) + "ms");
	}

	@Disabled
	@Test
	public void shouldBuildTreeFromScratch() throws Exception {
		//GraphDatabaseService db = this.databases.database("BultTest2");
		GraphDatabaseService db = this.db;

		GeometryEncoder encoder = new SimplePointEncoder();

		Method decodeEnvelopes = RTreeIndex.class.getDeclaredMethod("decodeEnvelopes", List.class);
		decodeEnvelopes.setAccessible(true);

		Method buildRTreeFromScratch = RTreeIndex.class.getDeclaredMethod("buildRtreeFromScratch", Node.class,
				List.class, double.class);
		buildRTreeFromScratch.setAccessible(true);

		Method expectedHeight = RTreeIndex.class.getDeclaredMethod("expectedHeight", double.class, int.class);
		expectedHeight.setAccessible(true);

		Random random = new Random();
		random.setSeed(42);

		List<Integer> range = IntStream.rangeClosed(1, 300).boxed().collect(Collectors.toList());
		//test over the transiton from two to three deep trees
		range.addAll(IntStream.rangeClosed(4700, 5000).boxed().collect(Collectors.toList()));

		for (int i : range) {
			System.out.println("Building a Tree with " + i + " nodes");
			try (Transaction tx = db.beginTx()) {

				RTreeIndex rtree = new RTreeIndex();
				rtree.init(tx, tx.createNode(), encoder, DEFAULT_MAX_NODE_REFERENCES);
				List<Node> coords = new ArrayList<>(i);
				for (int j = 0; j < i; j++) {
					Node n = tx.createNode(Label.label("Coordinate"));
					n.setProperty(SimplePointEncoder.DEFAULT_X, random.nextDouble() * 90.0);
					n.setProperty(SimplePointEncoder.DEFAULT_Y, random.nextDouble() * 90.0);
					Geometry geometry = encoder.decodeGeometry(n);
					// add BBOX to Node if it's missing
					encoder.encodeGeometry(tx, geometry, n);
					coords.add(n);
					//                   layer.add(n);
				}

				buildRTreeFromScratch.invoke(rtree, rtree.getIndexRoot(tx), decodeEnvelopes.invoke(rtree, coords), 0.7);

				Map<Long, Long> results = RTreeTestUtils.get_height_map(tx, rtree.getIndexRoot(tx));
				assertEquals(1, results.size());
				assertEquals((int) expectedHeight.invoke(rtree, 0.7, coords.size()),
						results.keySet().iterator().next().intValue());
				assertEquals(results.values().iterator().next().intValue(), coords.size());
				tx.commit();
			}
		}
	}

	@Disabled
	@Test
	public void shouldPerformRTreeBulkInsertion() {
		// Use this line if you want to examine the output.
		//GraphDatabaseService db = databases.database("BulkTest");

		SpatialDatabaseService sdbs = spatial();
		int N = 10000;
		int Q = 40;
		Random random = new Random();
		random.setSeed(42);
		//        random.setSeed(42);
		// leads to: Caused by: org.neo4j.kernel.impl.store.InvalidRecordException: Node[142794,used=false,rel=-1,prop=-1,labels=Inline(0x0:[]),light,secondaryUnitId=-1] not in use

		long totalTimeStart = System.currentTimeMillis();
		for (int j = 1; j < Q + 1; j++) {
			System.out.println("BulkLoadingTestRun " + j);
			try (Transaction tx = db.beginTx()) {

				EditableLayer layer = sdbs.getOrCreateSimplePointLayer(tx, "BulkLoader", "rtree", "lon", "lat", null);
				List<Node> coords = new ArrayList<>(N);
				for (int i = 0; i < N; i++) {
					Node n = tx.createNode(Label.label("Coordinate"));
					n.setProperty("lat", random.nextDouble() * 90.0);
					n.setProperty("lon", random.nextDouble() * 90.0);
					coords.add(n);
					//                   layer.add(n);
				}
				long time = System.currentTimeMillis();

				layer.addAll(tx, coords);
				System.out.println(
						"********************** time taken to load " + N + " records: " + (System.currentTimeMillis()
								- time) + "ms");

				RTreeIndex rtree = (RTreeIndex) layer.getIndex();
				assertTrue(RTreeTestUtils.check_balance(tx, rtree.getIndexRoot(tx)));

				tx.commit();
			}
		}
		System.out.println("Total Time for " + (N * Q) + " Nodes in " + Q + " Batches of " + N + " is: ");
		System.out.println(((System.currentTimeMillis() - totalTimeStart) / 1000) + " seconds");

		try (Transaction tx = db.beginTx()) {
			String cypher = "MATCH ()-[:RTREE_ROOT]->(n)\n" +
					"MATCH (n)-[:RTREE_CHILD]->(m)-[:RTREE_CHILD]->(p)-[:RTREE_CHILD]->(s)-[:RTREE_REFERENCE]->(q)\n" +
					"RETURN COUNT(q) as count";
			Result result = tx.execute(cypher);
			System.out.println(result.columns().toString());
			long count = result.<Long>columnAs("count").next();
			assertEquals(N * Q, count);
			tx.commit();
		}

		try (Transaction tx = db.beginTx()) {
			Layer layer = sdbs.getLayer(tx, "BulkLoader");
			RTreeIndex rtree = (RTreeIndex) layer.getIndex();

			Node root = rtree.getIndexRoot(tx);
			List<Node> children = new ArrayList<>(100);
			for (Relationship r : root.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
				children.add(r.getEndNode());
			}
			double root_overlap = RTreeTestUtils.calculate_overlap(root);
			assertTrue(root_overlap < 0.01); //less than one percent
			System.out.println("********* Bulk Overlap Percentage" + root_overlap);

			double average_child_overlap = children.stream().mapToDouble(RTreeTestUtils::calculate_overlap).average()
					.getAsDouble();
			assertTrue(average_child_overlap < 0.02);
			System.out.println("*********** Bulk Average Child Overlap Percentage" + average_child_overlap);
			tx.commit();
		}
	}

	private List<String> populateSquareTestData(int width) {
		GraphDatabaseService db = this.db;
		ArrayList<String> nodes = new ArrayList<>(width * width);
		for (int i = 0; i < width; i++) {
			try (Transaction tx = db.beginTx()) {
				for (int j = 0; j < width; j++) {
					Node node = tx.createNode();
					node.addLabel(Label.label("Coordinates"));
					node.setProperty("lat", ((double) i / (double) width));
					node.setProperty("lon", ((double) j / (double) width));
					nodes.add(node.getElementId());
				}
				tx.commit();
			}
		}
		java.util.Collections.shuffle(nodes, new Random(8));
		return nodes;
	}

	private List<String> setup(String name, String index, int width) {
		long start = System.currentTimeMillis();
		List<String> nodes = populateSquareTestData(width);
		System.out.println(
				"Took " + (System.currentTimeMillis() - start) + "ms to create " + (width * width) + " nodes");
		getOrCreateSimplePointLayer(name, index, "lon", "lat");
		return nodes;
	}

	private static class NodeWithEnvelope {

		Envelope envelope;
		Node node;

		NodeWithEnvelope(Node node, Envelope envelope) {
			this.node = node;
			this.envelope = envelope;
		}
	}

	private static void checkIndexOverlaps(Transaction tx, Layer layer, TestStats stats) {
		RTreeIndex index = (RTreeIndex) layer.getIndex();
		Node root = index.getIndexRoot(tx);
		ArrayList<ArrayList<NodeWithEnvelope>> nodes = new ArrayList<>();
		nodes.add(new ArrayList<>());
		nodes.get(0).add(new NodeWithEnvelope(root, RTreeIndex.getIndexNodeEnvelope(root)));
		do {
			ArrayList<NodeWithEnvelope> children = new ArrayList<>();
			for (NodeWithEnvelope parent : nodes.get(nodes.size() - 1)) {
				for (Relationship rel : parent.node.getRelationships(Direction.OUTGOING,
						RTreeRelationshipTypes.RTREE_CHILD)) {
					Node child = rel.getEndNode();
					children.add(new NodeWithEnvelope(child, RTreeIndex.getIndexNodeEnvelope(child)));
				}
			}
			if (children.isEmpty()) {
				break;
			}
			nodes.add(children);
		} while (true);
		System.out.println("Comparison of index node areas to root area for " + nodes.size() + " index levels:");
		for (int level = 0; level < nodes.size(); level++) {
			double[] overlap = calculateOverlap(nodes.get(0).get(0), nodes.get(level));
			System.out.println("\t" + level + "\t" + nodes.get(level).size() + "\t" + overlap[0] + "\t" + overlap[1]);
			stats.put("Leaf Overlap Delta", overlap[0]);
			stats.put("Leaf Overlap Ratio", overlap[1]);
		}
	}

	private static double[] calculateOverlap(NodeWithEnvelope root, List<NodeWithEnvelope> nodes) {
		double rootArea = root.envelope.getArea();
		double nodesArea = 0.0;
		for (NodeWithEnvelope entry : nodes) {
			nodesArea += entry.envelope.getArea();
		}
		return new double[]{nodesArea - rootArea, nodesArea / rootArea};
	}

	private List<Node> queryRTree(Layer layer, TreeMonitor monitor, TestStats stats, boolean assertTouches) {
		List<Node> nodes = queryIndex(layer, stats);
		if (layer.getIndex() instanceof RTreeIndex) {
			getRTreeIndexStats((RTreeIndex) layer.getIndex(), monitor, stats, assertTouches, nodes.size());
		}
		return nodes;
	}

	private List<Node> queryRTree(Layer layer, TreeMonitor monitor, TestStats stats) {
		List<Node> nodes = queryIndex(layer, stats);
		if (layer.getIndex() instanceof RTreeIndex) {
			getRTreeIndexStats((RTreeIndex) layer.getIndex(), monitor, stats, true, nodes.size());
		} else if (layer.getIndex() instanceof ExplicitIndexBackedPointIndex) {
			getExplicitIndexBackedIndexStats((ExplicitIndexBackedPointIndex<?>) layer.getIndex(), stats);
		}
		return nodes;
	}

	private List<Node> queryIndex(Layer layer, TestStats stats) {
		List<Node> nodes;
		IndexTestConfig config = stats.config;
		long start = System.currentTimeMillis();
		try (Transaction tx = db.beginTx()) {
			org.locationtech.jts.geom.Envelope envelope = new org.locationtech.jts.geom.Envelope(config.searchMin,
					config.searchMax);
			nodes = GeoPipeline.startWithinSearch(tx, layer, layer.getGeometryFactory().toGeometry(envelope)).stream()
					.map(GeoPipeFlow::getGeomNode).collect(Collectors.toList());
			tx.commit();
		}
		long countGeometries = nodes.size();
		long queryTime = System.currentTimeMillis() - start;
		allStats.add(stats);
		stats.put("Query Time (ms)", queryTime);
		System.out.println("Took " + queryTime + "ms to find " + countGeometries + " nodes in 4x4 block");
		try (Transaction tx = db.beginTx()) {
			int geometrySize = layer.getIndex().count(tx);
			stats.put("Indexed", geometrySize);
			System.out.println("Index contains " + geometrySize + " geometries");
		}
		assertEquals(config.expectedGeometries, countGeometries,
				"Expected " + config.expectedGeometries + " nodes to be returned");
		return nodes;
	}

	private void getRTreeIndexStats(RTreeIndex index, TreeMonitor monitor, TestStats stats, boolean assertTouches,
			long countGeometries) {
		IndexTestConfig config = stats.config;
		int indexTouched = monitor.getCaseCounts().get("Index Does NOT Match");
		int indexMatched = monitor.getCaseCounts().get("Index Matches");
		int touched = monitor.getCaseCounts().get("Geometry Does NOT Match");
		int matched = monitor.getCaseCounts().get("Geometry Matches");
		int indexSize = 0;
		try (Transaction tx = db.beginTx()) {
		    indexSize += StreamSupport.stream(index.getAllIndexInternalNodes(tx).spliterator(), false).count();
		    tx.commit();
		}
		stats.put("Index Size", indexSize);
		stats.put("Found", matched);
		stats.put("Touched", touched);
		stats.put("Index Found", indexMatched);
		stats.put("Index Touched", indexTouched);
		System.out.println("Searched index of " + indexSize + " nodes in tree of height " + monitor.getHeight());
		System.out.println(
				"Matched " + matched + "/" + touched + " touched nodes (" + (100.0 * matched / touched) + "%)");
		System.out.println(
				"Having matched " + indexMatched + "/" + indexTouched + " touched index nodes (" + (100.0 * indexMatched
						/ indexTouched) + "%)");
		System.out.println(
				"Which means we touched " + indexTouched + "/" + indexSize + " index nodes (" + (100.0 * indexTouched
						/ indexSize) + "%)");
		// Note that due to some crazy GIS spec points on polygon edges are considered to be contained,
		// unless the polygon is a rectangle, in which case they are not contained, leading to
		// different numbers for expectedGeometries and expectedCount.
		// See https://github.com/locationtech/jts/blob/master/modules/core/src/main/java/org/locationtech/jts/operation/predicate/RectangleContains.java#L70
		assertEquals(config.expectedCount, matched,
				"Expected " + config.expectedCount + " nodes to be matched");
		int maxNodeReferences = stats.maxNodeReferences;
		int maxExpectedGeometriesTouched = matched * maxNodeReferences;
		if (countGeometries > 1 && assertTouches) {
			assertThat("Should not touch more geometries than " + maxNodeReferences + "*matched", touched,
					lessThanOrEqualTo(maxExpectedGeometriesTouched));
			int maxExpectedIndexTouched = indexMatched * maxNodeReferences;
			assertThat("Should not touch more index nodes than " + maxNodeReferences + "*matched", indexTouched,
					lessThanOrEqualTo(maxExpectedIndexTouched));
		}
	}

	private static void getExplicitIndexBackedIndexStats(ExplicitIndexBackedPointIndex<?> index, TestStats stats) {
		IndexTestConfig config = stats.config;
		ExplicitIndexBackedMonitor monitor = index.getMonitor();
		long touched = monitor.getHits() + monitor.getMisses();
		long matched = monitor.getHits();
		stats.put("Found", matched);
		stats.put("Touched", touched);
		System.out.println(
				"Matched " + matched + "/" + touched + " touched nodes (" + (100.0 * matched / touched) + "%)");
		// Note that due to some crazy GIS spec points on polygon edges are considered to be contained,
		// unless the polygon is a rectangle, in which case they are not contained, leading to
		// different numbers for expectedGeometries and expectedCount.
		// See https://github.com/locationtech/jts/blob/master/modules/core/src/main/java/org/locationtech/jts/operation/predicate/RectangleContains.java#L70
		assertEquals(config.expectedCount, matched,
				"Expected " + config.expectedCount + " nodes to be matched");
	}

	private class TimedLogger {

		long count;
		long gap;
		long start;
		long previous;

		private TimedLogger(String title, long count) {
			this(title, count, 1000);
		}

		private TimedLogger(String title, long count, long gap) {
			this.count = count;
			this.gap = gap;
			this.start = System.currentTimeMillis();
			this.previous = this.start;
			System.out.println(title);
		}

		private void log(long previous, String line, long number) {
			this.previous = previous;
			log(line, number);
		}

		private void log(String line, long number) {
			long current = System.currentTimeMillis();
			if (current - previous > gap) {
				double percentage = 100.0 * number / count;
				double seconds = (current - start) / 1000.0;
				int rate = (int) (number / seconds);
				System.out.println(
						"\t" + ((int) percentage) + "%\t" + number + "\t" + seconds + "s\t" + rate + "n/s:\t" + line);
				previous = current;
			}
		}
	}

	private static void verifyGeohashIndex(Layer layer) {
		LayerIndexReader index = layer.getIndex();
		assertInstanceOf(LayerGeohashPointIndex.class, index, "Index should be a geohash index");
	}

	private static void verifyHilbertIndex(Layer layer) {
		LayerIndexReader index = layer.getIndex();
		assertInstanceOf(LayerHilbertPointIndex.class, index, "Index should be a hilbert index");
	}

	private static void verifyZOrderIndex(Layer layer) {
		LayerIndexReader index = layer.getIndex();
		assertInstanceOf(LayerZOrderPointIndex.class, index, "Index should be a Z-Order index");
	}

	private void verifyTreeStructure(Layer layer, String splitMode, TestStats stats) {
		String layerNodeId;
		try (Transaction tx = db.beginTx()) {
			Node layerNode = layer.getLayerNode(tx);
			layerNodeId = layerNode.getElementId();
			tx.commit();
		}
		String queryDepthAndGeometries =
				"MATCH (layer)-[:RTREE_ROOT]->(root) WHERE elementId(layer)=$layerNodeId WITH root " +
						"MATCH p = (root)-[:RTREE_CHILD*]->(child)-[:RTREE_REFERENCE]->(geometry) " +
						"RETURN length(p) as depth, count(*) as geometries";

		String queryNumChildren = "MATCH (layer)-[:RTREE_ROOT]->(root) WHERE elementId(layer)=$layerNodeId WITH root " +
				"MATCH p = (root)-[:RTREE_CHILD*]->(child) " +
				"WHERE exists((child)-[:RTREE_REFERENCE]->()) " +
				"RETURN length(p) as depth, count (*) as leaves";
		String queryChildrenPerParent =
				"MATCH (layer)-[:RTREE_ROOT]->(root) WHERE elementId(layer)=$layerNodeId WITH root " +
						"MATCH p = (root)-[:RTREE_CHILD*0..]->(parent)-[:RTREE_CHILD]->(child) " +
						"WITH parent, count (*) as children RETURN avg(children) as childrenPerParent,min(children) as "
						+
						"MinChildrenPerParent,max(children) as MaxChildrenPerParent";
		String queryChildrenPerParent2 =
				"MATCH (layer)-[:RTREE_ROOT]->(root) WHERE elementId(layer)=$layerNodeId WITH root " +
						"MATCH p = (root)-[:RTREE_CHILD*0..]->(parent)-[:RTREE_CHILD|RTREE_REFERENCE]->(child) " +
						"RETURN parent, length(p) as depth, count (*) as children";
		Map<String, Object> params = Collections.singletonMap("layerNodeId", layerNodeId);
		int balanced = 0;
		long geometries = 0;
		try (Transaction tx = db.beginTx()) {
			Result resultDepth = tx.execute(queryDepthAndGeometries, params);
			while (resultDepth.hasNext()) {
				balanced++;
				Map<String, Object> depthMap = resultDepth.next();
				geometries = (long) depthMap.get("geometries");
				System.out.println("Tree depth to all geometries: " + depthMap);
			}
		}
		assertEquals(1, balanced, "All geometries should be at the same depth");
		Map<String, Object> leafMap;
		try (Transaction tx = db.beginTx()) {
			Result resultNumChildren = tx.execute(queryNumChildren, params);
			leafMap = resultNumChildren.next();
			System.out.println("Tree depth to all leaves that have geomtries: " + leafMap);
			Result resultChildrenPerParent = tx.execute(queryChildrenPerParent, params);
			System.out.println("Children per parent: " + resultChildrenPerParent.next());
		}

		int totalNodes = 0;
		int underfilledNodes = 0;
		int blockSize = Math.max(10, stats.maxNodeReferences) / 10;
		Integer[] histogram = new Integer[11];
		try (Transaction tx = db.beginTx()) {
			Result resultChildrenPerParent2 = tx.execute(queryChildrenPerParent2, params);
			Arrays.fill(histogram, 0);
			while (resultChildrenPerParent2.hasNext()) {
				Map<String, Object> result = resultChildrenPerParent2.next();
				long children = (long) result.get("children");
				if (children < blockSize * 3) {
					underfilledNodes++;
				}
				totalNodes++;
				histogram[(int) children / blockSize]++;
			}
		}
		allStats.add(stats);
		stats.put("Underfilled%", 100.0 * underfilledNodes / totalNodes);

		System.out.println(
				"Histogram of child count for " + totalNodes + " index nodes, with " + underfilledNodes + " (" + (
						100 * underfilledNodes / totalNodes) + "%) underfilled (less than 30% or " + (blockSize * 3)
						+ ")");
		for (int i = 0; i < histogram.length; i++) {
			System.out.println("[" + (i * blockSize) + ".." + ((i + 1) * blockSize) + "): " + histogram[i]);
		}
		if (!splitMode.equals(RTreeIndex.QUADRATIC_SPLIT)) {
			assertThat("Expected to have less than 30% of nodes underfilled", underfilledNodes,
					lessThan(3 * totalNodes / 10));
		}
		long leafCountFactor = splitMode.equals(RTreeIndex.QUADRATIC_SPLIT) ? 20 : 2;
		long maxLeafCount = leafCountFactor * geometries / stats.maxNodeReferences;
		assertThat("In " + splitMode + " we expected leaves to be no more than " + leafCountFactor
				+ "x(geometries/maxNodeReferences)", (long) leafMap.get("leaves"), lessThanOrEqualTo(maxLeafCount));
		try (Transaction tx = db.beginTx()) {
			checkIndexOverlaps(tx, layer, stats);
			tx.commit();
		}
	}

	private void restart() throws IOException {
		if (databases != null) {
			databases.shutdown();
		}
		if (storeDir.exists()) {
			System.out.println("Deleting previous database: " + storeDir);
			FileUtils.deleteDirectory(storeDir);
		}
		FileUtils.forceMkdir(storeDir);
		databases = new DatabaseManagementServiceBuilder(storeDir.toPath()).build();
		db = databases.database(DEFAULT_DATABASE_NAME);
	}

	private void doCleanShutdown() {
		try {
			System.out.println("Shutting down database");
			if (databases != null) {
				databases.shutdown();
			}
			//TODO: Uncomment this once all tests are stable
//            FileUtils.deleteDirectory(storeDir);
		} finally {
			databases = null;
			db = null;
		}
	}

	private static class TestStats {

		private final IndexTestConfig config;
		private final String insertMode;
		private final int dataSize;
		private final int blockSize;
		private final String splitMode;
		private final int maxNodeReferences;
		static LinkedHashSet<String> knownKeys = new LinkedHashSet<>();
		private final HashMap<String, Object> data = new HashMap<>();

		private TestStats(IndexTestConfig config, String insertMode, String splitMode, int blockSize,
				int maxNodeReferences) {
			this.config = config;
			this.insertMode = insertMode;
			this.dataSize = config.width * config.width;
			this.blockSize = blockSize;
			this.splitMode = splitMode;
			this.maxNodeReferences = maxNodeReferences;
		}

		private void setInsertTime(long start) {
			long current = System.currentTimeMillis();
			double seconds = (current - start) / 1000.0;
			int rate = (int) (dataSize / seconds);
			put("Insert Time (s)", seconds);
			put("Insert Rate (n/s)", rate);
		}

		public void put(String key, Object value) {
			knownKeys.add(key);
			data.put(key, value);
		}

		private static String[] headerArray() {
			return new String[]{"Size Name", "Insert Mode", "Split Mode", "Data Width", "Data Size", "Block Size",
					"Max Node References"};
		}

		private Object[] fieldArray() {
			return new Object[]{config.name, insertMode, splitMode, config.width, dataSize, blockSize,
					maxNodeReferences};
		}

		private static List<String> headerList() {
			ArrayList<String> fieldList = new ArrayList<>();
			fieldList.addAll(Arrays.asList(headerArray()));
			fieldList.addAll(knownKeys);
			return fieldList;
		}

		private List<Object> asList() {
			ArrayList<Object> fieldList = new ArrayList<>();
			fieldList.addAll(Arrays.asList(fieldArray()));
			fieldList.addAll(
					knownKeys.stream().map(data::get).map(v -> (v == null) ? "" : v).collect(Collectors.toList()));
			return fieldList;
		}

		private static String headerString() {
			return String.join("\t", headerList());
		}

		@Override
		public String toString() {
			return asList().stream().map(Object::toString).collect(Collectors.joining("\t"));
		}
	}

	private static final LinkedHashSet<TestStats> allStats = new LinkedHashSet<>();

	@AfterAll
	public static void afterClass() {
		System.out.println("\n\nComposite stats for " + allStats.size() + " tests run");
		System.out.println(TestStats.headerString());
		for (TestStats stats : allStats) {
			System.out.println(stats);
		}
	}
}
