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
package org.neo4j.spatial.osm.server.plugin;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.Utilities;
import org.neo4j.gis.spatial.filter.SearchIntersect;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.index.IndexManagerImpl;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spatial.api.Envelope;
import org.neo4j.spatial.api.SpatialRecord;
import org.neo4j.spatial.api.SpatialRecords;
import org.neo4j.spatial.api.index.SpatialIndexReader;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.osm.server.plugin.procedures.OsmSpatialProcedures;
import org.neo4j.spatial.osm.server.plugin.utils.FakeIndex;
import org.neo4j.spatial.osm.server.plugin.utils.SpatialIndexPerformanceProxy;
import org.neo4j.spatial.testutils.Neo4jTestCase;

/**
 * <p>
 * Test cases for initial version of Neo4j-Spatial. This was originally
 * converted directly from Davide Savazzi's console applications:
 * ShapefileImporter, Test, Test2 and Test3. It was then extended substantially
 * by Craig to include tests for successful search of a number of specific
 * geometries in a number of specific shapefiles. After this tests for OSM
 * dataset import and search were added. The latest OSM work, specific to
 * DynamicLayer's is tested in the new test class TestDynamicLayers.
 * </p>
 * <p>
 * This class can also be configured on the command-line, java runtime
 * configuration or maven configuration. Set the java system property
 * spatial.test.mode to one of the following options:
 * <dl>
 * <dt>long</dt>
 * <dd>All known tests are run, regardless of the size of data or length of time
 * taken to test. This is good for infrequent tests with laeger data. It is
 * possible the maven dependencies will not include the necessary data to run
 * this test, so if you get failures, check that it is not simply missing data.
 * <dt>short</dt>
 * <dd>A very short set of tests are run, just as a quite check. This might be
 * one small SHP and one small OSM file, or even less</dd>
 * <dt>dev</dt>
 * <dd>Tests here change all the time as the developers are coding new features.
 * Craig in particular uses this place for a kind of TDD approach, writing tests
 * as he codes to drive the development. No guarranteee these tests will pass,
 * especially if something is only half coded</dd>
 * <dt>any other value, or leave unset</dt>
 * <dd>The default set of tests to run. This should be something that will work
 * well in the hudson build, as well as for developers downloading and trying
 * out neo4j-spatial</dd>
 * </dl>
 * </p>
 */
public class TestSpatial extends Neo4jTestCase {

	private static final Logger LOGGER = Logger.getLogger(TestSpatial.class.getName());

	private final ArrayList<String> layers = new ArrayList<>();
	private final HashMap<String, Envelope> layerTestEnvelope = new HashMap<>();
	private final HashMap<String, ArrayList<TestGeometry>> layerTestGeometries = new HashMap<>();
	private final HashMap<Integer, Integer> geomStats = new HashMap<>();

	@Override
	protected List<Class<?>> loadProceduresAndFunctions() {
		return List.of(SpatialFunctions.class, SpatialProcedures.class, OsmSpatialProcedures.class);
	}

	@BeforeEach
	public void setUp() {
		// TODO: Rather load this from a configuration file, properties file or JRuby test code

		Envelope bbox = new Envelope(12.9, 12.99, 56.05, 56.07); // covers half of Billesholm

		addTestLayer("billesholm.osm", bbox);
		addTestGeometry(70423036, "Ljungsgårdsvägen", "outside top left",
				"(12.9599540,56.0570692), (12.9624780,56.0716282)");
		addTestGeometry(67835020, "Villagatan", "in the middle", "(12.9776065,56.0561477), (12.9814421,56.0572131)");
		addTestGeometry(60966388, "Storgatan", "crossing left edge",
				"(12.9682980,56.0524546), (12.9710302,56.0538436)");

		bbox = new Envelope(12.5, 14.1, 55.0, 56.3); // cover central Skåne
		addTestLayer("sweden.osm", bbox);
		addTestLayer("sweden.osm.administrative", bbox);
	}

	private void addTestLayer(String layer, Envelope bbox) {
		layers.add(layer);
		layerTestEnvelope.put(layer, bbox);
		layerTestGeometries.put(layer, new ArrayList<>());
	}

	private void addTestGeometry(Integer id, String name, String comments, String bounds) {
		String layer = layers.getLast();
		ArrayList<TestGeometry> geoms = layerTestGeometries.get(layer);
		geoms.add(new TestGeometry(id, name, comments, bounds));
	}

	@Test
	public void testOsmBillesholm() throws Exception {
		testLayer("billesholm.osm");
	}

	private void testLayer(String layerName) throws Exception {
		testImport(layerName);
		testSpatialIndex(layerName);
	}

	private long countLayerIndex(String layerName) {
		long count = 0;
		try (Transaction tx = graphDb().beginTx()) {
			SpatialDatabaseService spatial = new SpatialDatabaseService(
					new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
			Layer layer = spatial.getLayer(tx, layerName, true);
			if (layer != null && layer.getIndex() != null) {
				count = layer.getIndex().count(tx);
			}
			tx.commit();
		}
		return count;
	}

	private void testImport(String layerName) throws Exception {
		long start = System.currentTimeMillis();
		LOGGER.fine("\n===========\n=========== Import Test: " + layerName + "\n===========");
		if (countLayerIndex(layerName) < 1) {
			loadTestOsmData(layerName);
		} else {
			fail("Layer already present: " + layerName);
		}

		LOGGER.fine("Total time for load: " + 1.0 * (System.currentTimeMillis() - start) / 1000.0 + "s");
	}

	private void loadTestOsmData(String layerName) throws Exception {
		String OSM_DIR = "target/osm";
		String osmPath = OSM_DIR + File.separator + layerName;
		LOGGER.fine("\n=== Loading layer " + layerName + " from " + osmPath + " ===");
		OSMImporter importer = new OSMImporter(layerName);
		importer.setCharset(StandardCharsets.UTF_8);
		importer.importFile(graphDb(), osmPath);
		importer.reIndex(graphDb(), 1000);
	}

	private void testSpatialIndex(String layerName) {
		LOGGER.fine("\n=== Spatial Index Test: " + layerName + " ===");
		long start = System.currentTimeMillis();

		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		try (Transaction tx = graphDb().beginTx()) {
			Layer layer = spatial.getLayer(tx, layerName, true);
			if (layer == null || layer.getIndex() == null || layer.getIndex().count(tx) < 1) {
				fail("Layer not loaded: " + layerName);
			}
			OSMDataset.fromLayer(tx, (OSMLayer) layer); // force lookup

			SpatialIndexReader fakeIndex = new SpatialIndexPerformanceProxy(
					new FakeIndex(layer, spatial.indexManager, SpatialDatabaseRecord::new));
			SpatialIndexReader rtreeIndex = new SpatialIndexPerformanceProxy(layer.getIndex());

			LOGGER.fine("RTreeIndex bounds: " + rtreeIndex.getBoundingBox(tx));
			LOGGER.fine("FakeIndex bounds: " + fakeIndex.getBoundingBox(tx));
			assertEnvelopeEquals(fakeIndex.getBoundingBox(tx), rtreeIndex.getBoundingBox(tx));

			LOGGER.fine("RTreeIndex count: " + rtreeIndex.count(tx));
			Assertions.assertEquals(fakeIndex.count(tx), rtreeIndex.count(tx));

			Envelope bbox = layerTestEnvelope.get(layerName);

			LOGGER.fine(
					"Displaying test geometries for layer '" + layerName + "' including expected search results");
			for (TestGeometry testData : layerTestGeometries.get(layerName)) {
				LOGGER.fine("\tGeometry: " + testData + " " + (testData.inOrIntersects(bbox) ? "is" : "is NOT")
						+ " inside search region");
			}

			for (SpatialIndexReader index : new SpatialIndexReader[]{rtreeIndex, fakeIndex}) {
				ArrayList<TestGeometry> foundData = new ArrayList<>();

				SearchIntersect searchQuery = new SearchIntersect(layer,
						layer.getGeometryFactory().toGeometry(Utilities.fromNeo4jToJts(bbox)));
				SpatialRecords results = index.search(tx, searchQuery);

				int count = 0;
				int ri = 0;
				for (SpatialRecord r : results) {
					count++;
					if (ri++ < 10) {
						StringBuilder props = new StringBuilder();
						for (String prop : r.getPropertyNames(tx)) {
							if (!props.isEmpty()) {
								props.append(", ");
							}
							props.append(prop).append(": ").append(r.getProperty(tx, prop));
						}

						LOGGER.fine(
								"\tRTreeIndex result[" + ri + "]: " + r.getId() + ":"
										+ SpatialDatabaseService.convertJtsClassToGeometryType(
										r.getGeometry().getClass()) + " - " + r
										+ ": PROPS[" + props + "]");
					} else if (ri == 10) {
						LOGGER.fine("\t.. and " + (count - ri) + " more ..");
					}

					addGeomStats(r.getGeomNode());

					String name = (String) r.getProperty(tx, "NAME");
					if (name == null) {
						name = (String) r.getProperty(tx, "name");
					}

					Integer id = (Integer) r.getProperty(tx, "ID");
					if ((name != null && !name.isEmpty()) || id != null) {
						for (TestGeometry testData : layerTestGeometries.get(layerName)) {
							if ((name != null && !name.isEmpty() && testData.name.equals(name))
									|| (testData.id.equals(id))) {
								LOGGER.fine(
										"\tFound match in test data: test[" + testData + "] == result[" + r + "]");
								foundData.add(testData);
							}
						}
					} else {
						LOGGER.warning(
								"\tNo name or id in RTreeIndex result: " + r.getId() + ":"
										+ SpatialDatabaseService.convertJtsClassToGeometryType(
										r.getGeometry().getClass()) + " - "
										+ r);
					}
				}

				dumpGeomStats();

				LOGGER.fine("Found " + foundData.size() + " test datasets in region[" + bbox + "]");
				for (TestGeometry testData : foundData) {
					LOGGER.fine("\t" + testData + ": " + testData.bounds);
				}

				LOGGER.fine(
						"Verifying results for " + layerTestGeometries.size() + " test datasets in region[" + bbox
								+ "]");
				for (TestGeometry testData : layerTestGeometries.get(layerName)) {
					LOGGER.fine("\ttesting " + testData + ": " + testData.bounds);
					if (testData.inOrIntersects(bbox) && !foundData.contains(testData)) {
						String error =
								"Incorrect test result: test[" + testData + "] not found by search inside region["
										+ bbox + "]";
						for (TestGeometry data : foundData) {
							LOGGER.fine(data.toString());
						}
						LOGGER.fine(error);
						fail(error);
					}
				}
			}

			LOGGER.fine(
					"Total time for index test: " + 1.0 * (System.currentTimeMillis() - start) / 1000.0 + "s");
			tx.commit();
		}
	}

	private static void assertEnvelopeEquals(Envelope a, Envelope b) {
		Assertions.assertNotNull(a);
		Assertions.assertNotNull(b);
		Assertions.assertEquals(a.getDimension(), b.getDimension());

		for (int i = 0; i < a.getDimension(); i++) {
			Assertions.assertEquals(a.getMin(i), b.getMin(i), 0);
			Assertions.assertEquals(a.getMax(i), b.getMax(i), 0);
		}
	}

	private void addGeomStats(Node geomNode) {
		addGeomStats((Integer) geomNode.getProperty(Constants.PROP_TYPE, null));
	}

	private void addGeomStats(Integer geom) {
		geomStats.compute(geom, (k, count) -> count == null ? 1 : count + 1);
	}

	private void dumpGeomStats() {
		LOGGER.fine("Geometry statistics for " + geomStats.size() + " geometry types:");
		for (Integer key : geomStats.keySet()) {
			Integer count = geomStats.get(key);
			LOGGER.fine("\t" + SpatialDatabaseService.convertGeometryTypeToName(key) + ": " + count);
		}
		geomStats.clear();
	}

	/**
	 * This class represents mock objects for representing geometries in simple form in memory for
	 * testing against real geometries. We have a few hard-coded test geometries we expect to find
	 * stored in predictable ways in the test database. Currently we only test for bounding box so
	 * this class only contains that information.
	 */
	private static class TestGeometry {

		private final Integer id;
		private final String name;
		private final String comments;
		protected Envelope bounds;

		public TestGeometry(Integer id, String name, String comments, String bounds) {
			this.id = id;
			this.name = name == null ? "" : name;
			this.comments = comments;

			float[] bf = new float[4];
			int bi = 0;
			for (String bound : bounds.replaceAll("[()\\s]+", "").split(",")) {
				bf[bi++] = Float.parseFloat(bound);
			}
			this.bounds = new Envelope(bf[0], bf[2], bf[1], bf[3]);
		}

		@Override
		public String toString() {
			return (!name.isEmpty() ? name : "ID[" + id + "]") + (comments == null || comments.isEmpty() ? ""
					: " (" + comments + ")");
		}

		public boolean inOrIntersects(Envelope env) {
			return env.intersects(bounds);
		}
	}
}
