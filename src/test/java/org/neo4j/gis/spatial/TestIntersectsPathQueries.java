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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.pipes.processing.OrthodromicDistance;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class TestIntersectsPathQueries {

	/**
	 * This test case is designed to capture the conditions described in the bug
	 * report at https://github.com/neo4j/spatial/issues/112. The report claims
	 * that intersects searches on large sets of points are very low. This test
	 * We should write a test case that demonstrates the difference in performance
	 * between three approaches.
	 * <ul>
	 * <li>Iterating over set of points</li>
	 * <li>build a MultiPoint and search with that</li>
	 * <li>build a LineString and search with that</li>
	 * </ul>
	 * We would expect the LineString and MultiPoint to perform very much better
	 * than the set iteration, especially if the sample Geometry sets (roads) is
	 * very large, and the set of test points quite large.
	 */
	@Test
	public void testPointSetGeoptimaIntersection() throws InterruptedException {
		String osmPath = "albert/osm/massachusetts.highway.osm";
		String shpPath = "albert/shp/massachusetts_highway.shp";
		String dbRoot = "target/geoptima";
		String dbName = "massachusetts.highway.db";
		String layerName = "massachusetts";
		String tracePath = "albert/locations_input.txt";
		File dbDir = new File(dbRoot, "data/databases/" + dbName);
		if (dbDir.exists() && dbDir.isDirectory()) {
			System.out.println("Found database[" + dbName + "]  - running test directly on existing database");
			runTestPointSetGeoptimaIntersection(tracePath, dbRoot, dbName, layerName, false);
		} else if ((new File(shpPath)).exists()) {
			System.out.println(
					"No database[" + dbName + "] but found shp[" + shpPath + "] - importing before running test");
			importShapefileDatabase(shpPath, dbRoot, dbName, layerName);
			runTestPointSetGeoptimaIntersection(tracePath, dbRoot, dbName, layerName, false);
		} else if ((new File(osmPath)).exists()) {
			System.out.println(
					"No database[" + dbName + "] but found osm[" + osmPath + "] - importing before running test");
			importOSMDatabase(osmPath, dbRoot, dbName, layerName);
			runTestPointSetGeoptimaIntersection(tracePath, dbRoot, dbName, layerName, false);
		} else {
			System.out.println("No database[" + dbName + "] or osm[" + osmPath + "] - cannot run test");
		}
	}

	private static void importShapefileDatabase(String shpPath, String dbRoot, String dbName, String layerName) {
		withDatabase(dbRoot, dbName, Neo4jTestCase.LARGE_CONFIG, graphDb -> {
			ShapefileImporter importer = new ShapefileImporter(graphDb, new ConsoleListener(), 10000, true);
			importer.setFilterEnvelope(makeFilterEnvelope());
			try {
				importer.importFile(shpPath, layerName, StandardCharsets.UTF_8);
				return null;
			} catch (IOException e) {
				return e;
			}
		});
	}

	private static Envelope makeFilterEnvelope() {
		Envelope filterEnvelope = new Envelope();
		filterEnvelope.expandToInclude(new Coordinate(-71.00, 42.10));
		filterEnvelope.expandToInclude(new Coordinate(-71.70, 42.50));
		return filterEnvelope;
	}

	private static void importOSMDatabase(String osmPath, String dbRoot, String dbName, String layerName)
			throws InterruptedException {
		// TODO: Port to batch inserter in `github.com/neo4j-contrib/osm` project
		OSMImporter importer = new OSMImporter(layerName, new ConsoleListener(), makeFilterEnvelope());
		withDatabase(dbRoot, dbName, Neo4jTestCase.LARGE_CONFIG, graphDb -> {
			try {
				importer.importFile(graphDb, osmPath, 10000);
				return null;
			} catch (Exception e) {
				return e;
			}
		});
		// Weird hack to force GC on large loads
		long start = System.currentTimeMillis();
		if (System.currentTimeMillis() - start > 300000) {
			for (int i = 0; i < 3; i++) {
				System.gc();
				Thread.sleep(1000);
			}
		}
		withDatabase(dbRoot, dbName, Neo4jTestCase.LARGE_CONFIG, graphDb -> {
			importer.reIndex(graphDb, 10000, false);
			try {
				TestOSMImport.checkOSMLayer(graphDb, layerName);
				return null;
			} catch (Exception e) {
				return e;
			}
		});
	}

	private static class Performance {

		long start;
		long duration;
		String name;
		Collection<Node> results;
		int count;

		private Performance(String name) {
			this.name = name;
			start();
		}

		public void start() {
			this.start = System.currentTimeMillis();
		}

		public void stop(int count) {
			this.count = count;
		}

		public void stop(Collection<Node> results) {
			this.duration = System.currentTimeMillis() - start;
			this.results = results;
			this.count = results.size();
		}

		private double overlaps(Collection<Node> original) {
			if (results == null) {
				return 0.0;
			}
			if (original.size() < results.size()) {
				return fractionOf(results, original);
			}
			return fractionOf(original, results);
		}

		private static double fractionOf(Collection<Node> subset, Collection<Node> set) {
			HashSet<Node> all = new HashSet<>(set);
			int count = 0;
			for (Node node : subset) {
				if (all.contains(node)) {
					count++;
				}
			}
			return ((double) count) / ((double) set.size());
		}

		public double duration() {
			return duration / 1000.0;
		}

		@Override
		public String toString() {
			return name + "\t" + duration;
		}
	}

	@SuppressWarnings("SameParameterValue")
	private static void runTestPointSetGeoptimaIntersection(String tracePath, String dbRoot, String dbName,
			String layerName, boolean testMultiPoint) {
		withDatabase(dbRoot, dbName, Neo4jTestCase.NORMAL_CONFIG, graphDb -> {
			SpatialDatabaseService spatial = new SpatialDatabaseService(
					new IndexManager((GraphDatabaseAPI) graphDb, SecurityContext.AUTH_DISABLED));
			try {
				int indexCount;
				try (Transaction tx = graphDb.beginTx()) {
					long numberOfNodes = (Long) tx.execute("MATCH (n) RETURN count(n)").columnAs("count(n)").next();
					long numberOfRelationships = (Long) tx.execute("MATCH ()-[r]->() RETURN count(r)")
							.columnAs("count(r)").next();
					System.out.println("Opened database with " + numberOfNodes + " nodes and " + numberOfRelationships
							+ " relationships");

					System.out.println(
							"Searching for '" + layerName + "' in " + spatial.getLayerNames(tx).length + " layers:");
					for (String name : spatial.getLayerNames(tx)) {
						System.out.println("\t" + name);
					}
					//OSMLayer layer = (OSMLayer) spatial.getOrCreateLayer(layerName, OSMGeometryEncoder.class, OSMLayer.class);
					Layer layer = spatial.getLayer(tx, layerName);
					assertNotNull(layer.getIndex(), "Layer index should not be null");
					assertNotNull(layer.getIndex().getBoundingBox(tx), "Layer index envelope should not be null");
					Envelope bbox = Utilities.fromNeo4jToJts(layer.getIndex().getBoundingBox(tx));
					TestOSMImport.debugEnvelope(bbox, layerName, Constants.PROP_BBOX);
					indexCount = TestOSMImport.checkIndexCount(tx, layer);
					tx.commit();
				}
				TestOSMImport.checkFeatureCount(graphDb, indexCount, layerName);

				HashMap<String, Performance> performances = new LinkedHashMap<>();

				// Import the sample data of points on a path (drive test)
				ArrayList<Coordinate> coordinates = new ArrayList<>();
				BufferedReader locations = new BufferedReader(new FileReader(tracePath));
				String line;
				Performance performance = new Performance("import");
				while ((line = locations.readLine()) != null) {
					String[] fields = line.split("\\s");
					if (fields.length > 1) {
						double latitude = Double.parseDouble(fields[0]);
						double longitude = Double.parseDouble(fields[1]);
						coordinates.add(new Coordinate(longitude, latitude));
					}
				}
				locations.close();
				performance.stop(coordinates.size());
				performances.put(performance.name, performance);

				// Slow Test, iterating over all Point objects
				double distanceInKm = 0.01;
				HashSet<Node> results = new HashSet<>();
				System.out.println(
						"Searching for geometries near " + coordinates.size() + " locations: " + coordinates.get(0)
								+ " ... "
								+ coordinates.get(coordinates.size() - 1));
				performance = new Performance("search points");
				try (Transaction tx = graphDb.beginTx()) {
					Layer layer = spatial.getLayer(tx, layerName);
					for (Coordinate coordinate : coordinates) {
						List<Node> res = GeoPipeline.startNearestNeighborLatLonSearch(tx, layer, coordinate,
										distanceInKm)
								.sort(OrthodromicDistance.DISTANCE).toNodeList();
						results.addAll(res);
					}
					printResults(results);
					performance.stop(results);
					tx.commit();
				}
				performances.put(performance.name, performance);
				System.out.println("Point search took " + performance.duration() + " seconds to find " + results.size()
						+ " results");

				// Faster tests with LineString and MultiPoint
				GeometryFactory geometryFactory = new GeometryFactory();
				CoordinateArraySequence cseq = new CoordinateArraySequence(coordinates.toArray(new Coordinate[0]));
				HashMap<String, Geometry> testGeoms = new LinkedHashMap<>();
				testGeoms.put("LineString", geometryFactory.createLineString(cseq));
				testGeoms.put("LineString.buffer(0.001)", testGeoms.get("LineString").buffer(0.001));
				testGeoms.put("LineString.buffer(0.0001)", testGeoms.get("LineString").buffer(0.0001));
				testGeoms.put("LineString.buffer(0.00001)", testGeoms.get("LineString").buffer(0.00001));
				testGeoms.put("Simplified.LS.buffer(0.0001)",
						TopologyPreservingSimplifier.simplify(testGeoms.get("LineString").buffer(0.0001), 0.00005));
				if (testMultiPoint) {
					testGeoms.put("MultiPoint", geometryFactory.createMultiPoint(cseq));
					testGeoms.put("MultiPoint.buffer(0.001)", testGeoms.get("MultiPoint").buffer(0.001));
					testGeoms.put("MultiPoint.buffer(0.0001)", testGeoms.get("MultiPoint").buffer(0.0001));
					testGeoms.put("MultiPoint.buffer(0.00001)", testGeoms.get("MultiPoint").buffer(0.00001));
					testGeoms.put("Simplified.MP.buffer(0.0001)",
							TopologyPreservingSimplifier.simplify(testGeoms.get("MultiPoint").buffer(0.0001), 0.00005));
				}
				for (Entry<String, Geometry> entry : testGeoms.entrySet()) {
					String gname = entry.getKey();
					Geometry geometry = entry.getValue();
					System.out.println("Searching for geometries near Geometry: " + gname);
					performance = new Performance(gname);
					try (Transaction tx = graphDb.beginTx()) {
						Layer layer = spatial.getLayer(tx, layerName);
						List<Node> res = runSearch(GeoPipeline.startIntersectSearch(tx, layer, geometry), true);
						performance.stop(res);
						performances.put(performance.name, performance);
						System.out.println(
								"Geometry search took " + performance.duration() + " seconds to find " + res.size()
										+ " results");
						tx.commit();
					}
				}

				// Print summary of results
				System.out.println("\nActivity\tDuration\tResults\tOverlap");
				for (Performance perf : performances.values()) {
					System.out.println(
							perf.name + "\t" + perf.duration() + "\t" + perf.count + "\t" + perf.overlaps(results));
				}

				// For lambda exceptions
				return null;
			} catch (Exception e) {
				// For lambda exceptions
				return e;
			}
		});
	}

	private static void printResults(Collection<Node> results) {
		System.out.println("\tFound " + results.size() + " results:");
		int count = 0;
		for (Node node : results) {
			Object name = node.hasProperty("NAME") ? node.getProperty("NAME") : (node.hasProperty("name") ? node
					.getProperty("name") : node);
			System.out.println("\t\t" + name);
			if (++count >= 5) {
				break;
			}
		}
		if (results.size() > 5) {
			System.out.println("\t\t...");
		}
	}

	@SuppressWarnings("SameParameterValue")
	private static List<Node> runSearch(GeoPipeline pipeline, boolean verbose) {
		List<Node> results = pipeline.toNodeList();
		if (verbose) {
			printResults(results);
		}
		return results;
	}

	private static void withDatabase(String dbRoot, String dbName, Map<Setting<?>, Object> rawConfig,
			Function<GraphDatabaseService, Exception> withDb) throws RuntimeException {
		DatabaseManagementService databases = new DatabaseManagementServiceBuilder(
				new File(dbRoot, dbName).toPath()).setConfig(rawConfig).build();
		try {
			GraphDatabaseService graphDb = databases.database(DEFAULT_DATABASE_NAME);
			Exception e = withDb.apply(graphDb);
			if (e != null) {
				throw new RuntimeException(e);
			}
		} finally {
			databases.shutdown();
		}
	}
}
