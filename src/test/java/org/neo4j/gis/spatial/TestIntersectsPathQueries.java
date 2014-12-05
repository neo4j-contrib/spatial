/**
 * Copyright (c) 2010-2013 "Neo Technology,"
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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier;

import junit.framework.TestCase;

import org.geotools.data.shapefile.shp.ShapefileException;
import org.junit.Test;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import javax.xml.stream.XMLStreamException;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.Map.Entry;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;

public class TestIntersectsPathQueries extends TestCase {

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
	 * 
	 * @throws ParseException
	 * @throws XMLStreamException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testPointSetGeoptimaIntersection() throws ParseException, IOException, XMLStreamException, InterruptedException {
		String osmPath = "albert/osm/massachusetts.highway.osm";
		String shpPath = "albert/shp/massachusetts_highway.shp";
		String dbPath = "target/massachusetts.highway.db";
		String layerName = "massachusetts";
		String tracePath = "albert/locations_input.txt";
		File dbDir = new File(dbPath);
		if(dbDir.exists() && dbDir.isDirectory()) {
			System.out.println("Found database["+dbPath+"]  - running test directly on existing database");
			runTestPointSetGeoptimaIntersection(tracePath, dbPath, layerName, false);
		} else if((new File(shpPath)).exists()) {
			System.out.println("No database["+dbPath+"] but found shp["+shpPath+"] - importing before running test");
			importShapefileDatabase(shpPath, dbPath, layerName);
			runTestPointSetGeoptimaIntersection(tracePath, dbPath, layerName, false);
		} else if((new File(osmPath)).exists()) {
			System.out.println("No database["+dbPath+"] but found osm["+osmPath+"] - importing before running test");
			importOSMDatabase(osmPath, dbPath, layerName);
			runTestPointSetGeoptimaIntersection(tracePath, dbPath, layerName, false);
		} else {
			System.out.println("No database["+dbPath+"] or osm["+osmPath+"] - cannot run test");
		}
	}
	
	private void importShapefileDatabase(String shpPath, String dbPath, String layerName) throws ShapefileException, FileNotFoundException, IOException {
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath).setConfig(Neo4jTestCase.LARGE_CONFIG ).newGraphDatabase();
        ShapefileImporter importer = new ShapefileImporter(graphDb, new ConsoleListener(), 10000, true);
        importer.setFilterEnvelope(makeFilterEnvelope());
        importer.importFile(shpPath, layerName, Charset.forName("UTF-8"));
        graphDb.shutdown();
	}

	private Envelope makeFilterEnvelope() {
		Envelope filterEnvelope = new Envelope();
		filterEnvelope.expandToInclude(new Coordinate(-71.00, 42.10));
		filterEnvelope.expandToInclude(new Coordinate(-71.70, 42.50));
		return filterEnvelope;
	}
	
	private void importOSMDatabase(String osmPath, String dbPath, String layerName) throws ParseException, IOException, XMLStreamException, InterruptedException {
		OSMImporter importer = new OSMImporter(layerName, new ConsoleListener(), makeFilterEnvelope());
		importer.setCharset(Charset.forName("UTF-8"));
        BatchInserter batchInserter = BatchInserters.inserter(dbPath, Neo4jTestCase.LARGE_CONFIG);
        //GraphDatabaseService graphDb = batchInserter.getGraphDbService();
		//importer.importFile(graphDb, osmPath, false, 10000, true);
		importer.importFile(batchInserter, osmPath, false);
		batchInserter.shutdown();
		//graphDb.shutdown();
		// Weird hack to force GC on large loads
		long start = System.currentTimeMillis();
		if (System.currentTimeMillis() - start > 300000) {
			for (int i = 0; i < 3; i++) {
				System.gc();
				Thread.sleep(1000);
			}
		}
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath).setConfig(Neo4jTestCase.LARGE_CONFIG ).newGraphDatabase();
		importer.reIndex(graphDb, 10000, false, false);
		TestOSMImport.checkOSMLayer(graphDb, layerName);
		graphDb.shutdown();
	}

	private class Performance {
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
			} else if (original.size() < results.size()) {
				return fractionOf(results, original);
			} else {
				return fractionOf(original, results);
			}
		}

		private double fractionOf(Collection<Node> subset, Collection<Node> set) {
			HashSet<Node> all = new HashSet<Node>(set);
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

		public String toString() {
			return name + "\t" + duration;
		}
	}

	private void runTestPointSetGeoptimaIntersection(String tracePath, String dbPath, String layerName, boolean testMultiPoint) throws ParseException, IOException, XMLStreamException {
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath).setConfig( Neo4jTestCase.NORMAL_CONFIG ).newGraphDatabase();
		SpatialDatabaseService spatial = new SpatialDatabaseService(graphDb);
                System.out.println("Opened database with node count=" + ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( NeoStoreProvider.class ).evaluate().getNodeStore().getNumberOfIdsInUse());

		System.out.println("Searching for '"+layerName+"' in "+spatial.getLayerNames().length+" layers:");
		for(String name:spatial.getLayerNames()){
			System.out.println("\t"+name);
		}
		//OSMLayer layer = (OSMLayer) spatial.getOrCreateLayer(layerName, OSMGeometryEncoder.class, OSMLayer.class);
		Layer layer = spatial.getLayer(layerName);
		assertNotNull("Layer index should not be null", layer.getIndex());
		assertNotNull("Layer index envelope should not be null", layer.getIndex().getBoundingBox());
		Envelope bbox = Utilities.fromNeo4jToJts(layer.getIndex().getBoundingBox());
		TestOSMImport.debugEnvelope(bbox, layerName, Constants.PROP_BBOX);
		TestOSMImport.checkIndexAndFeatureCount(layer);

		HashMap<String,Performance> performances = new LinkedHashMap<String,Performance>(); 

		// Import the sample data of points on a path (drive test)
		ArrayList<Coordinate> coordinates = new ArrayList<Coordinate>();
		BufferedReader locations = new BufferedReader(new FileReader(tracePath));
		String line;
		Performance performance = new Performance("import");
		while ((line = locations.readLine()) != null) {
			String[] fields = line.split("\\s");
			if(fields.length>1) {
				double latitude = Double.parseDouble(fields[0]);
				double longitude = Double.parseDouble(fields[1]);
				coordinates.add(new Coordinate(longitude,latitude));
			}
		}
		locations.close();
		performance.stop(coordinates.size());
		performances.put(performance.name,performance);

		// Slow Test, iterating over all Point objects
		double distanceInKm = 0.01;
		HashSet<Node> results = new HashSet<Node>();
		System.out.println("Searching for geometries near "+coordinates.size()+" locations: " + coordinates.get(0) + " ... "
				+ coordinates.get(coordinates.size() - 1));
		performance = new Performance("search points");
		try (Transaction tx = graphDb.beginTx()) {
			for (Coordinate coordinate : coordinates) {
				List<Node> res = GeoPipeline.startNearestNeighborLatLonSearch(layer, coordinate, distanceInKm)
						.sort("OrthodromicDistance").toNodeList();
				results.addAll(res);
			}
			printResults(results);
			performance.stop(results);
			tx.success();
		}
		performances.put(performance.name,performance);
		System.out.println("Point search took "+performance.duration()+" seconds to find "+results.size()+" results");

		// Faster tests with LineString and MultiPoint 
		GeometryFactory geometryFactory = new GeometryFactory();
		CoordinateArraySequence cseq = new CoordinateArraySequence(coordinates.toArray(new Coordinate[0]));
		HashMap<String,Geometry> testGeoms = new LinkedHashMap<String,Geometry>();
		testGeoms.put("LineString", geometryFactory.createLineString(cseq));
		testGeoms.put("LineString.buffer(0.001)",   testGeoms.get("LineString").buffer(0.001));
		testGeoms.put("LineString.buffer(0.0001)",  testGeoms.get("LineString").buffer(0.0001));
		testGeoms.put("LineString.buffer(0.00001)",  testGeoms.get("LineString").buffer(0.00001));
		testGeoms.put("Simplified.LS.buffer(0.0001)",  TopologyPreservingSimplifier.simplify(testGeoms.get("LineString").buffer(0.0001),0.00005));
		if (testMultiPoint) {
			testGeoms.put("MultiPoint", geometryFactory.createMultiPoint(cseq));
			testGeoms.put("MultiPoint.buffer(0.001)",   testGeoms.get("MultiPoint").buffer(0.001));
			testGeoms.put("MultiPoint.buffer(0.0001)",  testGeoms.get("MultiPoint").buffer(0.0001));
			testGeoms.put("MultiPoint.buffer(0.00001)",  testGeoms.get("MultiPoint").buffer(0.00001));
			testGeoms.put("Simplified.MP.buffer(0.0001)",  TopologyPreservingSimplifier.simplify(testGeoms.get("MultiPoint").buffer(0.0001),0.00005));
		}
		for (Entry<String, Geometry> entry: testGeoms.entrySet()) {
			String gname = entry.getKey();
			Geometry geometry = entry.getValue();
			System.out.println("Searching for geometries near Geometry: " + gname);
			performance = new Performance(gname);
			try (Transaction tx = graphDb.beginTx()) {
				List<Node> res = runSearch(GeoPipeline.startIntersectSearch(layer, geometry), true);
				performance.stop(res);
				performances.put(performance.name,performance);
				System.out.println("Geometry search took " + performance.duration() + " seconds to find " + res.size() + " results");
				tx.success();
			}
		}

		// Print summary of results
		System.out.println("\nActivity\tDuration\tResults\tOverlap");
		for(Performance perf: performances.values()) {
			System.out.println(perf.name + "\t" + perf.duration() + "\t" + perf.count + "\t" + perf.overlaps(results));
		}

		// Finished
		graphDb.shutdown();
	}
	
	private void printResults(Collection<Node> results) {
		System.out.println("\tFound " + results.size() + " results:");
		int count = 0;
		for (Node node : results) {
			Object name = node.hasProperty("NAME") ? node.getProperty("NAME") : (node.hasProperty("name") ? node
					.getProperty("name") : node);
			System.out.println("\t\t" + name);
			if(++count >= 5) {
				break;
			}
		}
		if (results.size() > 5) {
			System.out.println("\t\t...");
		}		
	}

	private List<Node> runSearch(GeoPipeline pipeline, boolean verbose) {
		List<Node> results = pipeline.toNodeList();
		if (verbose) {
			printResults(results);
		}
		return results;
	}
}
