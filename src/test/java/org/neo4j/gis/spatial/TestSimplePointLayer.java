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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.geotools.data.neo4j.StyledImageExporter;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jCRS;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jPoint;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.pipes.processing.OrthodromicDistance;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.opentest4j.AssertionFailedError;

public class TestSimplePointLayer extends Neo4jTestCase {

	private static final Coordinate testOrigin = new Coordinate(13.0, 55.6);

	@Test
	public void testNearestNeighborSearchOnEmptyLayer() {
		String layerName = "test";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		try (Transaction tx = graphDb().beginTx()) {
			EditableLayer layer = spatial.createSimplePointLayer(tx, layerName, "Longitude", "Latitude");
			assertNotNull(layer);
			tx.commit();
		}

		try (Transaction tx = graphDb().beginTx()) {
			EditableLayer layer = (EditableLayer) spatial.getLayer(tx, layerName);
			// finds geometries around point
			List<SpatialDatabaseRecord> results = GeoPipeline
					.startNearestNeighborLatLonSearch(tx, layer, new Coordinate(15.3, 56.2), 1.0)
					.toSpatialDatabaseRecordList();

			// should find no results
			assertEquals(0, results.size());
			tx.commit();
		}

	}

	@Test
	public void testSimplePointLayer() {
		String layerName = "test";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		try (Transaction tx = graphDb().beginTx()) {
			EditableLayer layer = spatial.createSimplePointLayer(tx, layerName, "Longitude", "Latitude");
			assertNotNull(layer);
			SpatialRecord record = layer.add(tx, layer.getGeometryFactory().createPoint(new Coordinate(15.3, 56.2)));
			assertNotNull(record);
			tx.commit();
		}

		try (Transaction tx = graphDb().beginTx()) {
			EditableLayer layer = (EditableLayer) spatial.getLayer(tx, layerName);
			// finds geometries that contain the given geometry
			Geometry geometry = layer.getGeometryFactory()
					.toGeometry(new org.locationtech.jts.geom.Envelope(15.0, 16.0, 56.0, 57.0));
			List<SpatialDatabaseRecord> results = GeoPipeline.startContainSearch(tx, layer, geometry)
					.toSpatialDatabaseRecordList();

			// should not be contained
			assertEquals(0, results.size());

			results = GeoPipeline.startWithinSearch(tx, layer, geometry).toSpatialDatabaseRecordList();

			assertEquals(1, results.size());
			tx.commit();
		}
	}

	@Test
	public void testNativePointLayer() {
		String layerName = "test";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			EditableLayer layer = spatial.createNativePointLayer(tx, layerName, "location");
			assertNotNull(layer);
			SpatialRecord record = layer.add(tx, layer.getGeometryFactory().createPoint(new Coordinate(15.3, 56.2)));
			assertNotNull(record);
		});

		try (Transaction tx = graphDb().beginTx()) {
			EditableLayer layer = (EditableLayer) spatial.getLayer(tx, layerName);
			// finds geometries that contain the given geometry
			Geometry geometry = layer.getGeometryFactory()
					.toGeometry(new org.locationtech.jts.geom.Envelope(15.0, 16.0, 56.0, 57.0));
			List<SpatialDatabaseRecord> results = GeoPipeline.startContainSearch(tx, layer, geometry)
					.toSpatialDatabaseRecordList();

			// should not be contained
			assertEquals(0, results.size());

			results = GeoPipeline.startWithinSearch(tx, layer, geometry).toSpatialDatabaseRecordList();

			assertEquals(1, results.size());
			tx.commit();
		}
	}

	@Test
	public void testNeoTextLayer() {
		String layerName = "neo-text";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			SimplePointLayer layer = spatial.createSimplePointLayer(tx, layerName);
			assertNotNull(layer);
			assertNotNull(layer.getName(), "layer name is not null");
			for (Coordinate coordinate : makeCoordinateDataFromTextFile("NEO4J-SPATIAL.txt", testOrigin)) {
				SpatialRecord record = layer.add(tx, coordinate);
				assertNotNull(record);
			}
		});
		saveLayerAsImage(layerName, 700, 70);

		try (Transaction tx = graphDb().beginTx()) {
			SimplePointLayer layer = (SimplePointLayer) spatial.getLayer(tx, layerName);
			Envelope bbox = layer.getIndex().getBoundingBox(tx);
			double[] centre = bbox.centre();

			List<GeoPipeFlow> results = GeoPipeline
					.startNearestNeighborLatLonSearch(tx, layer, new Coordinate(centre[0] + 0.1, centre[1]), 10.0)
					.sort(OrthodromicDistance.DISTANCE).toList();

			saveResultsAsImage(results, "temporary-results-layer-" + layer.getName(), 130, 70);
			assertEquals(71, results.size());
			checkPointOrder(results);

			results = GeoPipeline
					.startNearestNeighborLatLonSearch(tx, layer, new Coordinate(centre[0] + 0.1, centre[1]), 5.0)
					.sort(OrthodromicDistance.DISTANCE).toList();

			saveResultsAsImage(results, "temporary-results-layer2-" + layer.getName(), 130, 70);
			assertEquals(30, results.size());
			checkPointOrder(results);

			// Now test the old API
			results = layer.findClosestPointsTo(tx, new Coordinate(centre[0] + 0.1, centre[1]), 10.0);
			assertEquals(71, results.size());
			checkPointOrder(results);
			results = layer.findClosestPointsTo(tx, new Coordinate(centre[0] + 0.1, centre[1]), 1000);
			assertEquals(265, results.size());    // There are only 265 points in dataset
			checkPointOrder(results);
			results = layer.findClosestPointsTo(tx, new Coordinate(centre[0] + 0.1, centre[1]), 100);
			assertEquals(100,
					results.size());    // We expect an exact count from the layer method (but not from the pipeline)
			checkPointOrder(results);
			results = layer.findClosestPointsTo(tx, new Coordinate(centre[0] + 0.1, centre[1]));
			assertEquals(100, results.size());    // The default in SimplePointLayer is 100 results
			checkPointOrder(results);
			tx.commit();
		}

	}

	@Test
	public void testIndexingExistingSimplePointNodes() {
		String layerName = "my-simple-points";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		inTx(tx -> spatial.createSimplePointLayer(tx, layerName, "x", "y"));

		Coordinate[] coords = makeCoordinateDataFromTextFile("NEO4J-SPATIAL.txt", testOrigin);
		inTx(tx -> {
			Layer layer = spatial.getLayer(tx, layerName);
			for (Coordinate coordinate : coords) {
				Node n = tx.createNode();
				n.setProperty("x", coordinate.x);
				n.setProperty("y", coordinate.y);
				layer.add(tx, n);
			}
		});
		saveLayerAsImage(layerName, 700, 70);
		assertIndexCountSameAs(layerName, coords.length);
	}

	@Test
	public void testIndexingExistingNativePointNodes() {
		String layerName = "my-native-points";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		inTx(tx -> spatial.createNativePointLayer(tx, "my-native-points", "position"));
		Neo4jCRS crs = Neo4jCRS.findCRS("WGS-84");

		Coordinate[] coords = makeCoordinateDataFromTextFile("NEO4J-SPATIAL.txt", testOrigin);
		inTx(tx -> {
			Layer layer = spatial.getLayer(tx, layerName);
			for (Coordinate coordinate : coords) {
				Node n = tx.createNode();
				n.setProperty("x", coordinate.x);
				n.setProperty("y", coordinate.y);
				n.setProperty("position", new Neo4jPoint(coordinate, crs));
				layer.add(tx, n);
			}
		});
		saveLayerAsImage(layerName, 700, 70);
		assertIndexCountSameAs(layerName, coords.length);
	}

	@Test
	public void testIndexingExistingPointNodesWithMultipleLocations() {
		String layerNameA = "my-points-A";
		String layerNameB = "my-points-B";
		String layerNameC = "my-points-C";
		GraphDatabaseService db = graphDb();
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		double x_offset = 0.15, y_offset = 0.15;
		inTx(tx -> {
			spatial.createSimplePointLayer(tx, layerNameA, "xa", "ya", "bbox_a");
			spatial.createSimplePointLayer(tx, layerNameB, "xb", "yb", "bbox_b");
			spatial.createNativePointLayer(tx, layerNameC, "loc", "bbox_c");
		});
		Neo4jCRS crs = Neo4jCRS.findCRS("WGS-84");

		Coordinate[] coords = makeCoordinateDataFromTextFile("NEO4J-SPATIAL.txt", testOrigin);
		try (Transaction tx = db.beginTx()) {
			Layer layerA = spatial.getLayer(tx, layerNameA);
			Layer layerB = spatial.getLayer(tx, layerNameB);
			Layer layerC = spatial.getLayer(tx, layerNameC);
			for (Coordinate coordinate : coords) {
				Node n = tx.createNode();
				n.setProperty("xa", coordinate.x);
				n.setProperty("ya", coordinate.y);
				n.setProperty("xb", coordinate.x + x_offset);
				n.setProperty("yb", coordinate.y + y_offset);
				n.setProperty("loc",
						new Neo4jPoint(new double[]{coordinate.x + 2 * x_offset, coordinate.y + 2 * y_offset}, crs));

				layerA.add(tx, n);
				layerB.add(tx, n);
				layerC.add(tx, n);
			}
			tx.commit();
		}
		saveLayerAsImage(layerNameA, 700, 70);
		saveLayerAsImage(layerNameB, 700, 70);
		saveLayerAsImage(layerNameC, 700, 70);

		List<SpatialDatabaseRecord> results = new ArrayList<>();
		inTx(tx -> {
			Layer layerA = spatial.getLayer(tx, layerNameA);
			Layer layerB = spatial.getLayer(tx, layerNameB);
			Layer layerC = spatial.getLayer(tx, layerNameC);
			Envelope bboxA = layerA.getIndex().getBoundingBox(tx);
			Envelope bboxB = layerB.getIndex().getBoundingBox(tx);
			Envelope bboxC = layerC.getIndex().getBoundingBox(tx);
			double[] centreA = bboxA.centre();
			double[] centreB = bboxB.centre();
			double[] centreC = bboxC.centre();

			List<SpatialDatabaseRecord> resultsA;
			List<SpatialDatabaseRecord> resultsB;
			List<SpatialDatabaseRecord> resultsC;
			resultsA = GeoPipeline.startNearestNeighborLatLonSearch(tx, layerA,
					new Coordinate(centreA[0] + 0.1, centreA[1]), 10.0).toSpatialDatabaseRecordList();
			resultsB = GeoPipeline.startNearestNeighborLatLonSearch(tx, layerB,
					new Coordinate(centreB[0] + 0.1, centreB[1]), 10.0).toSpatialDatabaseRecordList();
			resultsC = GeoPipeline.startNearestNeighborLatLonSearch(tx, layerC,
					new Coordinate(centreC[0] + 0.1, centreC[1]), 10.0).toSpatialDatabaseRecordList();
			results.addAll(resultsA);
			results.addAll(resultsB);
			results.addAll(resultsC);
			assertEquals(71, resultsA.size());
			assertEquals(71, resultsB.size());
			assertEquals(71, resultsC.size());
			assertEquals(213, results.size());
			saveResultsAsImage(resultsA, "temporary-results-layer-" + layerA.getName(), 130, 70);
			saveResultsAsImage(resultsB, "temporary-results-layer-" + layerB.getName(), 130, 70);
			saveResultsAsImage(resultsC, "temporary-results-layer-" + layerC.getName(), 130, 70);
			saveResultsAsImage(results,
					"temporary-results-layer-" + layerA.getName() + "-" + layerB.getName() + "-" + layerC.getName(),
					200, 200);
		});

		assertIndexCountSameAs(layerNameA, coords.length);
		assertIndexCountSameAs(layerNameB, coords.length);
		assertIndexCountSameAs(layerNameC, coords.length);
	}

	private static void checkPointOrder(List<GeoPipeFlow> results) {
		for (int i = 0; i < results.size() - 1; i++) {
			GeoPipeFlow first = results.get(i);
			GeoPipeFlow second = results.get(i + 1);
			double d1 = (Double) first.getProperties().get(OrthodromicDistance.DISTANCE);
			double d2 = (Double) second.getProperties().get(OrthodromicDistance.DISTANCE);
			assertTrue(d1 <= d2,
					"Point at position " + i + " (d=" + d1 + ") must be closer than point at position " + (i + 1)
							+ " (d=" + d2 + ")");
		}
	}

	@Test
	public void testDensePointLayer() {
		String layerName = "neo-dense";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			SimplePointLayer layer = spatial.createSimplePointLayer(tx, layerName, "lon", "lat");
			assertNotNull(layer);
			for (Coordinate coordinate : makeDensePointData()) {
				Point point = layer.getGeometryFactory().createPoint(coordinate);
				SpatialRecord record = layer.add(tx, point);
				assertNotNull(record);
			}
		});

		saveLayerAsImage(layerName, 300, 300);

		inTx(tx -> {
			Layer layer = spatial.getLayer(tx, layerName);
			Envelope bbox = layer.getIndex().getBoundingBox(tx);
			double[] centre = bbox.centre();

			List<SpatialDatabaseRecord> results = GeoPipeline
					.startNearestNeighborLatLonSearch(tx, layer, new Coordinate(centre[0], centre[1]), 10.0)
					.toSpatialDatabaseRecordList();
			saveResultsAsImage(results, "temporary-results-layer-" + layer.getName(), 150, 150);
			assertEquals(456, results.size());

			// Repeat with sorting
			results = GeoPipeline
					.startNearestNeighborLatLonSearch(tx, layer, new Coordinate(centre[0], centre[1]), 10.0)
					.sort(OrthodromicDistance.DISTANCE)
					.toSpatialDatabaseRecordList();
			saveResultsAsImage(results, "temporary-results-layer-sorted-" + layer.getName(), 150, 150);
			assertEquals(456, results.size());
		});
	}

	private void saveLayerAsImage(String layerName, int width, int height) {
		ShapefileExporter shpExporter = new ShapefileExporter(graphDb());
		shpExporter.setExportDir("target/export/SimplePointTests");
		StyledImageExporter imageExporter = new StyledImageExporter(graphDb());
		imageExporter.setExportDir("target/export/SimplePointTests");
		imageExporter.setZoom(0.9);
		imageExporter.setSize(width, height);
		try {
			imageExporter.saveLayerImage(layerName);
			shpExporter.exportLayer(layerName);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AssertionFailedError("Failed to save layer '" + layerName + "' as image: " + e.getMessage());
		}
	}

	private void saveResultsAsImage(List<? extends SpatialRecord> results, String layerName, int width, int height) {
		ShapefileExporter shpExporter = new ShapefileExporter(graphDb());
		shpExporter.setExportDir("target/export/SimplePointTests");
		StyledImageExporter imageExporter = new StyledImageExporter(graphDb());
		imageExporter.setExportDir("target/export/SimplePointTests");
		imageExporter.setZoom(0.9);
		imageExporter.setSize(width, height);
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			EditableLayer tmpLayer = spatial.createSimplePointLayer(tx, layerName, "lon", "lat");
			for (SpatialRecord record : results) {
				tmpLayer.add(tx, record.getGeometry());
			}
		});
		try {
			imageExporter.saveLayerImage(layerName);
			shpExporter.exportLayer(layerName);
		} catch (Exception e) {
			throw new AssertionFailedError("Failed to save results image: " + e.getMessage());
		}
	}

	@SuppressWarnings({"SameParameterValue"})
	private static Coordinate[] makeCoordinateDataFromTextFile(String textFile, Coordinate origin) {
		CoordinateList data = new CoordinateList();
		try {
			BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/" + textFile));
			String line;
			int row = 0;
			while ((line = reader.readLine()) != null) {
				int col = 0;
				for (String character : line.split("")) {
					if (col > 0 && !character.matches("\\s")) {
						Coordinate coordinate = new Coordinate(origin.x + (double) col / 100.0,
								origin.y - (double) row / 100.0);
						data.add(coordinate);
					}
					col++;
				}
				row++;
			}
		} catch (IOException e) {
			throw new AssertionFailedError("Input data for string test invalid: " + e.getMessage());
		}
		return data.toCoordinateArray();
	}

	private static Coordinate[] makeDensePointData() {
		CoordinateList data = new CoordinateList();
		Coordinate origin = new Coordinate(13.0, 55.6);
		for (int row = 0; row < 40; row++) {
			for (int col = 0; col < 40; col++) {
				Coordinate coordinate = new Coordinate(origin.x + (double) col / 100.0,
						origin.y - (double) row / 100.0);
				data.add(coordinate);
			}
		}
		return data.toCoordinateArray();
	}

	private void assertIndexCountSameAs(String layerName, int count) {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		try (Transaction tx = graphDb().beginTx()) {
			int indexCount = spatial.getLayer(tx, layerName).getIndex().count(tx);
			assertEquals(count, indexCount);
			tx.commit();
		}
	}

	private void inTx(Consumer<Transaction> txFunction) {
		try (Transaction tx = graphDb().beginTx()) {
			txFunction.accept(tx);
			tx.commit();
		}
	}
}
