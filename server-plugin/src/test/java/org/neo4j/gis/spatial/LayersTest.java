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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LineString;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.spatial.encoders.NativePointEncoder;
import org.neo4j.gis.spatial.encoders.SimpleGraphEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.encoders.SimplePropertyEncoder;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.index.LayerGeohashPointIndex;
import org.neo4j.gis.spatial.index.LayerIndexReader;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.gis.spatial.rtree.ProgressLoggingListener;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class LayersTest {

	private DatabaseManagementService databases;
	private GraphDatabaseService graphDb;

	@BeforeEach
	public void setup() throws KernelException {
		databases = new TestDatabaseManagementServiceBuilder(new File("target/layers").toPath()).impermanent().build();
		graphDb = databases.database(DEFAULT_DATABASE_NAME);
		((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(GlobalProcedures.class)
				.registerProcedure(SpatialProcedures.class);
	}

	@AfterEach
	public void teardown() {
		databases.shutdown();
	}

	@Test
	public void testBasicLayerOperations() {
		String layerName = "test";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb, SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			Layer layer = spatial.getLayer(tx, layerName);
			assertNull(layer);
		});
		inTx(tx -> {
			Layer layer = spatial.createWKBLayer(tx, layerName, null);
			assertNotNull(layer);
			assertThat("Should be a default layer", layer instanceof DefaultLayer);
		});
		inTx(tx -> spatial.deleteLayer(tx, layerName,
				new ProgressLoggingListener("deleting layer '" + layerName + "'", System.out)));
		inTx(tx -> assertNull(spatial.getLayer(tx, layerName)));
	}

	@Test
	public void testSimplePointLayerWithRTree() {
		testPointLayer(LayerRTreeIndex.class, SimplePointEncoder.class);
	}

	@Test
	public void testSimplePointLayerWithGeohash() {
		testPointLayer(LayerGeohashPointIndex.class, SimplePointEncoder.class);
	}

	@Test
	public void testNativePointLayerWithRTree() {
		testPointLayer(LayerRTreeIndex.class, NativePointEncoder.class);
	}

	@Test
	public void testNativePointLayerWithGeohash() {
		testPointLayer(LayerGeohashPointIndex.class, NativePointEncoder.class);
	}

	private void testPointLayer(Class<? extends LayerIndexReader> indexClass,
			Class<? extends GeometryEncoder> encoderClass) {
		String layerName = "points";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb, SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			EditableLayer layer = (EditableLayer) spatial.createLayer(tx, layerName, encoderClass,
					EditableLayerImpl.class, indexClass, null, null);
			assertNotNull(layer);
		});
		inTx(tx -> {
			EditableLayer layer = (EditableLayer) spatial.getLayer(tx, layerName);
			SpatialDatabaseRecord record = layer.add(tx,
					layer.getGeometryFactory().createPoint(new Coordinate(15.3, 56.2)));
			assertNotNull(record);
		});
		// finds geometries that contain the given geometry
		try (Transaction tx = graphDb.beginTx()) {
			Layer layer = spatial.getLayer(tx, layerName);
			List<SpatialDatabaseRecord> results = GeoPipeline
					.startContainSearch(tx, layer,
							layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)))
					.toSpatialDatabaseRecordList();

			// should not be contained
			assertEquals(0, results.size());

			results = GeoPipeline
					.startWithinSearch(tx, layer,
							layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)))
					.toSpatialDatabaseRecordList();

			assertEquals(1, results.size());
			tx.commit();
		}
		inTx(tx -> spatial.deleteLayer(tx, layerName,
				new ProgressLoggingListener("deleting layer '" + layerName + "'", System.out)));
		inTx(tx -> assertNull(spatial.getLayer(tx, layerName)));
		IndexManager.waitForDeletions();
	}

	@Test
	public void testDeleteSimplePointGeometry() {
		testDeleteGeometry(SimplePointEncoder.class);
	}

	@Test
	public void testDeleteNativePointGeometry() {
		testDeleteGeometry(NativePointEncoder.class);
	}

	private void testDeleteGeometry(Class<? extends GeometryEncoder> encoderClass) {
		String layerName = "test";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb, SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			EditableLayer layer = (EditableLayer) spatial.createLayer(tx, layerName, encoderClass,
					EditableLayerImpl.class, null, null, null);
			assertNotNull(layer);
		});
		inTx(tx -> {
			EditableLayer layer = (EditableLayer) spatial.getLayer(tx, layerName);
			SpatialDatabaseRecord record = layer.add(tx,
					layer.getGeometryFactory().createPoint(new Coordinate(15.3, 56.2)));
			assertNotNull(record);
			// try to remove the geometry
			layer.delete(tx, record.getNodeId());
		});
	}

	@Test
	public void testEditableLayer() {
		String layerName = "test";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb, SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			EditableLayer layer = spatial.getOrCreateEditableLayer(tx, layerName, null, null);
			assertNotNull(layer);
		});
		inTx(tx -> {
			EditableLayer layer = (EditableLayer) spatial.getLayer(tx, layerName);
			SpatialDatabaseRecord record = layer.add(tx,
					layer.getGeometryFactory().createPoint(new Coordinate(15.3, 56.2)));
			assertNotNull(record);
		});

		try (Transaction tx = graphDb.beginTx()) {
			EditableLayer layer = (EditableLayer) spatial.getLayer(tx, layerName);

			// finds geometries that contain the given geometry
			List<SpatialDatabaseRecord> results = GeoPipeline
					.startContainSearch(tx, layer,
							layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)))
					.toSpatialDatabaseRecordList();

			// should not be contained
			assertEquals(0, results.size());

			results = GeoPipeline
					.startWithinSearch(tx, layer,
							layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)))
					.toSpatialDatabaseRecordList();

			assertEquals(1, results.size());
			tx.commit();
		}
	}

	@Test
	public void testSnapToLine() {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb, SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			EditableLayer layer = spatial.getOrCreateEditableLayer(tx, "roads", null, null);
			Coordinate crossing_bygg_forstadsgatan = new Coordinate(13.0171471, 55.6074148);
			Coordinate[] waypoints_forstadsgatan = {new Coordinate(13.0201511, 55.6066846),
					crossing_bygg_forstadsgatan};
			LineString ostra_forstadsgatan_malmo = layer.getGeometryFactory().createLineString(waypoints_forstadsgatan);
			Coordinate[] waypoints_byggmastaregatan = {crossing_bygg_forstadsgatan,
					new Coordinate(13.0182092, 55.6088238)};
			LineString byggmastaregatan_malmo = layer.getGeometryFactory().createLineString(waypoints_byggmastaregatan);
			LineString[] test_way_segments = {byggmastaregatan_malmo, ostra_forstadsgatan_malmo};
			/* MultiLineString test_way = */
			layer.getGeometryFactory().createMultiLineString(test_way_segments);
		});
		inTx(tx -> {
			// Coordinate slussgatan14 = new Coordinate( 13.0181127, 55.608236 );
			//TODO now determine the nearest point on test_way to slussis
		});
	}

	@Test
	public void testEditableLayers() {
		testSpecificEditableLayer("test dynamic layer with property encoder", SimplePropertyEncoder.class,
				DynamicLayer.class);
		testSpecificEditableLayer("test dynamic layer with graph encoder", SimpleGraphEncoder.class,
				DynamicLayer.class);
		testSpecificEditableLayer("test OSM layer with OSM encoder", OSMGeometryEncoder.class, OSMLayer.class);
		testSpecificEditableLayer("test editable layer with property encoder", SimplePropertyEncoder.class,
				EditableLayerImpl.class);
		testSpecificEditableLayer("test editable layer with graph encoder", SimpleGraphEncoder.class,
				EditableLayerImpl.class);
		testSpecificEditableLayer("test editable layer with OSM encoder", OSMGeometryEncoder.class,
				EditableLayerImpl.class);
	}

	private String testSpecificEditableLayer(String layerName, Class<? extends GeometryEncoder> geometryEncoderClass,
			Class<? extends Layer> layerClass) {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb, SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			Layer layer = spatial.createLayer(tx, layerName, geometryEncoderClass, layerClass, null);
			assertNotNull(layer);
			assertInstanceOf(EditableLayer.class, layer, "Should be an editable layer");
		});
		inTx(tx -> {
			Layer layer = spatial.getLayer(tx, layerName);
			assertNotNull(layer);
			assertInstanceOf(EditableLayer.class, layer, "Should be an editable layer");
			EditableLayer editableLayer = (EditableLayer) layer;

			CoordinateList coordinates = new CoordinateList();
			coordinates.add(new Coordinate(13.1, 56.2), false);
			coordinates.add(new Coordinate(13.2, 56.0), false);
			coordinates.add(new Coordinate(13.3, 56.2), false);
			coordinates.add(new Coordinate(13.2, 56.0), false);
			coordinates.add(new Coordinate(13.1, 56.2), false);
			coordinates.add(new Coordinate(13.0, 56.0), false);
			editableLayer.add(tx, layer.getGeometryFactory().createLineString(coordinates.toCoordinateArray()));

			coordinates = new CoordinateList();
			coordinates.add(new Coordinate(14.1, 56.0), false);
			coordinates.add(new Coordinate(14.3, 56.1), false);
			coordinates.add(new Coordinate(14.2, 56.1), false);
			coordinates.add(new Coordinate(14.0, 56.0), false);
			editableLayer.add(tx, layer.getGeometryFactory().createLineString(coordinates.toCoordinateArray()));
		});

		// TODO this test is not complete

		try (Transaction tx = graphDb.beginTx()) {
			EditableLayer layer = (EditableLayer) spatial.getLayer(tx, layerName);
			printResults(layer, GeoPipeline
					.startIntersectSearch(tx, layer,
							layer.getGeometryFactory().toGeometry(new Envelope(13.2, 14.1, 56.1, 56.2)))
					.toSpatialDatabaseRecordList());

			printResults(layer, GeoPipeline
					.startContainSearch(tx, layer,
							layer.getGeometryFactory().toGeometry(new Envelope(12.0, 15.0, 55.0, 57.0)))
					.toSpatialDatabaseRecordList());
			tx.commit();
		}
		return layerName;
	}

	private static void printResults(Layer layer, List<SpatialDatabaseRecord> results) {
		System.out.println("\tTesting layer '" + layer.getName() + "' (class " + layer.getClass() + "), found results: "
				+ results.size());
		for (SpatialDatabaseRecord r : results) {
			System.out.println("\t\tGeometry: " + r);
		}
	}

	@Test
	public void testShapefileExport() throws Exception {
		ShapefileExporter exporter = new ShapefileExporter(graphDb);
		exporter.setExportDir("target/export");
		ArrayList<String> layers = new ArrayList<>();

		layers.add(testSpecificEditableLayer("test dynamic layer with property encoder", SimplePropertyEncoder.class,
				DynamicLayer.class));
		layers.add(testSpecificEditableLayer("test dynamic layer with graph encoder", SimpleGraphEncoder.class,
				DynamicLayer.class));
		layers.add(testSpecificEditableLayer("test dynamic layer with OSM encoder", OSMGeometryEncoder.class,
				OSMLayer.class));

		for (String layerName : layers) {
			exporter.exportLayer(layerName);
		}
	}

	@Test
	public void testIndexAccessAfterBulkInsertion() {
		// Use these two lines if you want to examine the output.
//        File dbPath = new File("target/var/BulkTest");
//        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath.getCanonicalPath());
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb, SecurityContext.AUTH_DISABLED));
		inTx(tx -> spatial.getOrCreateSimplePointLayer(tx, "Coordinates", "rtree", "lat", "lon", null));

		Random rand = new Random();

		try (Transaction tx = graphDb.beginTx()) {
			SimplePointLayer layer = (SimplePointLayer) spatial.getLayer(tx, "Coordinates");
			List<Node> coordinateNodes = new ArrayList<>();
			for (int i = 0; i < 1000; i++) {
				Node node = tx.createNode();
				node.addLabel(Label.label("Coordinates"));
				node.setProperty("lat", rand.nextDouble());
				node.setProperty("lon", rand.nextDouble());
				coordinateNodes.add(node);
			}
			layer.addAll(tx, coordinateNodes);
			layer.finalizeTransaction(tx);
			tx.commit();
		}

		try (Transaction tx = graphDb.beginTx()) { // 'points',{longitude:15.0,latitude:60.0},100
			Result result = tx.execute(
					"CALL spatial.withinDistance('Coordinates',{longitude:0.5, latitude:0.5},1000.0) YIELD node AS malmo");
			int i = 0;
			ResourceIterator<Node> thing = result.columnAs("malmo");
			while (thing.hasNext()) {
				assertNotNull(thing.next());
				i++;
			}
			assertEquals(i, 1000);
			tx.commit();
		}

		try (Transaction tx = graphDb.beginTx()) {
			String cypher = "MATCH ()-[:RTREE_ROOT]->(n)\n" +
					"MATCH (n)-[:RTREE_CHILD]->(m)-[:RTREE_REFERENCE]->(p)\n" +
					"RETURN count(p)";
			Result result = tx.execute(cypher);
//           System.out.println(result.columns().toString());
			Object obj = result.columnAs("count(p)").next();
			assertInstanceOf(Long.class, obj);
			assertEquals(1000L, (long) ((Long) obj));
			tx.commit();
		}
	}

	private void inTx(Consumer<Transaction> txFunction) {
		try (Transaction tx = graphDb.beginTx()) {
			txFunction.accept(tx);
			tx.commit();
		}
	}
}
