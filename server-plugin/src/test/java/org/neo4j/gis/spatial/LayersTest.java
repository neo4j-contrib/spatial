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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.Envelope;
import org.neo4j.gis.spatial.encoders.NativePointEncoder;
import org.neo4j.gis.spatial.encoders.SimpleGraphEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.encoders.SimplePropertyEncoder;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.index.IndexManagerImpl;
import org.neo4j.gis.spatial.index.LayerGeohashPointIndex;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.gis.spatial.rtree.ProgressLoggingListener;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spatial.api.SpatialRecord;
import org.neo4j.spatial.api.encoder.GeometryEncoder;
import org.neo4j.spatial.api.index.SpatialIndexWriter;
import org.neo4j.spatial.api.layer.EditableLayer;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.testutils.Neo4jTestCase;

public class LayersTest extends Neo4jTestCase {

	private static final Logger LOGGER = Logger.getLogger(LayersTest.class.getName());

	@Override
	protected List<Class<?>> loadProceduresAndFunctions() {
		return List.of(SpatialFunctions.class, SpatialProcedures.class);
	}

	@Test
	public void testBasicLayerOperations() {
		String layerName = "test";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			Layer layer = spatial.getLayer(tx, layerName, true);
			assertNull(layer);
		});
		inTx(tx -> {
			Layer layer = spatial.createWKBLayer(tx, layerName, null);
			assertNotNull(layer);
			assertThat("Should be a default layer", layer instanceof DefaultLayer);
		});
		inTx(tx -> spatial.deleteLayer(tx, layerName,
				new ProgressLoggingListener("deleting layer '" + layerName + "'", LOGGER)));
		inTx(tx -> assertNull(spatial.getLayer(tx, layerName, true)));
	}

	static Stream<Arguments> pointIndexesAndEncoders() {
		return Stream.of(
				arguments(LayerRTreeIndex.class, SimplePointEncoder.class),
				arguments(LayerGeohashPointIndex.class, SimplePointEncoder.class),
				arguments(LayerRTreeIndex.class, NativePointEncoder.class),
				arguments(LayerGeohashPointIndex.class, NativePointEncoder.class));
	}

	@ParameterizedTest
	@MethodSource("pointIndexesAndEncoders")
	public void testPointLayer(Class<? extends SpatialIndexWriter> indexClass,
			Class<? extends GeometryEncoder> encoderClass) {
		String layerName = "points";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			EditableLayer layer = (EditableLayer) spatial.createLayer(tx, layerName, encoderClass,
					EditableLayerImpl.class, indexClass, null, null);
			assertNotNull(layer);
		});
		inTx(tx -> {
			EditableLayer layer = (EditableLayer) spatial.getLayer(tx, layerName, false);
			SpatialRecord record = layer.add(tx,
					layer.getGeometryFactory().createPoint(new Coordinate(15.3, 56.2)));
			layer.finalizeTransaction(tx);
			assertNotNull(record);
		});
		// finds geometries that contain the given geometry
		try (Transaction tx = graphDb().beginTx()) {
			Layer layer = spatial.getLayer(tx, layerName, true);
			List<SpatialRecord> results = GeoPipeline
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
				new ProgressLoggingListener("deleting layer '" + layerName + "'", LOGGER)));
		inTx(tx -> assertNull(spatial.getLayer(tx, layerName, true)));
		IndexManagerImpl.waitForDeletions();
	}

	@ParameterizedTest
	@ValueSource(classes = {SimplePointEncoder.class, NativePointEncoder.class})
	public void testDeleteGeometry(Class<? extends GeometryEncoder> encoderClass) {
		String layerName = "test";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			EditableLayer layer = (EditableLayer) spatial.createLayer(tx, layerName, encoderClass,
					EditableLayerImpl.class, LayerRTreeIndex.class, null, null);
			assertNotNull(layer);
		});
		inTx(tx -> {
			EditableLayer layer = (EditableLayer) spatial.getLayer(tx, layerName, false);
			SpatialRecord record = layer.add(tx,
					layer.getGeometryFactory().createPoint(new Coordinate(15.3, 56.2)));
			assertNotNull(record);
			layer.delete(tx, record.getId());
			layer.finalizeTransaction(tx);
		});
		inTx(tx -> {
			Layer layer = spatial.getLayer(tx, layerName, true);
			assertEquals(0, layer.getIndex().count(tx));
			assertEquals(0, GeoPipeline.startWithinSearch(tx, layer,
							layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)))
					.toSpatialDatabaseRecordList().size());
		});
	}

	@Test
	public void testEditableLayers() {
		testSpecificEditableLayer("test dynamic layer with property encoder", SimplePropertyEncoder.class,
				DynamicLayer.class);
		testSpecificEditableLayer("test dynamic layer with graph encoder", SimpleGraphEncoder.class,
				DynamicLayer.class);
		testSpecificEditableLayer("test editable layer with property encoder", SimplePropertyEncoder.class,
				EditableLayerImpl.class);
		testSpecificEditableLayer("test editable layer with graph encoder", SimpleGraphEncoder.class,
				EditableLayerImpl.class);
	}

	private String testSpecificEditableLayer(String layerName, Class<? extends GeometryEncoder> geometryEncoderClass,
			Class<? extends Layer> layerClass) {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			Layer layer = spatial.createLayer(tx, layerName, geometryEncoderClass, layerClass, null);
			assertNotNull(layer);
			assertInstanceOf(EditableLayer.class, layer, "Should be an editable layer");
		});
		inTx(tx -> {
			Layer layer = spatial.getLayer(tx, layerName, false);
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
			editableLayer.finalizeTransaction(tx);
		});

		try (Transaction tx = graphDb().beginTx()) {
			EditableLayer layer = (EditableLayer) spatial.getLayer(tx, layerName, true);
			// only the first line crosses the window
			assertEquals(1, GeoPipeline
					.startIntersectSearch(tx, layer,
							layer.getGeometryFactory().toGeometry(new Envelope(13.2, 14.1, 56.1, 56.2)))
					.toSpatialDatabaseRecordList().size(), layerName);
			// no line contains the whole envelope
			assertEquals(0, GeoPipeline
					.startContainSearch(tx, layer,
							layer.getGeometryFactory().toGeometry(new Envelope(12.0, 15.0, 55.0, 57.0)))
					.toSpatialDatabaseRecordList().size(), layerName);
			tx.commit();
		}
		return layerName;
	}

	@Test
	public void testIndexAccessAfterBulkInsertion() {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		inTx(tx -> spatial.getOrCreateSimplePointLayer(tx, "Coordinates", "rtree", "lat", "lon", null, true));

		Random rand = new Random(42);

		try (Transaction tx = graphDb().beginTx()) {
			SimplePointLayer layer = (SimplePointLayer) spatial.getLayer(tx, "Coordinates", false);
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

		try (Transaction tx = graphDb().beginTx()) { // 'points',{longitude:15.0,latitude:60.0},100
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

		try (Transaction tx = graphDb().beginTx()) {
			String cypher = "MATCH ()-[:RTREE_ROOT]->(n)\n" +
					"MATCH (n)-[:RTREE_CHILD]->(m)-[:RTREE_REFERENCE]->(p)\n" +
					"RETURN count(p)";
			Result result = tx.execute(cypher);
			Object obj = result.columnAs("count(p)").next();
			assertInstanceOf(Long.class, obj);
			assertEquals(1000L, (long) ((Long) obj));
			tx.commit();
		}
	}
}
