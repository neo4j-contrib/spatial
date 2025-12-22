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
package org.neo4j.gis.spatial.procedures;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gis.spatial.Constants.LABEL_LAYER;
import static org.neo4j.gis.spatial.Constants.PROP_GEOMENCODER;
import static org.neo4j.gis.spatial.Constants.PROP_GEOMENCODER_CONFIG;
import static org.neo4j.gis.spatial.Constants.PROP_INDEX_TYPE;
import static org.neo4j.gis.spatial.Constants.PROP_LAYER;
import static org.neo4j.gis.spatial.Constants.PROP_LAYER_TYPE;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.SpatialRelationshipTypes;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.index.IndexManagerImpl;
import org.neo4j.gis.spatial.utilities.ReferenceNodes;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.testutils.AbstractApiTest;

public class SpatialProceduresTest extends AbstractApiTest {

	private static final Logger LOGGER = Logger.getLogger(SpatialProceduresTest.class.getName());

	@Override
	protected void registerApiProceduresAndFunctions() throws KernelException {
		registerProceduresAndFunctions(SpatialProcedures.class);
		registerProceduresAndFunctions(SpatialFunctions.class);
	}

	public static void testCall(GraphDatabaseService db, String call, Consumer<Map<String, Object>> consumer) {
		testCall(db, call, null, consumer);
	}

	public static void testCall(GraphDatabaseService db, String call, Map<String, Object> params,
			Consumer<Map<String, Object>> consumer) {
		testCall(db, call, params, consumer, true);
	}

	public static void testCallFails(GraphDatabaseService db, String call, Map<String, Object> params, String error) {
		try {
			testResult(db, call, params, (res) -> {
				while (res.hasNext()) {
					res.next();
				}
			});
			fail("Expected an exception containing '" + error + "', but no exception was thrown");
		} catch (Exception e) {
			Assertions.assertThat(e.getMessage())
					.contains(error);
		}
	}

	public static void testCall(GraphDatabaseService db, String call, Map<String, Object> params,
			Consumer<Map<String, Object>> consumer, boolean onlyOne) {
		testResult(db, call, params, (res) -> {
			assertTrue(res.hasNext(), "Expect at least one result but got none: " + call);
			Map<String, Object> row = res.next();
			consumer.accept(row);
			if (onlyOne) {
				assertFalse(res.hasNext(), "Expected only one result, but there are more");
			}
		});
	}

	public static void testCallCount(GraphDatabaseService db, String call, Map<String, Object> params, int count) {
		testResult(db, call, params, (res) -> {
			int numLeft = count;
			while (numLeft > 0) {
				assertTrue(res.hasNext(),
						"Expected " + count + " results but found only " + (count - numLeft));
				res.next();
				numLeft--;
			}
			assertFalse(res.hasNext(), "Expected " + count + " results but there are more");
		});
	}

	public static void testResult(GraphDatabaseService db, String call, Consumer<Result> resultConsumer) {
		testResult(db, call, null, resultConsumer);
	}

	private static Layer makeLayerOfVariousTypes(SpatialDatabaseService spatial, Transaction tx, String name,
			int index) {
		return switch (index % 3) {
			case 0 -> spatial.getOrCreateSimplePointLayer(tx, name, SpatialDatabaseService.INDEX_TYPE_RTREE, "x", "y",
					null, false);
			case 1 -> spatial.getOrCreateNativePointLayer(tx, name, SpatialDatabaseService.INDEX_TYPE_RTREE, "location",
					null, false);
			default -> spatial.getOrCreateDefaultLayer(tx, name, null, false);
		};
	}

	private void makeOldSpatialModel(Transaction tx, String... layers) {
		KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) db, ktx.securityContext()));
		ArrayList<Node> layerNodes = new ArrayList<>();
		int index = 0;
		// First create a set of layers
		for (String name : layers) {
			Layer layer = makeLayerOfVariousTypes(spatial, tx, name, index);
			layerNodes.add(layer.getLayerNode(tx));
			index++;
		}
		// Then downgrade to old format, without label and with reference node and relationships
		Node root = ReferenceNodes.createDeprecatedReferenceNode(tx, "spatial_root");
		for (Node node : layerNodes) {
			node.removeLabel(LABEL_LAYER);
			root.createRelationshipTo(node, SpatialRelationshipTypes.LAYER);
		}
	}

	@Test
	public void old_spatial_model_throws_errors() {
		try (Transaction tx = db.beginTx()) {
			makeOldSpatialModel(tx, "layer1", "layer2", "layer3");
			tx.commit();
		}
		testCallFails(db, "CALL spatial.layers", null,
				"Old reference node exists - please upgrade the spatial database to the new format");
	}

	@Test
	public void old_spatial_model_can_be_upgraded() {
		try (Transaction tx = db.beginTx()) {
			makeOldSpatialModel(tx, "layer1", "layer2", "layer3");
			tx.commit();
		}
		testCallFails(db, "CALL spatial.layers", null,
				"Old reference node exists - please upgrade the spatial database to the new format");
		testCallCount(db, "CALL spatial.upgrade", null, 3);
		testCallCount(db, "CALL spatial.layers", null, 3);
	}

	@Test
	public void add_node_to_non_existing_layer() {
		execute("CALL spatial.addPointLayer('some_name')");
		Node node = createNode("CREATE (n:Point {latitude:60.1,longitude:15.2}) RETURN n", "n");
		testCallFails(db, "CALL spatial.addNode.byId('wrong_name',$nodeId)", Map.of("nodeId", node.getElementId()),
				"No such layer 'wrong_name'");
	}

	@Test
	public void add_node_point_layer() {
		execute("CALL spatial.addPointLayer('points')");
		executeWrite("CREATE (n:Point {latitude:60.1,longitude:15.2})");
		Node node = createNode("MATCH (n:Point) WITH n CALL spatial.addNode('points',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.bbox('points',{longitude:15.0,latitude:60.0},{longitude:15.3, latitude:60.2})",
				r -> assertEquals(node, r.get("node")));
		testCall(db, "CALL spatial.withinDistance('points',{longitude:15.0,latitude:60.0},100)",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void add_node_and_search_bbox_and_distance() {
		execute("CALL spatial.addPointLayerXY('geom','lon','lat')");
		Node node = createNode(
				"CREATE (n:Node {lat:60.1,lon:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0},{lon:15.3, lat:60.2})",
				r -> assertEquals(node, r.get("node")));
		testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	// This tests issue https://github.com/neo4j-contrib/spatial/issues/298
	public void add_node_point_layer_and_search_multiple_points_precision() {
		execute("CALL spatial.addPointLayer('bar')");
		execute("create (n:Point) set n={latitude: 52.2029252, longitude: 0.0905302} with n call spatial.addNode('bar', n) yield node return node");
		execute("create (n:Point) set n={latitude: 52.202925, longitude: 0.090530} with n call spatial.addNode('bar', n) yield node return node");
//        long countLow = execute("call spatial.withinDistance('bar', {latitude:52.202925,longitude:0.0905302}, 100) YIELD node RETURN node");
//        assertThat("Expected two nodes when using low precision", countLow, equalTo(2L));
		long countHigh = execute(
				"call spatial.withinDistance('bar', {latitude:52.2029252,longitude:0.0905302}, 100) YIELD node RETURN node");
		MatcherAssert.assertThat("Expected two nodes when using high precision", countHigh, equalTo(2L));
	}

	@Test
	public void add_node_and_search_bbox_and_distance_geohash() {
		execute("CALL spatial.addPointLayerGeohash('geom')");
		Node node = createNode(
				"CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0},{lon:15.3, lat:60.2})",
				r -> assertEquals(node, r.get("node")));
		testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void add_node_and_search_bbox_and_distance_zorder() {
		execute("CALL spatial.addPointLayerZOrder('geom')");
		Node node = createNode(
				"CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0},{lon:15.3, lat:60.2})",
				r -> assertEquals(node, r.get("node")));
		testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void add_node_and_search_bbox_and_distance_hilbert() {
		execute("CALL spatial.addPointLayerHilbert('geom')");
		Node node = createNode(
				"CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0},{lon:15.3, lat:60.2})",
				r -> assertEquals(node, r.get("node")));
		testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	// This tests issue https://github.com/neo4j-contrib/spatial/issues/298
	public void add_node_point_layer_and_search_multiple_points_precision_geohash() {
		execute("CALL spatial.addPointLayerGeohash('bar')");
		execute("create (n:Point) set n={latitude: 52.2029252, longitude: 0.0905302} with n call spatial.addNode('bar', n) yield node return node");
		execute("create (n:Point) set n={latitude: 52.202925, longitude: 0.090530} with n call spatial.addNode('bar', n) yield node return node");
//        long countLow = execute("call spatial.withinDistance('bar', {latitude:52.202925,longitude:0.0905302}, 100) YIELD node RETURN node");
//        assertEquals("Expected two nodes when using low precision", countLow, equalTo(2L));
		long countHigh = execute(
				"call spatial.withinDistance('bar', {latitude:52.2029252,longitude:0.0905302}, 100) YIELD node RETURN node");
		assertEquals(2L, countHigh, "Expected two nodes when using high precision");
	}

	//
	// Testing interaction between Neo4j Spatial and the Neo4j 3.0 Point type (point() and distance() functions)
	//

	@Test
	public void create_point_and_distance() {
		double distance = (Double) executeObject(
				"WITH point({latitude: 5.0, longitude: 4.0}) as geometry RETURN point.distance(geometry, point({latitude: 5.0, longitude: 4.0})) as distance",
				"distance");
		Assertions.assertThat(distance).describedAs("distance").isEqualTo(0.0);
	}

	@Test
	public void create_point_and_return() {
		Object geometry = executeObject("RETURN point({latitude: 5.0, longitude: 4.0}) as geometry", "geometry");
		assertInstanceOf(Geometry.class, geometry, "Should be Geometry type");
	}

	@Test
	public void create_node_decode_to_geometry() {
		execute("CALL spatial.addWKTLayer('geom','geom')");
		Object geometry = executeObject(
				"CREATE (n:Node {geom:'POINT(4.0 5.0)'}) RETURN spatial.decodeGeometry('geom',n) AS geometry",
				"geometry");
		assertInstanceOf(Geometry.class, geometry, "Should be Geometry type");
	}

	@Test
	// TODO: Currently this only works for point geometries because Neo4k 3.4 can only return Point geometries from procedures
	public void create_node_and_convert_to_geometry() {
		execute("CALL spatial.addWKTLayer('geom','geom')");
		Geometry geom = (Geometry) executeObject(
				"CREATE (n:Node {geom:'POINT(4.0 5.0)'}) RETURN spatial.decodeGeometry('geom',n) AS geometry",
				"geometry");
		double distance = (Double) executeObject("RETURN point.distance($geom, point({y: 6.0, x: 4.0})) as distance",
				Map.of("geom", geom), "distance");
		MatcherAssert.assertThat("Expected the cartesian distance of 1.0", distance, closeTo(1.0, 0.00001));
	}

	@Test
	public void testAddNativePointLayerWithConfig() {
		testCall(db, "CALL spatial.addNativePointLayerWithConfig('geom','pos:mbr','hilbert')", (result) -> {
			Node node = (Node) result.get("node");
			assertEquals("geom", node.getProperty(PROP_LAYER));
			assertEquals("NativePointEncoder", node.getProperty(PROP_GEOMENCODER));
			assertEquals("hilbert",
					node.getProperty(PROP_INDEX_TYPE));
			assertEquals("pos:mbr", node.getProperty(PROP_GEOMENCODER_CONFIG));
		});
	}

	@Test
	public void testAddNativePointLayerXY() {
		testCall(db, "CALL spatial.addNativePointLayerXY('geom','x','y')", (result) -> {
			var o = result.get("node");
			Assertions.assertThat(o).isInstanceOf(Node.class);
			Node node = (Node) o;
			assertEquals("geom", node.getProperty(PROP_LAYER));
			assertEquals("SimplePointEncoder", node.getProperty(PROP_GEOMENCODER));
			assertEquals("rtree", node.getProperty(PROP_INDEX_TYPE));
			assertEquals("x:y", node.getProperty(PROP_GEOMENCODER_CONFIG));

		});

		execute(
				"CREATE (n:Node {id: 42, x: 5.0, y: 4.0}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node");

		testCall(db, "CALL spatial.withinDistance('geom',point({latitude:4.1,longitude:5.1}),100)", (result) -> {
			var o = result.get("node");
			Assertions.assertThat(o).isInstanceOf(Node.class);
			assertEquals(42L, ((Node) o).getProperty("id"));
		});
	}

	@Test
	public void create_a_pointlayer_with_x_and_y() {
		testCall(db, "CALL spatial.addPointLayerXY('geom','lon','lat')",
				(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
	}

	@Test
	public void create_a_pointlayer_with_config() {
		testCall(db, "CALL spatial.addPointLayerWithConfig('geom','lon:lat')",
				(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
	}

	@Test
	public void create_a_pointlayer_with_config_on_existing_wkt_layer() {
		execute("CALL spatial.addWKTLayer('geom','wkt')");
		try {
			testCall(db, "CALL spatial.addPointLayerWithConfig('geom','lon:lat')",
					(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
			fail("Expected exception to be thrown");
		} catch (Exception e) {
			Assertions.assertThat(e.getMessage()).contains("Layer already exists: 'geom'");
		}
	}

	@Test
	public void create_a_pointlayer_with_rtree() {
		testCall(db, "CALL spatial.addPointLayer('geom')",
				(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
	}

	@Test
	public void create_a_pointlayer_with_geohash() {
		testCall(db, "CALL spatial.addPointLayerGeohash('geom')",
				(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
	}

	@Test
	public void create_a_pointlayer_with_zorder() {
		testCall(db, "CALL spatial.addPointLayerZOrder('geom')",
				(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
	}

	@Test
	public void create_a_pointlayer_with_hilbert() {
		testCall(db, "CALL spatial.addPointLayerHilbert('geom')",
				(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
	}

	@Test
	public void create_and_delete_a_pointlayer_with_rtree() {
		testCall(db, "CALL spatial.addPointLayer('geom')",
				(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
		testCallCount(db, "CALL spatial.layers()", null, 1);
		execute("CALL spatial.removeLayer('geom')");
		testCallCount(db, "CALL spatial.layers()", null, 0);
	}

	@Test
	public void create_and_delete_a_pointlayer_with_geohash() {
		testCall(db, "CALL spatial.addPointLayerGeohash('geom')",
				(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
		testCallCount(db, "CALL spatial.layers()", null, 1);
		execute("CALL spatial.removeLayer('geom')");
		testCallCount(db, "CALL spatial.layers()", null, 0);
	}

	@Test
	public void create_and_delete_a_pointlayer_with_zorder() {
		testCall(db, "CALL spatial.addPointLayerZOrder('geom')",
				(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
		testCallCount(db, "CALL spatial.layers()", null, 1);
		execute("CALL spatial.removeLayer('geom')");
		testCallCount(db, "CALL spatial.layers()", null, 0);
	}

	@Test
	public void create_and_delete_a_pointlayer_with_hilbert() {
		testCall(db, "CALL spatial.addPointLayerHilbert('geom')",
				(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
		testCallCount(db, "CALL spatial.layers()", null, 1);
		execute("CALL spatial.removeLayer('geom')");
		testCallCount(db, "CALL spatial.layers()", null, 0);
	}

	@Test
	public void create_a_simple_pointlayer_using_named_encoder() {
		testCall(db, "CALL spatial.addLayerWithEncoder('geom','SimplePointEncoder','')", (r) -> {
			Node node = (Node) r.get("node");
			assertEquals("geom", node.getProperty(PROP_LAYER));
			assertEquals("SimplePointEncoder",
					node.getProperty(PROP_GEOMENCODER));
			assertEquals("SimplePointLayer", node.getProperty(PROP_LAYER_TYPE));
			assertFalse(node.hasProperty(PROP_GEOMENCODER_CONFIG));
		});
	}

	@Test
	public void create_a_simple_pointlayer_using_named_and_configured_encoder() {
		testCall(db, "CALL spatial.addLayerWithEncoder('geom','SimplePointEncoder','x:y:mbr')", (r) -> {
			Node node = (Node) r.get("node");
			assertEquals("geom", node.getProperty(PROP_LAYER));
			assertEquals("SimplePointEncoder",
					node.getProperty(PROP_GEOMENCODER));
			assertEquals("SimplePointLayer", node.getProperty(PROP_LAYER_TYPE));
			assertEquals("x:y:mbr", node.getProperty(PROP_GEOMENCODER_CONFIG));
		});
	}

	@Test
	public void create_a_native_pointlayer_using_named_encoder() {
		testCall(db, "CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','')", (r) -> {
			Node node = (Node) r.get("node");
			assertEquals("geom", node.getProperty(PROP_LAYER));
			assertEquals("NativePointEncoder",
					node.getProperty(PROP_GEOMENCODER));
			assertEquals("SimplePointLayer", node.getProperty(PROP_LAYER_TYPE));
			assertFalse(node.hasProperty(PROP_GEOMENCODER_CONFIG));
		});
	}

	@Test
	public void create_a_native_pointlayer_using_named_and_configured_encoder() {
		testCall(db, "CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','pos:mbr')", (r) -> {
			Node node = (Node) r.get("node");
			assertEquals("geom", node.getProperty(PROP_LAYER));
			assertEquals("NativePointEncoder",
					node.getProperty(PROP_GEOMENCODER));
			assertEquals("SimplePointLayer", node.getProperty(PROP_LAYER_TYPE));
			assertEquals("pos:mbr", node.getProperty(PROP_GEOMENCODER_CONFIG));
		});
	}

	@Test
	public void create_a_native_pointlayer_using_named_and_configured_encoder_with_cartesian() {
		testCall(db, "CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','pos:mbr:Cartesian')", (r) -> {
			Node node = (Node) r.get("node");
			assertEquals("geom", node.getProperty(PROP_LAYER));
			assertEquals("NativePointEncoder",
					node.getProperty(PROP_GEOMENCODER));
			assertEquals("SimplePointLayer", node.getProperty(PROP_LAYER_TYPE));
			assertEquals("pos:mbr:Cartesian", node.getProperty(PROP_GEOMENCODER_CONFIG));
		});
	}

	@Test
	public void create_a_native_pointlayer_using_named_and_configured_encoder_with_geographic() {
		testCall(db, "CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','pos:mbr:WGS-84')", (r) -> {
			Node node = (Node) r.get("node");
			assertEquals("geom", node.getProperty(PROP_LAYER));
			assertEquals("NativePointEncoder", node.getProperty(PROP_GEOMENCODER));
			assertEquals("SimplePointLayer", node.getProperty(PROP_LAYER_TYPE));
			assertEquals("pos:mbr:WGS-84", node.getProperty(PROP_GEOMENCODER_CONFIG));
		});
	}

	@Test
	public void create_a_wkt_layer_using_know_format() {
		testCall(db, "CALL spatial.addLayer('geom','WKT',null)",
				(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
	}

	@Test
	public void list_layer_names() {
		String wkt = "LINESTRING (15.2 60.1, 15.3 60.1)";
		execute("CALL spatial.addWKTLayer('geom','wkt')");
		execute("CALL spatial.addWKT('geom',$wkt)", Map.of("wkt", wkt));

		testCall(db, "CALL spatial.layers()", (r) -> {
			assertEquals("geom", r.get("name"));
			assertEquals("EditableLayer(name='geom', encoder=WKTGeometryEncoder(geom='wkt', bbox='bbox'))",
					r.get("signature"));
		});
	}

	@Test
	public void add_and_remove_layer() {
		execute("CALL spatial.addWKTLayer('geom','wkt')");
		testCallCount(db, "CALL spatial.layers()", null, 1);
		execute("CALL spatial.removeLayer('geom')");
		testCallCount(db, "CALL spatial.layers()", null, 0);
	}

	@Test
	public void add_and_remove_multiple_layers() {
		int NUM_LAYERS = 100;
		String wkt = "LINESTRING (15.2 60.1, 15.3 60.1)";
		for (int i = 0; i < NUM_LAYERS; i++) {
			String name = "wktLayer_" + i;
			testCallCount(db, "CALL spatial.layers()", null, i);
			execute("CALL spatial.addWKTLayer($layerName,'wkt')", Map.of("layerName", name));
			execute("CALL spatial.addWKT($layerName,$wkt)", Map.of("wkt", wkt, "layerName", name));
			testCallCount(db, "CALL spatial.layers()", null, i + 1);
		}
		for (int i = 0; i < NUM_LAYERS; i++) {
			String name = "wktLayer_" + i;
			testCallCount(db, "CALL spatial.layers()", null, NUM_LAYERS - i);
			execute("CALL spatial.removeLayer($layerName)", Map.of("layerName", name));
			testCallCount(db, "CALL spatial.layers()", null, NUM_LAYERS - i - 1);
		}
		testCallCount(db, "CALL spatial.layers()", null, 0);
	}

	@Test
	public void get_and_set_feature_attributes() {
		execute("CALL spatial.addWKTLayer('geom','wkt')");
		testCallCount(db, "CALL spatial.layers()", null, 1);
		testCallCount(db, "CALL spatial.getFeatureAttributes('geom')", null, 0);
		execute("CALL spatial.setFeatureAttributes('geom',['name','type','color'])");
		testCallCount(db, "CALL spatial.getFeatureAttributes('geom')", null, 3);
	}

	@Test
	public void list_spatial_procedures() {
		testResult(db, "CALL spatial.procedures()", (res) -> {
			Map<String, String> procs = new LinkedHashMap<>();
			while (res.hasNext()) {
				Map<String, Object> r = res.next();
				procs.put(r.get("name").toString(), r.get("signature").toString());
			}
			for (String key : procs.keySet()) {
				LOGGER.fine(key + ": " + procs.get(key));
			}
			assertEquals("spatial.procedures() :: (name :: STRING, signature :: STRING)",
					procs.get("spatial.procedures"));
			assertEquals("spatial.layers() :: (name :: STRING, signature :: STRING)",
					procs.get("spatial.layers"));
			assertEquals("spatial.layer(name :: STRING) :: (node :: NODE)", procs.get("spatial.layer"));
			assertEquals(
					"spatial.addLayer(name :: STRING, type :: STRING, encoderConfig :: STRING, indexConfig =  :: STRING) :: (node :: NODE)",
					procs.get("spatial.addLayer"));
			assertEquals("spatial.addNode(layerName :: STRING, node :: NODE) :: (node :: NODE)",
					procs.get("spatial.addNode"));
			assertEquals("spatial.addWKT(layerName :: STRING, geometry :: STRING) :: (node :: NODE)",
					procs.get("spatial.addWKT"));
			assertEquals("spatial.intersects(layerName :: STRING, geometry :: ANY) :: (node :: NODE)",
					procs.get("spatial.intersects"));
		});
	}

	@Test
	public void list_layer_types() {
		testResult(db, "CALL spatial.layerTypes()", (res) -> {
			// Convert the result list to a Map keyed by 'id' for an easy lookup
			Map<String, Map<String, Object>> layerTypes = res.stream()
					.collect(Collectors.toMap(r -> r.get("id").toString(), r -> r));
			Assertions.assertThat(layerTypes).hasSize(12);

			assertLayerType(layerTypes, "SimplePoint", "SimplePointEncoder", "SimplePointLayer", "rtree",
					"longitude:latitude");
			assertLayerType(layerTypes, "NativePoint", "NativePointEncoder", "SimplePointLayer", "rtree",
					"location");
			assertLayerType(layerTypes, "NativePoints", "NativePointsEncoder", "EditableLayer", "rtree",
					"geometry");
			assertLayerType(layerTypes, "WKT", "WKTGeometryEncoder", "EditableLayer", "rtree", "geometry");
			assertLayerType(layerTypes, "WKB", "WKBGeometryEncoder", "EditableLayer", "rtree", "geometry");
			assertLayerType(layerTypes, "Geohash", "SimplePointEncoder", "SimplePointLayer", "geohash",
					"longitude:latitude");
			assertLayerType(layerTypes, "ZOrder", "SimplePointEncoder", "SimplePointLayer", "zorder",
					"longitude:latitude");
			assertLayerType(layerTypes, "Hilbert", "SimplePointEncoder", "SimplePointLayer", "hilbert",
					"longitude:latitude");
			assertLayerType(layerTypes, "NativeGeohash", "NativePointEncoder", "SimplePointLayer", "geohash",
					"location");
			assertLayerType(layerTypes, "NativeZOrder", "NativePointEncoder", "SimplePointLayer", "zorder",
					"location");
			assertLayerType(layerTypes, "NativeHilbert", "NativePointEncoder", "SimplePointLayer", "hilbert",
					"location");
			assertLayerType(layerTypes, "OSM", "OSMGeometryEncoder", "OSMLayer", "rtree", "geometry");
		});
	}

	private void assertLayerType(Map<String, Map<String, Object>> layerTypes, String id, String encoder, String layer,
			String index, String defaultConfig) {
		Assertions.assertThat(layerTypes).containsKey(id);
		Assertions.assertThat(layerTypes.get(id))
				.containsEntry("id", id)
				.containsEntry("encoder", encoder)
				.containsEntry("layer", layer)
				.containsEntry("index", index)
				.containsEntry("crsName", "WGS84(DD)")
				.containsEntry("defaultEncoderConfig", defaultConfig);
	}

	@Test
	public void find_layer() {
		String wkt = "LINESTRING (15.2 60.1, 15.3 60.1)";
		execute("CALL spatial.addWKTLayer('geom','wkt')");
		execute("CALL spatial.addWKT('geom',$wkt)", Map.of("wkt", wkt));

		testCall(db, "CALL spatial.layer('geom')",
				(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
		testCallFails(db, "CALL spatial.layer('badname')", null, "No such layer 'badname'");
	}

	@Test
	public void add_a_node_to_the_spatial_rtree_index_for_simple_points() {
		execute("CALL spatial.addPointLayer('geom')");
		Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) RETURN n", "n");
		testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void add_a_node_to_the_spatial_geohash_index_for_simple_points() {
		execute("CALL spatial.addPointLayerGeohash('geom')");
		Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) RETURN n", "n");
		testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void add_a_node_to_the_spatial_zorder_index_for_simple_points() {
		execute("CALL spatial.addPointLayerZOrder('geom')");
		Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) RETURN n", "n");
		testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void add_a_node_to_the_spatial_hilbert_index_for_simple_points() {
		execute("CALL spatial.addPointLayerHilbert('geom')");
		Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) RETURN n", "n");
		testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				r -> assertEquals(node, r.get("node")));
	}


	static Stream<Arguments> provideEncodersAndIndexes() {
		return Stream.of("Simple", "Native")
				.flatMap(encoder -> Stream.of("Geohash", "ZOrder", "Hilbert", "RTree")
						.map(index -> arguments(encoder, index)));
	}

	@ParameterizedTest
	@MethodSource("provideEncodersAndIndexes")
	public void testPointLayer(String encoder, String indexType) {
		String procName = encoder.equals("Native") ? "spatial.addNativePointLayer" : "spatial.addPointLayer";
		if (!indexType.equals("RTree")) {
			procName += indexType;
		}
		String layerName = ("my-" + encoder + "-" + indexType + "-layer").toLowerCase();
		execute("CALL " + procName + "('" + layerName + "')");
		execute("CREATE (n:Node {id: 42, latitude:60.1,longitude:15.2}) SET n.location=point(n) RETURN n");

		testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('" + layerName + "',n) YIELD node RETURN node",
				(result) -> assertEquals(42L, ((Node) result.get("node")).getProperty("id")));

		testResult(db, "CALL spatial.layers()", (res) -> {
			while (res.hasNext()) {
				Map<String, Object> r = res.next();
				String expectedEncoder =
						r.get("name").toString().contains("native") ? "NativePointEncoder" : "SimplePointEncoder";
				MatcherAssert.assertThat("Expect simple:native encoders to appear in simple:native layers",
						r.get("signature").toString(), containsString(expectedEncoder));
			}
		});

		execute("CALL spatial.removeLayer('" + layerName + "')");
		testCallCount(db, "CALL spatial.layers()", null, 0);
	}

	@Test
	public void testDistanceNode() {
		execute("CALL spatial.addPointLayer('geom')");
		Node node = createNode(
				"CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void testDistanceNodeWithGeohashIndex() {
		execute("CALL spatial.addPointLayer('geom','geohash')");
		Node node = createNode(
				"CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void testDistanceNodeGeohash() {
		execute("CALL spatial.addPointLayerGeohash('geom')");
		Node node = createNode(
				"CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void testDistanceNodeZOrder() {
		execute("CALL spatial.addPointLayerZOrder('geom')");
		Node node = createNode(
				"CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void testDistanceNodeHilbert() {
		execute("CALL spatial.addPointLayerHilbert('geom')");
		Node node = createNode(
				"CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void add_a_node_to_the_spatial_index_short() {
		execute("CALL spatial.addPointLayerXY('geom','lon','lat')");
		Node node = createNode("CREATE (n:Node {lat:60.1,lon:15.2}) RETURN n", "n");
		testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void add_a_node_to_the_spatial_index_short_with_geohash() {
		execute("CALL spatial.addPointLayerXY('geom','lon','lat','geohash')");
		Node node = createNode("CREATE (n:Node {lat:60.1,lon:15.2}) RETURN n", "n");
		testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void add_two_nodes_to_the_spatial_layer() {
		execute("CALL spatial.addPointLayerXY('geom','lon','lat')");
		String node1;
		String node2;
		try (Transaction tx = db.beginTx()) {
			Result result = tx.execute(
					"CREATE (n1:Node {lat:60.1,lon:15.2}),(n2:Node {lat:60.1,lon:15.3}) WITH n1,n2 CALL spatial.addNodes('geom',[n1,n2]) YIELD count RETURN n1,n2,count");
			Map<String, Object> row = result.next();
			node1 = ((Node) row.get("n1")).getElementId();
			node2 = ((Node) row.get("n2")).getElementId();
			long count = (Long) row.get("count");
			assertEquals(2L, count);
			result.close();
			tx.commit();
		}
		testResult(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", res -> {
			assertTrue(res.hasNext());
			assertEquals(node1, ((Node) res.next().get("node")).getElementId());
			assertTrue(res.hasNext());
			assertEquals(node2, ((Node) res.next().get("node")).getElementId());
			assertFalse(res.hasNext());
		});
		try (Transaction tx = db.beginTx()) {
			Node node = (Node) tx.execute("MATCH (node) WHERE elementId(node) = $nodeId RETURN node",
					Map.of("nodeId", node1)).next().get("node");
			Result removeResult = tx.execute("CALL spatial.removeNode('geom',$node) YIELD nodeId RETURN nodeId",
					Map.of("node", node));
			assertEquals(node1, removeResult.next().get("nodeId"));
			removeResult.close();
			tx.commit();
		}
		testResult(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", res -> {
			assertTrue(res.hasNext());
			assertEquals(node2, ((Node) res.next().get("node")).getElementId());
			assertFalse(res.hasNext());
		});
		try (Transaction tx = db.beginTx()) {
			Result removeResult = tx.execute("CALL spatial.removeNode.byId('geom',$nodeId) YIELD nodeId RETURN nodeId",
					Map.of("nodeId", node2));
			assertEquals(node2, removeResult.next().get("nodeId"));
			removeResult.close();
			tx.commit();
		}
		testResult(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
				res -> assertFalse(res.hasNext()));
	}

	@Test
	public void add_many_nodes_to_the_simple_point_layer_using_addNodes() {
		// Playing with this number in both tests leads to rough benchmarking of the addNode/addNodes comparison
		int count = 1000;
		execute("CALL spatial.addLayer('simple_poi','SimplePoint','')");
		String query = """
				UNWIND range(1,$count) as i
				CREATE (n:Point {id:i, latitude:(56.0+toFloat(i)/100.0),longitude:(12.0+toFloat(i)/100.0)})
				WITH collect(n) as points
				CALL spatial.addNodes('simple_poi',points) YIELD count
				RETURN count""";
		testCountQuery("addNodes", query, count, "count", Map.of("count", count));
		testRemoveNodes("simple_poi", count);
	}

	@Test
	public void add_many_nodes_to_the_simple_point_layer_using_addNode() {
		// Playing with this number in both tests leads to rough benchmarking of the addNode/addNodes comparison
		int count = 1000;
		execute("CALL spatial.addLayer('simple_poi','SimplePoint','')");
		String query = """
				UNWIND range(1,$count) as i
				CREATE (n:Point {id:i, latitude:(56.0+toFloat(i)/100.0),longitude:(12.0+toFloat(i)/100.0)})
				WITH n
				CALL spatial.addNode('simple_poi',n) YIELD node
				RETURN count(node)""";
		testCountQuery("addNode", query, count, "count(node)", Map.of("count", count));
		testRemoveNode("simple_poi", count);
	}

	@Test
	public void add_many_nodes_to_the_native_point_layer_using_addNodes() {
		// Playing with this number in both tests leads to rough benchmarking of the addNode/addNodes comparison
		int count = 1000;
		execute("CALL spatial.addLayer('native_poi','NativePoint','')");
		String query = """
				UNWIND range(1,$count) as i
				WITH i, Point({latitude:(56.0+toFloat(i)/100.0),longitude:(12.0+toFloat(i)/100.0)}) AS location
				CREATE (n:Point {id: i, location:location})
				WITH collect(n) as points
				CALL spatial.addNodes('native_poi',points) YIELD count
				RETURN count""";
		testCountQuery("addNodes", query, count, "count", Map.of("count", count));
		testRemoveNodes("native_poi", count);
	}

	@Test
	public void add_many_nodes_to_the_native_point_layer_using_addNode() {
		// Playing with this number in both tests leads to rough benchmarking of the addNode/addNodes comparison
		int count = 1000;
		execute("CALL spatial.addLayer('native_poi','NativePoint','')");
		String query = """
				UNWIND range(1,$count) as i
				WITH i, Point({latitude:(56.0+toFloat(i)/100.0),longitude:(12.0+toFloat(i)/100.0)}) AS location
				CREATE (n:Point {id: i, location:location})
				WITH n
				CALL spatial.addNode('native_poi',n) YIELD node
				RETURN count(node)""";
		testCountQuery("addNode", query, count, "count(node)", Map.of("count", count));
		testRemoveNode("native_poi", count);
	}

	private void testRemoveNode(String layer, int count) {
		// Check all nodes are there
		testCountQuery("withinDistance",
				"CALL spatial.withinDistance('" + layer + "',{lon:15.0,lat:60.0},1000) YIELD node RETURN count(node)",
				count, "count(node)", null);
		// Now remove half the points
		String remove = "UNWIND range(1,$count) as i\n" +
				"MATCH (n:Point {id:i})\n" +
				"WITH n\n" +
				"CALL spatial.removeNode('" + layer + "',n) YIELD nodeId\n" +
				"RETURN count(nodeId)";
		testCountQuery("removeNode", remove, count / 2, "count(nodeId)", Map.of("count", count / 2));
		// Check that only half remain
		testCountQuery("withinDistance",
				"CALL spatial.withinDistance('" + layer + "',{lon:15.0,lat:60.0},1000) YIELD node RETURN count(node)",
				count / 2, "count(node)", null);
	}

	private void testRemoveNodes(String layer, int count) {
		// Check all nodes are there
		testCountQuery("withinDistance",
				"CALL spatial.withinDistance('" + layer + "',{lon:15.0,lat:60.0},1000) YIELD node RETURN count(node)",
				count, "count(node)", null);
		// Now remove half the points
		String remove = "UNWIND range(1,$count) as i\n" +
				"MATCH (n:Point {id:i})\n" +
				"WITH collect(n) as points\n" +
				"CALL spatial.removeNodes('" + layer + "',points) YIELD count\n" +
				"RETURN count";
		testCountQuery("removeNodes", remove, count / 2, "count", Map.of("count", count / 2));
		// Check that only half remain
		testCountQuery("withinDistance",
				"CALL spatial.withinDistance('" + layer + "',{lon:15.0,lat:60.0},1000) YIELD node RETURN count(node)",
				count / 2, "count(node)", null);
	}

	@Test
	public void import_shapefile() {
		testCountQuery("importShapefile", "CALL spatial.importShapefile('../example-data/shp/highway.shp')", 143,
				"count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}

	@Test
	public void import_shapefile_without_extension() {
		testCountQuery("importShapefile", "CALL spatial.importShapefile('../example-data/shp/highway')", 143, "count",
				null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}

	@Test
	public void import_shapefile_to_layer() {
		execute("CALL spatial.addWKTLayer('geom','wkt')");
		testCountQuery("importShapefileToLayer",
				"CALL spatial.importShapefileToLayer('geom','../example-data/shp/highway.shp')", 143,
				"count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}


	@Test
	public void import_osm() {
		testCountQuery("importOSM", "CALL spatial.importOSM('map.osm')", 55, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}

	@Test
	public void import_osm_twice_should_fail() {
		testCountQuery("importOSM", "CALL spatial.importOSM('map.osm')", 55, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
		testCallFails(db, "CALL spatial.importOSM('map.osm')", null, "Layer already exists: 'map.osm'");
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}

	@Test
	public void import_osm_without_extension() {
		testCountQuery("importOSM", "CALL spatial.importOSM('map')", 55, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}

	@Test
	public void import_osm_to_layer() {
		execute("CALL spatial.addLayer('geom','OSM','')");
		testCountQuery("importOSMToLayer", "CALL spatial.importOSMToLayer('geom','map.osm')", 55, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}

	@Test
	public void import_osm_twice_should_pass_with_different_layers() {
		execute("CALL spatial.addLayer('geom1','OSM','')");
		execute("CALL spatial.addLayer('geom2','OSM','')");

		testCountQuery("importOSM", "CALL spatial.importOSMToLayer('geom1','map.osm')", 55, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 2);
		testCallCount(db, "CALL spatial.withinDistance('geom1',{lon:6.3740429666,lat:50.93676351666},10000)", null,
				217);
		testCallCount(db, "CALL spatial.withinDistance('geom2',{lon:6.3740429666,lat:50.93676351666},10000)", null, 0);

		testCountQuery("importOSM", "CALL spatial.importOSMToLayer('geom2','map.osm')", 55, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 2);
		testCallCount(db, "CALL spatial.withinDistance('geom1',{lon:6.3740429666,lat:50.93676351666},10000)", null,
				217);
		testCallCount(db, "CALL spatial.withinDistance('geom2',{lon:6.3740429666,lat:50.93676351666},10000)", null,
				217);
	}

	@Test
	public void import_osm_to_layer_without_changesets() {
		execute("CALL spatial.addLayer('osm_example','OSM','')");
		testCountQuery("importOSMToLayerWithoutChangesets", "CALL spatial.importOSMToLayer('osm_example','sample.osm')",
				1, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}

	@Test
	public void import_osm_and_add_geometry() {
		execute("CALL spatial.addLayer('geom','OSM','')");
		testCountQuery("importOSMToLayerAndAddGeometry", "CALL spatial.importOSMToLayer('geom','map.osm')", 55, "count",
				null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},100)", null, 0);
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},10000)", null, 217);

		// Adding a point to the layer
		Node node = createNode(
				"CALL spatial.addWKT('geom', 'POINT(6.3740429666 50.93676351666)') YIELD node RETURN node", "node");
		testCall(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},100)",
				r -> assertEquals(node, r.get("node")));
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},100)", null, 1);
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},10000)", null, 218);
	}

	@Test
	public void import_osm_and_polygons_withinDistance() {
		Map<String, Object> params = Map.of("osmFile", "withinDistance.osm", "busShelterID", 2938842290L);
		execute("CALL spatial.addLayer('geom','OSM','')");
		testCountQuery("importOSMAndPolygonsWithinDistance", "CALL spatial.importOSMToLayer('geom',$osmFile)", 74,
				"count", params);
		testCallCount(db, "CALL spatial.layers()", null, 1);
		testCallCount(db,
				"MATCH (n) WHERE n.node_osm_id = $busShelterID CALL spatial.withinDistance('geom',n,100) YIELD node, distance RETURN node, distance",
				params, 516);
		testResult(db,
				"MATCH (n) WHERE n.node_osm_id = $busShelterID CALL spatial.withinDistance('geom',n,100) YIELD node, distance WITH node, distance ORDER BY distance LIMIT 20 MATCH (node)<-[:GEOM]-(osmNode) RETURN node, distance, osmNode, properties(osmNode) as props",
				params, res -> {
					while (res.hasNext()) {
						Map<String, Object> r = res.next();
						assertThat("Result should have 'node'", r, hasKey("node"));
						assertThat("Result should have 'distance'", r, hasKey("distance"));
						assertThat("Result should have 'osmNode'", r, hasKey("osmNode"));
						assertThat("Result should have 'props'", r, hasKey("props"));
						Node node = (Node) r.get("node");
						double distance = (Double) r.get("distance");
						Node osmNode = (Node) r.get("osmNode");
						@SuppressWarnings({"rawtypes", "unchecked"}) Map<String, Object> props = (Map) r.get("props");
						LOGGER.fine(
								"(node[" + node.getElementId() + "])<-[:GEOM {distance:" + distance + "}]-(osmNode["
										+ osmNode.getElementId() + "] " + props + ") ");
						assertThat("Node should have either way_osm_id or node_osm_id", props,
								anyOf(hasKey("node_osm_id"), hasKey("way_osm_id")));
					}
				});
		testResult(db,
				"MATCH (n) WHERE n.node_osm_id = $busShelterID CALL spatial.withinDistance('geom',n,100) YIELD node, distance WITH node, distance ORDER BY distance LIMIT 20 MATCH (n) WHERE elementId(n)=elementId(node) RETURN node, distance, spatial.decodeGeometry('geom',n) AS geometry",
				params, res -> {
					while (res.hasNext()) {
						Map<String, Object> r = res.next();
						assertThat("Result should have 'node'", r, hasKey("node"));
						assertThat("Result should have 'distance'", r, hasKey("distance"));
						assertThat("Result should have 'geometry'", r, hasKey("geometry"));
						Node node = (Node) r.get("node");
						double distance = (Double) r.get("distance");
						Object geometry = r.get("geometry");
						LOGGER.fine(node.toString() + " at " + distance + ": " + geometry);
						if (geometry instanceof Point) {
							assertThat("Point has 2D coordinates",
									((Point) geometry).getCoordinate().getCoordinate().length, equalTo(2));
						} else if (geometry instanceof Map) {
							@SuppressWarnings("unchecked") Map<String, Object> map = (Map<String, Object>) geometry;
							assertThat("Geometry should contain a type", map, hasKey("type"));
							assertThat("Geometry should contain coordinates", map, hasKey("coordinates"));
							assertThat("Geometry should not be a point", map.get("type"), not(equalTo("Point")));
						} else {
							fail("Geometry should be either a point or a Map containing coordinates");
						}
					}
				});
	}

	@Test
	public void find_geometries_in_a_bounding_box_short() {
		execute("CALL spatial.addPointLayerXY('geom','lon','lat')");
		Node node = createNode(
				"CREATE (n:Node {lat:60.1,lon:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void find_geometries_in_a_bounding_box() {
		execute("CALL spatial.addPointLayer('geom')");
		Node node = createNode(
				"CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void find_geometries_in_a_polygon() {
		execute("CALL spatial.addPointLayer('geom')");
		executeWrite(
				"UNWIND [{name:'a',latitude:60.1,longitude:15.2},{name:'b',latitude:60.3,longitude:15.5}] as point CREATE (n:Node) SET n += point WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node.name as name");
		String polygon = "POLYGON((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2))";
		testCall(db, "CALL spatial.intersects('geom','" + polygon + "') YIELD node RETURN node.name as name",
				r -> assertEquals("b", r.get("name")));
	}

	@Test
	public void find_geometries_in_a_bounding_box_geohash() {
		execute("CALL spatial.addPointLayerGeohash('geom')");
		Node node = createNode(
				"CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void find_geometries_in_a_polygon_geohash() {
		execute("CALL spatial.addPointLayerGeohash('geom')");
		executeWrite(
				"UNWIND [{name:'a',latitude:60.1,longitude:15.2},{name:'b',latitude:60.3,longitude:15.5}] as point CREATE (n:Node) SET n += point WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node.name as name");
		String polygon = "POLYGON((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2))";
		testCall(db, "CALL spatial.intersects('geom','" + polygon + "') YIELD node RETURN node.name as name",
				r -> assertEquals("b", r.get("name")));
	}

	@Test
	public void find_geometries_in_a_bounding_box_zorder() {
		execute("CALL spatial.addPointLayerZOrder('geom')");
		Node node = createNode(
				"CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void find_geometries_in_a_polygon_zorder() {
		execute("CALL spatial.addPointLayerZOrder('geom')");
		executeWrite(
				"UNWIND [{name:'a',latitude:60.1,longitude:15.2},{name:'b',latitude:60.3,longitude:15.5}] as point CREATE (n:Node) SET n += point WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node.name as name");
		String polygon = "POLYGON((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2))";
		testCall(db, "CALL spatial.intersects('geom','" + polygon + "') YIELD node RETURN node.name as name",
				r -> assertEquals("b", r.get("name")));
	}

	@Test
	public void find_geometries_in_a_bounding_box_hilbert() {
		execute("CALL spatial.addPointLayerHilbert('geom')");
		Node node = createNode(
				"CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})",
				r -> assertEquals(node, r.get("node")));
	}

	@Test
	public void find_geometries_in_a_polygon_hilbert() {
		execute("CALL spatial.addPointLayerHilbert('geom')");
		executeWrite(
				"UNWIND [{name:'a',latitude:60.1,longitude:15.2},{name:'b',latitude:60.3,longitude:15.5}] as point CREATE (n:Node) SET n += point WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node.name as name");
		String polygon = "POLYGON((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2))";
		testCall(db, "CALL spatial.intersects('geom','" + polygon + "') YIELD node RETURN node.name as name",
				r -> assertEquals("b", r.get("name")));
	}

	@Test
	public void create_a_WKT_layer() {
		testCall(db, "CALL spatial.addWKTLayer('geom','wkt')",
				r -> assertEquals("wkt", ((Node) r.get("node")).getProperty(PROP_GEOMENCODER_CONFIG)));
	}

	@Test
	public void add_a_WKT_geometry_to_a_layer() {
		String lineString = "LINESTRING (15.2 60.1, 15.3 60.1)";

		execute("CALL spatial.addWKTLayer('geom','wkt')");
		testCall(db, "CALL spatial.addWKT('geom',$wkt)", Map.of("wkt", lineString),
				r -> assertEquals(lineString, ((Node) r.get("node")).getProperty("wkt")));
	}

	@Test
	public void find_geometries_close_to_a_point_wkt() {
		String lineString = "LINESTRING (15.2 60.1, 15.3 60.1)";
		execute("CALL spatial.addLayer('geom','WKT','wkt')");
		execute("CALL spatial.addWKT('geom',$wkt)", Map.of("wkt", lineString));
		testCall(db, "CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)",
				r -> assertEquals(lineString, ((Node) r.get("node")).getProperty("wkt")));
	}

	@Test
	public void find_geometries_close_to_a_point_geohash() {
		String lineString = "POINT (15.2 60.1)";
		execute("CALL spatial.addLayer('geom','geohash','lon:lat')");
		execute("CALL spatial.addWKT('geom',$wkt)", Map.of("wkt", lineString));
		testCallCount(db, "CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)", null, 1);
	}

	@Test
	public void find_geometries_close_to_a_point_zorder() {
		String lineString = "POINT (15.2 60.1)";
		execute("CALL spatial.addLayer('geom','zorder','lon:lat')");
		execute("CALL spatial.addWKT('geom',$wkt)", Map.of("wkt", lineString));
		testCallCount(db, "CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)", null, 1);
	}

	@Test
	public void find_geometries_close_to_a_point_hilbert() {
		String lineString = "POINT (15.2 60.1)";
		execute("CALL spatial.addLayer('geom','hilbert','lon:lat')");
		execute("CALL spatial.addWKT('geom',$wkt)", Map.of("wkt", lineString));
		testCallCount(db, "CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)", null, 1);
	}

	@Test
	public void find_no_geometries_using_closest_on_empty_layer() {
		execute("CALL spatial.addLayer('geom','WKT','wkt')");
		testCallCount(db, "CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)", null, 0);
	}

	@Test
	public void testNativePoints() {
		execute("CREATE (node:Foo { points: [point({latitude: 5.0, longitude: 4.0}), point({latitude: 6.0, longitude: 5.0})]})");
		execute("CALL spatial.addLayer('line','NativePoints','points') YIELD node" +
				" MATCH (n:Foo)" +
				" WITH collect(n) AS nodes" +
				" CALL spatial.addNodes('line', nodes) YIELD count RETURN count");
		testCallCount(db, "CALL spatial.closest('line',{lon:5.1, lat:4.1}, 1.0)", null, 1);
	}

	@Test
	public void testNativePoints3D() {
		execute("CREATE (node:Foo { points: [point({latitude: 5.0, longitude: 4.0, height: 1.0}), point({latitude: 6.0, longitude: 5.0, height: 2.0})]})");
		Exception exception = assertThrows(QueryExecutionException.class,
				() -> execute("CALL spatial.addLayer('line','NativePoints','points:bbox:Cartesian') YIELD node" +
						" MATCH (n:Foo)" +
						" WITH collect(n) AS nodes" +
						" CALL spatial.addNodes('line', nodes) YIELD count RETURN count"));

		assertEquals(
				"Failed to invoke procedure `spatial.addNodes`: Caused by: java.lang.IllegalStateException: Trying to decode geometry with wrong CRS: layer configured to crs=7203, but geometry has crs=4979",
				exception.getMessage());
	}

	@Test
	public void testNativePointsCartesian() {
		execute("CREATE (node:Foo { points: [point({x: 5.0, y: 4.0}), point({x: 6.0, y: 5.0})]})");
		execute("CALL spatial.addLayer('line','NativePoints','points:bbox:Cartesian') YIELD node" +
				" MATCH (n:Foo)" +
				" WITH collect(n) AS nodes" +
				" CALL spatial.addNodes('line', nodes) YIELD count RETURN count");
		testCallCount(db, "CALL spatial.closest('line',point({x:5.1, y:4.1}), 1.0)", null, 1);
	}

	@Test
	public void testNativePointsCartesian3D() {
		execute("CREATE (node:Foo { points: [point({x: 5.0, y: 4.0, z: 1}), point({x: 6.0, y: 5.0, z: 2})]})");
		Exception exception = assertThrows(QueryExecutionException.class,
				() -> execute("CALL spatial.addLayer('line','NativePoints','points:bbox:Cartesian') YIELD node" +
						" MATCH (n:Foo)" +
						" WITH collect(n) AS nodes" +
						" CALL spatial.addNodes('line', nodes) YIELD count RETURN count"));

		assertEquals(
				"Failed to invoke procedure `spatial.addNodes`: Caused by: java.lang.IllegalStateException: Trying to decode geometry with wrong CRS: layer configured to crs=7203, but geometry has crs=9157",
				exception.getMessage());
	}

	@Test
	public void testCQLQuery() {
		execute("CALL spatial.addWKTLayer('geom','wkt') YIELD node");
		execute("""
						CREATE (n1:Node {wkt: 'POINT(15.2 60.1)', name: 'point1'})
						CREATE (n2:Node {wkt: 'POINT(25.2 30.1)', name: 'point2'})
						WITH n1, n2
						CALL spatial.addNode('geom', n1) YIELD node as added1
						WITH n2, added1
						CALL spatial.addNode('geom', n2) YIELD node as added2
						RETURN added1, added2
				""");
		Object name = executeObject(
				"CALL spatial.cql('geom', 'name = \\'point1\\'') YIELD node RETURN node.name as name", "name");
		assertEquals("point1", name);
	}

	@Test
	public void testGetFeatureCount() {
		execute("CALL spatial.addPointLayer('count_layer') YIELD node");
		var count = executeObject("CALL spatial.getFeatureCount('count_layer') YIELD count", "count");
		assertEquals(0L, count);
		execute("""
						CREATE (n1:Point {latitude: 60.1, longitude: 15.2, name: 'point1'})
						CREATE (n2:Point {latitude: 60.3, longitude: 15.5, name: 'point2'})
						WITH n1, n2
						CALL spatial.addNode('count_layer', n1) YIELD node as added1
						WITH n2, added1
						CALL spatial.addNode('count_layer', n2) YIELD node as added2
						RETURN added1, added2
				""");
		var count2 = executeObject("CALL spatial.getFeatureCount('count_layer') YIELD count", "count");
		assertEquals(2L, count2);
	}

	@Test
	public void testGetLayerBoundingBox() {
		execute("CALL spatial.addPointLayer('bbox_layer', 'rtree', 'wgs84') YIELD node");
		execute("""
						CREATE (n1:Point {latitude: 60.0, longitude: 15.0, name: 'southwest'})
						CREATE (n2:Point {latitude: 61.0, longitude: 16.0, name: 'northeast'})
						WITH n1, n2
						CALL spatial.addNode('bbox_layer', n1) YIELD node as added1
						WITH n2, added1
						CALL spatial.addNode('bbox_layer', n2) YIELD node as added2
						RETURN added1, added2
				""");
		testCall(db, "CALL spatial.getLayerBoundingBox('bbox_layer') YIELD minX, minY, maxX, maxY, crs", (result) -> {
			assertThat((Double) result.get("minX"), closeTo(15.0, 0.0001));
			assertThat((Double) result.get("minY"), closeTo(60.0, 0.0001));
			assertThat((Double) result.get("maxX"), closeTo(16.0, 0.0001));
			assertThat((Double) result.get("maxY"), closeTo(61.0, 0.0001));
			assertEquals("WGS84(DD)", result.get("crs"));
		});
	}

	@Test
	public void testLayerMeta() {
		execute("CALL spatial.addPointLayer('meta_layer', 'rtree', 'wgs84') YIELD node");
		execute("""
						CREATE (n1:Point {latitude: 60.0, longitude: 15.0, name: 'southwest'})
						WITH n1
						CALL spatial.addNode('meta_layer', n1) YIELD node
						RETURN node
				""");
		try (Transaction tx = db.beginTx()) {
			var result = tx.execute(
							"CALL spatial.layerMeta('meta_layer') YIELD name, geometryType, crs, hasComplexAttributes, extraAttributes")
					.next();
			assertEquals("meta_layer", result.get("name"));
			assertEquals("org.locationtech.jts.geom.Point", result.get("geometryType"));
			Assertions.assertThat((String) result.get("crs")).contains("WGS84(DD)");
			assertFalse((Boolean) result.get("hasComplexAttributes"));
		}
	}

	@Test
	public void testUpdateWKT() {
		execute("CALL spatial.addWKTLayer('update_layer', 'wkt') YIELD node");
		execute("""
						CREATE (n:Node {wkt: 'POINT(15.2 60.1)', name: 'updatable_point'})
						WITH n
						CALL spatial.addNode('update_layer', n) YIELD node as added_node
						RETURN n, added_node
				""");
		Object wkt = executeObject("""
								MATCH (n:Node {name: 'updatable_point'})
								CALL spatial.updateWKT('update_layer', n, 'POINT(25.5 65.5)') YIELD node
								RETURN node.wkt as wkt
				""", "wkt");
		assertEquals("POINT (25.5 65.5)", wkt);
		Object name = executeObject("""
								CALL spatial.withinDistance('update_layer', {longitude: 25.5, latitude: 65.5}, 1) YIELD node
								RETURN node.name as name
				""", "name");
		assertEquals("updatable_point", name);
	}

	@Test
	public void test_spatial_getFeatureCount() {
		execute("CALL spatial.addPointLayer('count_layer')");
		Object count = executeObject("CALL spatial.getFeatureCount('count_layer') YIELD count", "count");
		assertEquals(0L, count);

		execute("""
						CREATE (n:Node {latitude: 60.1, longitude: 15.2, name: 'first'})
						WITH n
						CALL spatial.addNode('count_layer', n) YIELD node
						RETURN node
				""");
		Object count2 = executeObject("CALL spatial.getFeatureCount('count_layer') YIELD count", "count");
		assertEquals(1L, count2);

		execute("""
						UNWIND range(1,3) as i
						CREATE (n:Node {id: i, latitude: (60.0 + i * 0.1), longitude: (15.0 + i * 0.1)})
						WITH collect(n) as nodes
						CALL spatial.addNodes('count_layer', nodes) YIELD count
						RETURN count
				""");
		Object count3 = executeObject("CALL spatial.getFeatureCount('count_layer') YIELD count", "count");
		assertEquals(4L, count3);

		execute("CALL spatial.addWKTLayer('wkt_layer', 'wkt')");
		Object count4 = executeObject("CALL spatial.getFeatureCount('wkt_layer') YIELD count", "count");
		assertEquals(0L, count4);

		execute("CALL spatial.addWKT('wkt_layer', 'POINT(15.2 60.1)') YIELD node RETURN node");
		execute("CALL spatial.addWKT('wkt_layer', 'LINESTRING (15.2 60.1, 15.3 60.1)') YIELD node RETURN node");
		Object count5 = executeObject("CALL spatial.getFeatureCount('wkt_layer') YIELD count", "count");
		assertEquals(2L, count5);

		// Test error for non-existent layer
		testCallFails(db, "CALL spatial.getFeatureCount('non_existent_layer')", null,
				"No such layer 'non_existent_layer'");
	}
}
