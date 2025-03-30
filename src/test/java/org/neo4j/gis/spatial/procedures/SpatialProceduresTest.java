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

import static org.assertj.core.api.Assertions.entry;
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
import static org.neo4j.gis.spatial.Constants.PROP_LAYER;
import static org.neo4j.gis.spatial.Constants.PROP_LAYER_CLASS;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.doc.domain.examples.Example;
import org.neo4j.doc.domain.examples.ExampleCypher;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.spatial.AbstractApiTest;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.SpatialRelationshipTypes;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.utilities.ReferenceNodes;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class SpatialProceduresTest extends AbstractApiTest {

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
			assertTrue(e.getMessage().contains(error));
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

	public static void testResult(GraphDatabaseService db, String call, Map<String, Object> params,
			Consumer<Result> resultConsumer) {
		try (Transaction tx = db.beginTx()) {
			Map<String, Object> p = (params == null) ? Map.of() : params;
			resultConsumer.accept(tx.execute(call, p));
			tx.commit();
		}
	}

	public static void registerProceduresAndFunctions(GraphDatabaseService db, Class<?> procedure)
			throws KernelException {
		GlobalProcedures procedures = ((GraphDatabaseAPI) db).getDependencyResolver()
				.resolveDependency(GlobalProcedures.class);
		procedures.registerProcedure(procedure);
		procedures.registerFunction(procedure);
	}

	private static Layer makeLayerOfVariousTypes(SpatialDatabaseService spatial, Transaction tx, String name,
			int index) {
		return switch (index % 3) {
			case 0 -> spatial.getOrCreateSimplePointLayer(tx, name, SpatialDatabaseService.INDEX_TYPE_RTREE, "x", "y",
					null);
			case 1 -> spatial.getOrCreateNativePointLayer(tx, name, SpatialDatabaseService.INDEX_TYPE_RTREE, "location",
					null);
			default -> spatial.getOrCreateDefaultLayer(tx, name, null);
		};
	}

	private void makeOldSpatialModel(Transaction tx, String... layers) {
		KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) db, ktx.securityContext()));
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
		System.out.println(distance);
	}

	@Test
	public void create_point_and_return() {
		Object geometry = executeObject("RETURN point({latitude: 5.0, longitude: 4.0}) as geometry", "geometry");
		assertInstanceOf(Geometry.class, geometry, "Should be Geometry type");
	}

	@Test
	public void create_node_decode_to_geometry() {
		docExample("spatial.decodeGeometry", "Decode a geometry from a node property")
				.additionalSignature("spatial.addWKTLayer")
				.runCypher("CALL spatial.addWKTLayer('geom','geom')",
						config -> config.storeResult().setTitle("Create a WKT layer"))
				.runCypher(
						"CREATE (n:Node {geom:'POINT(4.0 5.0)'}) RETURN spatial.decodeGeometry('geom',n) AS geometry",
						config -> config.storeResult().setTitle("Decode a geometry"))
				.assertSingleResult("geometry", geometry -> {
					assertInstanceOf(Geometry.class, geometry, "Should be Geometry type");
				});
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
		docExample("spatial.addNativePointLayerWithConfig", "Create a native point layer with a configuration")
				.runCypher("CALL spatial.addNativePointLayerWithConfig('geom','pos:mbr','hilbert')",
						ExampleCypher::storeResult)
				.assertSingleResult("node", o -> {
					Assertions.assertThat(o).isInstanceOf(Node.class);
					Node node = (Node) o;
					assertEquals("geom", node.getProperty("layer"));
					assertEquals("org.neo4j.gis.spatial.encoders.NativePointEncoder", node.getProperty("geomencoder"));
					assertEquals("org.neo4j.gis.spatial.index.LayerHilbertPointIndex",
							node.getProperty("index_class"));
					assertEquals("pos:mbr", node.getProperty("geomencoder_config"));
				});
	}

	@Test
	public void testAddNativePointLayerXY() {
		docExample("spatial.addNativePointLayerXY", "Create a native point layer")
				.additionalSignature("spatial.withinDistance")
				.additionalSignature("spatial.addNode")
				.runCypher("CALL spatial.addNativePointLayerXY('geom','x','y')",
						ExampleCypher::storeResult)
				.assertSingleResult("node", o -> {
					Assertions.assertThat(o).isInstanceOf(Node.class);
					Node node = (Node) o;
					assertEquals("geom", node.getProperty("layer"));
					assertEquals("org.neo4j.gis.spatial.encoders.SimplePointEncoder", node.getProperty("geomencoder"));
					assertEquals("org.neo4j.gis.spatial.index.LayerRTreeIndex", node.getProperty("index_class"));
					assertEquals("x:y", node.getProperty("geomencoder_config"));
				})
				.runCypher(
						"CREATE (n:Node {id: 42, x: 5.0, y: 4.0}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
						config -> config.setComment("create a node and add it to the index"))
				.runCypher("CALL spatial.withinDistance('geom',point({latitude:4.1,longitude:5.1}),100)",
						config -> config.storeResult().setComment("Find node within distance"))
				.assertSingleResult("node", o -> assertEquals(42L, ((Node) o).getProperty("id")));

	}

	@Test
	public void create_a_pointlayer_with_x_and_y() {
		testCall(db, "CALL spatial.addPointLayerXY('geom','lon','lat')",
				(r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
	}

	@Test
	public void create_a_pointlayer_with_config() {
		docExample("spatial.addPointLayerWithConfig", "Create a point layer with X and Y properties")
				.runCypher("CALL spatial.addPointLayerWithConfig('geom','lon:lat')", ExampleCypher::storeResult)
				.assertSingleResult("node", node -> {
					assertEquals("geom", ((Node) node).getProperty("layer"));
				});
	}

	@Test
	public void create_a_pointlayer_with_config_on_existing_wkt_layer() {
		execute("CALL spatial.addWKTLayer('geom','wkt')");
		try {
			testCall(db, "CALL spatial.addPointLayerWithConfig('geom','lon:lat')",
					(r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
			fail("Expected exception to be thrown");
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("Cannot create existing layer"));
		}
	}

	@Test
	public void create_a_pointlayer_with_config_on_existing_osm_layer() {
		execute("CALL spatial.addLayer('geom','OSM','')");
		try {
			testCall(db, "CALL spatial.addPointLayerWithConfig('geom','lon:lat')",
					(r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
			fail("Expected exception to be thrown");
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("Cannot create existing layer"));
		}
	}

	@Test
	public void create_a_pointlayer_with_rtree() {
		testCall(db, "CALL spatial.addPointLayer('geom')",
				(r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
	}

	@Test
	public void create_a_pointlayer_with_geohash() {
		testCall(db, "CALL spatial.addPointLayerGeohash('geom')",
				(r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
	}

	@Test
	public void create_a_pointlayer_with_zorder() {
		testCall(db, "CALL spatial.addPointLayerZOrder('geom')",
				(r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
	}

	@Test
	public void create_a_pointlayer_with_hilbert() {
		testCall(db, "CALL spatial.addPointLayerHilbert('geom')",
				(r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
	}

	@Test
	public void create_and_delete_a_pointlayer_with_rtree() {
		testCall(db, "CALL spatial.addPointLayer('geom')",
				(r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
		testCallCount(db, "CALL spatial.layers()", null, 1);
		execute("CALL spatial.removeLayer('geom')");
		testCallCount(db, "CALL spatial.layers()", null, 0);
	}

	@Test
	public void create_and_delete_a_pointlayer_with_geohash() {
		testCall(db, "CALL spatial.addPointLayerGeohash('geom')",
				(r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
		testCallCount(db, "CALL spatial.layers()", null, 1);
		execute("CALL spatial.removeLayer('geom')");
		testCallCount(db, "CALL spatial.layers()", null, 0);
	}

	@Test
	public void create_and_delete_a_pointlayer_with_zorder() {
		testCall(db, "CALL spatial.addPointLayerZOrder('geom')",
				(r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
		testCallCount(db, "CALL spatial.layers()", null, 1);
		execute("CALL spatial.removeLayer('geom')");
		testCallCount(db, "CALL spatial.layers()", null, 0);
	}

	@Test
	public void create_and_delete_a_pointlayer_with_hilbert() {
		testCall(db, "CALL spatial.addPointLayerHilbert('geom')",
				(r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
		testCallCount(db, "CALL spatial.layers()", null, 1);
		execute("CALL spatial.removeLayer('geom')");
		testCallCount(db, "CALL spatial.layers()", null, 0);
	}

	@Test
	public void create_a_simple_pointlayer_using_named_encoder() {
		Example example = docExample("spatial.addLayerWithEncoder", "Create a `SimplePointEncoder`");
		example.runCypher("CALL spatial.addLayerWithEncoder('geom','SimplePointEncoder','')",
						ExampleCypher::storeResult)
				.assertSingleResult("node", o -> {
					Assertions.assertThat(o).isInstanceOf(Node.class);
					Node node = (Node) o;
					assertEquals("geom", node.getProperty("layer"));
					assertEquals("org.neo4j.gis.spatial.encoders.SimplePointEncoder",
							node.getProperty("geomencoder"));
					assertEquals("org.neo4j.gis.spatial.SimplePointLayer", node.getProperty("layer_class"));
					assertFalse(node.hasProperty(PROP_GEOMENCODER_CONFIG));
				});
	}

	@Test
	public void create_a_simple_pointlayer_using_named_and_configured_encoder() {
		Example example = docExample("spatial.addLayerWithEncoder",
				"Create a `SimplePointEncoder` with a customized encoder configuration");
		example.runCypher("CALL spatial.addLayerWithEncoder('geom','SimplePointEncoder','x:y:mbr')",
						config -> config.storeResult().setComment("""
								Configures the encoder to use the nodes `x` property instead of `longitude`,
								the `y` property instead of `latitude` 
								and the `mbr` property instead of `bbox`.
								"""))
				.assertSingleResult("node", o -> {
					Assertions.assertThat(o).isInstanceOf(Node.class);
					Node node = (Node) o;
					assertEquals("geom", node.getProperty(PROP_LAYER));
					assertEquals("org.neo4j.gis.spatial.encoders.SimplePointEncoder",
							node.getProperty(PROP_GEOMENCODER));
					assertEquals("org.neo4j.gis.spatial.SimplePointLayer", node.getProperty(PROP_LAYER_CLASS));
					assertEquals("x:y:mbr", node.getProperty(PROP_GEOMENCODER_CONFIG));
				});
	}

	@Test
	public void create_a_native_pointlayer_using_named_encoder() {
		Example example = docExample("spatial.addLayerWithEncoder", "Create a `NativePointEncoder`");
		example.runCypher("CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','')",
						ExampleCypher::storeResult)
				.assertSingleResult("node", o -> {
					Assertions.assertThat(o).isInstanceOf(Node.class);
					Node node = (Node) o;
					assertEquals("geom", node.getProperty(PROP_LAYER));
					assertEquals("org.neo4j.gis.spatial.encoders.NativePointEncoder",
							node.getProperty(PROP_GEOMENCODER));
					assertEquals("org.neo4j.gis.spatial.SimplePointLayer", node.getProperty(PROP_LAYER_CLASS));
					assertFalse(node.hasProperty(PROP_GEOMENCODER_CONFIG));
				});
	}

	@Test
	public void create_a_native_pointlayer_using_named_and_configured_encoder() {
		Example example = docExample("spatial.addLayerWithEncoder",
				"Create a `NativePointEncoder` with a customized encoder configuration");
		example.runCypher("CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','pos:mbr')",
						config -> config.storeResult().setComment("""
								Configures the encoder to use the nodes `pos` property instead of `location`
								and the `mbr` property instead of `bbox`.
								"""))
				.assertSingleResult("node", o -> {
					Assertions.assertThat(o).isInstanceOf(Node.class);
					Node node = (Node) o;
					assertEquals("geom", node.getProperty(PROP_LAYER));
					assertEquals("org.neo4j.gis.spatial.encoders.NativePointEncoder",
							node.getProperty(PROP_GEOMENCODER));
					assertEquals("org.neo4j.gis.spatial.SimplePointLayer", node.getProperty(PROP_LAYER_CLASS));
					assertEquals("pos:mbr", node.getProperty(PROP_GEOMENCODER_CONFIG));
				});
	}

	@Test
	public void create_a_native_pointlayer_using_named_and_configured_encoder_with_cartesian() {
		Example example = docExample("spatial.addLayerWithEncoder",
				"Create a `NativePointEncoder` with a customized encoder configuration using Cartesian coordinates");
		example.runCypher("CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','pos:mbr:Cartesian')",
						config -> config.storeResult().setComment("""
								Configures the encoder to use the nodes `pos` property instead of `location`,
								the `mbr` property instead of `bbox` and Cartesian coordinates.
								"""))
				.assertSingleResult("node", o -> {
					Assertions.assertThat(o).isInstanceOf(Node.class);
					Node node = (Node) o;
					assertEquals("geom", node.getProperty(PROP_LAYER));
					assertEquals("org.neo4j.gis.spatial.encoders.NativePointEncoder",
							node.getProperty(PROP_GEOMENCODER));
					assertEquals("org.neo4j.gis.spatial.SimplePointLayer", node.getProperty(PROP_LAYER_CLASS));
					assertEquals("pos:mbr:Cartesian", node.getProperty(PROP_GEOMENCODER_CONFIG));
				});
	}

	@Test
	public void create_a_native_pointlayer_using_named_and_configured_encoder_with_geographic() {
		testCall(db, "CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','pos:mbr:WGS-84')", (r) -> {
			Node node = dump((Node) r.get("node"));
			assertEquals("geom", node.getProperty(PROP_LAYER));
			assertEquals("org.neo4j.gis.spatial.encoders.NativePointEncoder",
					node.getProperty(PROP_GEOMENCODER));
			assertEquals("org.neo4j.gis.spatial.SimplePointLayer", node.getProperty(PROP_LAYER_CLASS));
			assertEquals("pos:mbr:WGS-84", node.getProperty(PROP_GEOMENCODER_CONFIG));
		});
	}

	@Test
	public void create_a_wkt_layer_using_know_format() {
		testCall(db, "CALL spatial.addLayer('geom','WKT',null)",
				(r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
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
		docExample("spatial.layers", "Add and Remove a layer")
				.additionalSignature("spatial.removeLayer")
				.runCypher("CALL spatial.addWKTLayer('geom','wkt')", ExampleCypher::storeResult)
				.runCypher("CALL spatial.layers()", ExampleCypher::storeResult)
				.assertResult(res -> Assertions.assertThat(res).hasSize(1))
				.runCypher("CALL spatial.removeLayer('geom')", ExampleCypher::storeResult)
				.runCypher("CALL spatial.layers()", ExampleCypher::storeResult)
				.assertResult(res -> Assertions.assertThat(res).isEmpty());
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
		docExample("spatial.getFeatureAttributes", "Get the feature attributes of a layer")
				.additionalSignature("spatial.setFeatureAttributes")
				.runCypher("CALL spatial.addWKTLayer('geom','wkt')", ExampleCypher::storeResult)
				.runCypher("CALL spatial.getFeatureAttributes('geom')", ExampleCypher::storeResult)
				.assertResult(res -> Assertions.assertThat(res).hasSize(0))
				.runCypher("CALL spatial.setFeatureAttributes('geom',['name','type','color'])",
						ExampleCypher::storeResult)
				.runCypher("CALL spatial.getFeatureAttributes('geom')", ExampleCypher::storeResult)
				.assertResult(res -> Assertions.assertThat(res).hasSize(3));
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
				System.out.println(key + ": " + procs.get(key));
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
		Example example = docExample("spatial.layerTypes", "List the available layer types");
		example.runCypher("CALL spatial.layerTypes()", ExampleCypher::storeResult)
				.assertResult(res -> {
					Map<String, String> procs = new LinkedHashMap<>();
					for (Map<String, Object> r : res) {
						procs.put(r.get("name").toString(), r.get("signature").toString());
					}
					Assertions.assertThat(procs).containsOnly(
							entry(
									"simplepoint",
									"RegisteredLayerType(name='SimplePoint', geometryEncoder=SimplePointEncoder, layerClass=SimplePointLayer, index=LayerRTreeIndex, crs='WGS84(DD)', defaultConfig='longitude:latitude')"
							),
							entry(
									"nativepoint",
									"RegisteredLayerType(name='NativePoint', geometryEncoder=NativePointEncoder, layerClass=SimplePointLayer, index=LayerRTreeIndex, crs='WGS84(DD)', defaultConfig='location')"
							),
							entry(
									"nativepoints",
									"RegisteredLayerType(name='NativePoints', geometryEncoder=NativePointsEncoder, layerClass=EditableLayerImpl, index=LayerRTreeIndex, crs='WGS84(DD)', defaultConfig='geometry')"
							),
							entry(
									"wkt",
									"RegisteredLayerType(name='WKT', geometryEncoder=WKTGeometryEncoder, layerClass=EditableLayerImpl, index=LayerRTreeIndex, crs='WGS84(DD)', defaultConfig='geometry')"
							),
							entry(
									"wkb",
									"RegisteredLayerType(name='WKB', geometryEncoder=WKBGeometryEncoder, layerClass=EditableLayerImpl, index=LayerRTreeIndex, crs='WGS84(DD)', defaultConfig='geometry')"
							),
							entry(
									"geohash",
									"RegisteredLayerType(name='Geohash', geometryEncoder=SimplePointEncoder, layerClass=SimplePointLayer, index=LayerGeohashPointIndex, crs='WGS84(DD)', defaultConfig='longitude:latitude')"
							),
							entry(
									"zorder",
									"RegisteredLayerType(name='ZOrder', geometryEncoder=SimplePointEncoder, layerClass=SimplePointLayer, index=LayerZOrderPointIndex, crs='WGS84(DD)', defaultConfig='longitude:latitude')"
							),
							entry(
									"hilbert",
									"RegisteredLayerType(name='Hilbert', geometryEncoder=SimplePointEncoder, layerClass=SimplePointLayer, index=LayerHilbertPointIndex, crs='WGS84(DD)', defaultConfig='longitude:latitude')"
							),
							entry(
									"nativegeohash",
									"RegisteredLayerType(name='NativeGeohash', geometryEncoder=NativePointEncoder, layerClass=SimplePointLayer, index=LayerGeohashPointIndex, crs='WGS84(DD)', defaultConfig='location')"
							),
							entry(
									"nativezorder",
									"RegisteredLayerType(name='NativeZOrder', geometryEncoder=NativePointEncoder, layerClass=SimplePointLayer, index=LayerZOrderPointIndex, crs='WGS84(DD)', defaultConfig='location')"
							),
							entry(
									"nativehilbert",
									"RegisteredLayerType(name='NativeHilbert', geometryEncoder=NativePointEncoder, layerClass=SimplePointLayer, index=LayerHilbertPointIndex, crs='WGS84(DD)', defaultConfig='location')"
							),
							entry(
									"osm",
									"RegisteredLayerType(name='OSM', geometryEncoder=OSMGeometryEncoder, layerClass=OSMLayer, index=LayerRTreeIndex, crs='WGS84(DD)', defaultConfig='geometry')"
							)
					);
				});
	}

	@Test
	public void find_layer() {
		Example example = docExample("spatial.layer", "Find an existing layer");
		Example example1 = example.runCypher("CALL spatial.addWKTLayer('geom','wkt')",
				config -> config.setComment("Create a WKT layer"));
		example1.runCypher("CALL spatial.layer('geom')", ExampleCypher::storeResult)
				.assertSingleResult("node", r -> assertEquals("geom", ((Node) r).getProperty("layer")));
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

		Example example = docExample(procName, "Create a layer to index a node");
		example.runCypher("CALL " + procName + "('" + layerName + "')", ExampleCypher::storeResult)
				.runCypher("CREATE (n:Node {id: 42, latitude:60.1,longitude:15.2}) SET n.location=point(n) RETURN n",
						config -> config.setComment("Create a node to index"))
				.runCypher("MATCH (n:Node) WITH n CALL spatial.addNode('" + layerName + "',n) YIELD node RETURN node",
						config -> config.storeResult().setComment("Index node").storeResult())
				.assertSingleResult("node", o -> assertEquals(42L, ((Node) o).getProperty("id")))
				.runCypher("CALL spatial.withinDistance('" + layerName + "',{lon:15.0,lat:60.0},100)",
						config -> config.storeResult().setComment("Find node within distance"))
				.assertSingleResult("node", o -> assertEquals(42L, ((Node) o).getProperty("id")));

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
		docExample("spatial.addPointLayerXY", "Create a point layer with X and Y properties")
				.additionalSignature("spatial.addNodes")
				.additionalSignature("spatial.removeNode")
				.additionalSignature("spatial.removeNode.byId")
				.runCypher("CALL spatial.addPointLayerXY('geom','lon','lat')")
				.runCypher(
						"CREATE (n1:Node {id: 1, lat:60.1,lon:15.2}),(n2:Node {id: 2, lat:60.1,lon:15.3}) WITH n1,n2 CALL spatial.addNodes('geom',[n1,n2]) YIELD count RETURN n1,n2,count",
						config -> config.storeResult().setComment("Add two nodes to the layer"))
				.assertSingleResult("n1", o -> assertEquals(60.1, ((Node) o).getProperty("lat")))
				.assertSingleResult("n2", o -> assertEquals(60.1, ((Node) o).getProperty("lat")))
				.assertSingleResult("count", o -> assertEquals(2L, o))
				.runCypher("CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
						config -> config.storeResult().setComment("Find nodes within distance"))
				.assertResult(res -> {
					assertEquals(2, res.size());
					assertEquals(1L, ((Node) res.get(0).get("node")).getProperty("id"));
					assertEquals(2L, ((Node) res.get(1).get("node")).getProperty("id"));
				})
				.runCypher("""
								MATCH (node) WHERE node.id = 1
								CALL spatial.removeNode('geom', node) YIELD nodeId
								RETURN nodeId
								""",
						config -> config.setComment("Remove node 1"))
				.runCypher("CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
						ExampleCypher::storeResult)
				.assertSingleResult("node", o -> assertEquals(2L, ((Node) o).getProperty("id")))
				.runCypher("""
								MATCH (node) WHERE node.id = 2
								CALL spatial.removeNode.byId('geom', elementId(node)) YIELD nodeId
								RETURN nodeId
								""",
						config -> config.setComment("Remove node 2"))
				.runCypher("CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
						ExampleCypher::storeResult)
				.assertResult(res -> Assertions.assertThat(res).isEmpty());
	}

	@Test
	public void add_many_nodes_to_the_simple_point_layer_using_addNodes() {
		// Playing with this number in both tests leads to rough benchmarking of the addNode/addNodes comparison
		int count = 1000;
		execute("CALL spatial.addLayer('simple_poi','SimplePoint','')");
		String query = "UNWIND range(1,$count) as i\n" +
				"CREATE (n:Point {id:i, latitude:(56.0+toFloat(i)/100.0),longitude:(12.0+toFloat(i)/100.0)})\n" +
				"WITH collect(n) as points\n" +
				"CALL spatial.addNodes('simple_poi',points) YIELD count\n" +
				"RETURN count";
		testCountQuery("addNodes", query, count, "count", Map.of("count", count));
		testRemoveNodes("simple_poi", count);
	}

	@Test
	public void add_many_nodes_to_the_simple_point_layer_using_addNode() {
		// Playing with this number in both tests leads to rough benchmarking of the addNode/addNodes comparison
		int count = 1000;
		execute("CALL spatial.addLayer('simple_poi','SimplePoint','')");
		String query = "UNWIND range(1,$count) as i\n" +
				"CREATE (n:Point {id:i, latitude:(56.0+toFloat(i)/100.0),longitude:(12.0+toFloat(i)/100.0)})\n" +
				"WITH n\n" +
				"CALL spatial.addNode('simple_poi',n) YIELD node\n" +
				"RETURN count(node)";
		testCountQuery("addNode", query, count, "count(node)", Map.of("count", count));
		testRemoveNode("simple_poi", count);
	}

	@Test
	public void add_many_nodes_to_the_native_point_layer_using_addNodes() {
		// Playing with this number in both tests leads to rough benchmarking of the addNode/addNodes comparison
		int count = 1000;
		execute("CALL spatial.addLayer('native_poi','NativePoint','')");
		String query = "UNWIND range(1,$count) as i\n" +
				"WITH i, Point({latitude:(56.0+toFloat(i)/100.0),longitude:(12.0+toFloat(i)/100.0)}) AS location\n" +
				"CREATE (n:Point {id: i, location:location})\n" +
				"WITH collect(n) as points\n" +
				"CALL spatial.addNodes('native_poi',points) YIELD count\n" +
				"RETURN count";
		testCountQuery("addNodes", query, count, "count", Map.of("count", count));
		testRemoveNodes("native_poi", count);
	}

	@Test
	public void add_node_to_multiple_indexes_in_chunks() {
		Example example = docExample("spatial.addLayer", "Add the same node to multiple layers");
		Example example1 = example.runCypher("""
				UNWIND range(1,$count) as i
				CREATE (n:Point {
				    id: i,
				    point1: point( { latitude: 56.0, longitude: 12.0 } ),
				    point2: point( { latitude: 57.0, longitude: 13.0 } )
				})""", config -> config.setParams(Map.of("count", 100)).setTitle("Create some nodes"));
		Example example2 = example1.runCypher("""
				CALL spatial.addLayer(
					'point1',
					'NativePoint',
					'point1:point1BB',
					'{"referenceRelationshipType": "RTREE_P1_TYPE"}'
				)
				""", config1 -> config1.setComment("""
				Create a layer `point1` to index property `point1` of node `Point`.
				Save the bounding box in the property `point1BB` of the `Point` node.
				Associate the node with the index layer via relationship type `RTREE_P1_TYPE`.
				"""));
		Example example3 = example2.runCypher("""
				CALL spatial.addLayer(
					'point2',
					'NativePoint',
					'point2:point2BB',
					'{"referenceRelationshipType": "RTREE_P2_TYPE"}'
				)
				""", config -> config.setComment("""
				Create a layer `point2` to index property `point2` of node `Point`.
				Save the bounding box in the property `point2BB` of the `Point` node.
				Associate the node with the index layer via relationship type `RTREE_P2_TYPE`.
				"""));
		Example example4 = example3.runCypher("""
				MATCH (p:Point)
				WITH (count(p) / 10) AS pages, collect(p) AS nodes
				UNWIND range(0, pages) AS i CALL {
				    WITH i, nodes
				    CALL spatial.addNodes('point1', nodes[(i * 10)..((i + 1) * 10)]) YIELD count
				    RETURN count AS count
				} IN TRANSACTIONS OF 1 ROWS
				RETURN sum(count) AS count
				""", config1 -> config1.storeResult().setComment("Index the nodes in layer `point1` in chunks of 10"));
		example4.runCypher("""
				MATCH (p:Point)
				WITH (count(p) / 10) AS pages, collect(p) AS nodes
				UNWIND range(0, pages) AS i CALL {
					WITH i, nodes
					CALL spatial.addNodes('point2', nodes[(i * 10)..((i + 1) * 10)]) YIELD count
					RETURN count AS count
				} IN TRANSACTIONS OF 1 ROWS
				RETURN sum(count) AS count
				""", config -> config.storeResult().setComment("Index the nodes in layer `point2` in chunks of 10"));
	}

	@Test
	public void add_many_nodes_to_the_native_point_layer_using_addNode() {
		// Playing with this number in both tests leads to rough benchmarking of the addNode/addNodes comparison
		int count = 1000;
		execute("CALL spatial.addLayer('native_poi','NativePoint','')");
		String query = "UNWIND range(1,$count) as i\n" +
				"WITH i, Point({latitude:(56.0+toFloat(i)/100.0),longitude:(12.0+toFloat(i)/100.0)}) AS location\n" +
				"CREATE (n:Point {id: i, location:location})\n" +
				"WITH n\n" +
				"CALL spatial.addNode('native_poi',n) YIELD node\n" +
				"RETURN count(node)";
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
		docExample("spatial.importShapefile", "Import a shape-file")
				.runCypher("CALL spatial.importShapefile('shp/highway.shp')", ExampleCypher::storeResult)
				.assertSingleResult("count", count -> assertEquals(143L, count))
				.runCypher("CALL spatial.layers()", ExampleCypher::storeResult)
				.assertResult(r -> Assertions.assertThat(r).hasSize(1));
	}

	@Test
	public void import_shapefile_without_extension() {
		testCountQuery("importShapefile", "CALL spatial.importShapefile('shp/highway')", 143, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}

	@Test
	public void import_shapefile_to_layer() {
		docExample("spatial.importShapefileToLayer", "Import a shape-file")
				.runCypher("CALL spatial.addWKTLayer('geom','wkt')")
				.runCypher("CALL spatial.importShapefileToLayer('geom', 'shp/highway.shp')", ExampleCypher::storeResult)
				.assertSingleResult("count", count -> assertEquals(143L, count))
				.runCypher("CALL spatial.layers()", ExampleCypher::storeResult)
				.assertResult(r -> Assertions.assertThat(r).hasSize(1));
	}

	@Test
	public void import_osm() {
		docExample("spatial.importOSM", "Import an OSM file")
				.runCypher("CALL spatial.importOSM('map.osm')", ExampleCypher::storeResult)
				.assertSingleResult("count", count -> assertEquals(55L, count))
				.runCypher("CALL spatial.layers()", ExampleCypher::storeResult)
				.assertResult(r -> Assertions.assertThat(r).hasSize(1));
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
		docExample("spatial.importOSMToLayer", "Import an OSM file")
				.runCypher("CALL spatial.addLayer('geom','OSM','')")
				.runCypher("CALL spatial.importOSMToLayer('geom','map.osm')", ExampleCypher::storeResult)
				.assertSingleResult("count", count -> assertEquals(55L, count))
				.runCypher("CALL spatial.layers()", ExampleCypher::storeResult)
				.assertResult(r -> Assertions.assertThat(r).hasSize(1));
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

	@Disabled
	@Test
	public void import_cracow_to_layer() {
		execute("CALL spatial.addLayer('geom','OSM','')");
		testCountQuery("importCracowToLayer", "CALL spatial.importOSMToLayer('geom','issue-347/cra.osm')", 256253,
				"count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
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
						@SuppressWarnings("rawtypes") Map<String, Object> props = (Map) r.get("props");
						System.out.println(
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
						System.out.println(node.toString() + " at " + distance + ": " + geometry);
						if (geometry instanceof Point) {
							assertThat("Point has 2D coordinates",
									((Point) geometry).getCoordinate().getCoordinate().length, equalTo(2));
						} else if (geometry instanceof Map) {
							Map<String, Object> map = (Map<String, Object>) geometry;
							assertThat("Geometry should contain a type", map, hasKey("type"));
							assertThat("Geometry should contain coordinates", map, hasKey("coordinates"));
							assertThat("Geometry should not be a point", map.get("type"), not(equalTo("Point")));
						} else {
							fail("Geometry should be either a point or a Map containing coordinates");
						}
					}
				});
	}

	private void testCountQuery(String name, String query, long count, String column, Map<String, Object> params) {
		// warmup
		try (Transaction tx = db.beginTx()) {
			Result results = tx.execute("EXPLAIN " + query, params == null ? Map.of() : params);
			results.close();
			tx.commit();
		}
		long start = System.currentTimeMillis();
		testResult(db, query, params, res -> {
			assertTrue(res.hasNext(), "Expected a single result");
			long c = (Long) res.next().get(column);
			assertFalse(res.hasNext(), "Expected a single result");
			assertEquals(count, c, "Expected count of " + count + " nodes but got " + c);
		});
		System.out.println(name + " query took " + (System.currentTimeMillis() - start) + "ms - " + params);
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
		docExample("spatial.bbox", "Find geometries in a bounding box")
				.runCypher("CALL spatial.addPointLayer('geom')")
				.runCypher("""
						CREATE (n:Node {id: 1, latitude:60.1,longitude:15.2})
						WITH n CALL spatial.addNode('geom',n) YIELD node
						RETURN node
						""")
				.runCypher("CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})",
						c -> c.storeResult().setComment("Find node within bounding box"))
				.assertSingleResult("node", o -> assertEquals(1L, ((Node) o).getProperty("id")));
	}

	@Test
	public void find_geometries_in_a_polygon() {
		String polygon = "POLYGON((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2))";
		docExample("spatial.intersects", "Find geometries in a polygon")
				.runCypher("CALL spatial.addPointLayer('geom')")
				.runCypher("""
						UNWIND [ {name:'a',latitude:60.1,longitude:15.2}, {name:'b',latitude:60.3,longitude:15.5} ] as point
						CREATE (n:Node)
						SET n += point
						WITH n
						CALL spatial.addNode('geom',n) YIELD node
						RETURN node.name as name
						""", ExampleCypher::storeResult)
				.runCypher("CALL spatial.intersects('geom','" + polygon + "') YIELD node\n RETURN node.name as name",
						ExampleCypher::storeResult)
				.assertSingleResult("name", name -> assertEquals("b", name));
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
				r -> assertEquals("wkt", dump(((Node) r.get("node"))).getProperty("geomencoder_config")));
	}

	private static Node dump(Node n) {
		System.out.printf("id %s props %s%n", n.getElementId(), n.getAllProperties());
		System.out.flush();
		return n;
	}

	@Test
	public void add_a_WKT_geometry_to_a_layer() {
		String lineString = "LINESTRING (15.2 60.1, 15.3 60.1)";

		execute("CALL spatial.addWKTLayer('geom','wkt')");
		testCall(db, "CALL spatial.addWKT('geom',$wkt)", Map.of("wkt", lineString),
				r -> assertEquals(lineString, dump(((Node) r.get("node"))).getProperty("wkt")));
	}

	@Test
	public void find_geometries_close_to_a_point_wkt() {
		String lineString = "LINESTRING (15.2 60.1, 15.3 60.1)";
		docExample("spatial.addWKT", "Add a WKT geometry to a layer")
				.additionalSignature("spatial.addWKTLayer")
				.runCypher("CALL spatial.addWKTLayer('geom', 'wkt')")
				.runCypher("CALL spatial.addWKT('geom',$wkt)",
						c -> c.setParams(Map.of("wkt", lineString)).storeResult())
				.runCypher("CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)", ExampleCypher::storeResult)
				.assertSingleResult("node", node -> assertEquals(lineString, (((Node) node)).getProperty("wkt")));
	}

	@Test
	public void find_geometries_close_to_a_point_geohash() {
		var points = List.of("POINT (15.2 60.1)", "POINT (25.2 30.1)");
		docExample("spatial.addWKTs", "Add multiple WKT geometries to a layer")
				.additionalSignature("spatial.closest")
				.runCypher("CALL spatial.addLayer('geom','geohash','lon:lat')")
				.runCypher("CALL spatial.addWKTs('geom',$wkt)",
						c -> c.setParams(Map.of("wkt", points)).storeResult())
				.runCypher("CALL spatial.closest('geom',{lon:15.0, lat:60.0}, 1.0)", ExampleCypher::storeResult)
				.assertResult(list -> Assertions.assertThat(list).hasSize(1));
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
		Exception exception = assertThrows(QueryExecutionException.class, () -> {
			execute("CALL spatial.addLayer('line','NativePoints','points:bbox:Cartesian') YIELD node" +
					" MATCH (n:Foo)" +
					" WITH collect(n) AS nodes" +
					" CALL spatial.addNodes('line', nodes) YIELD count RETURN count");
		});

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
		Exception exception = assertThrows(QueryExecutionException.class, () -> {
			execute("CALL spatial.addLayer('line','NativePoints','points:bbox:Cartesian') YIELD node" +
					" MATCH (n:Foo)" +
					" WITH collect(n) AS nodes" +
					" CALL spatial.addNodes('line', nodes) YIELD count RETURN count");
		});

		assertEquals(
				"Failed to invoke procedure `spatial.addNodes`: Caused by: java.lang.IllegalStateException: Trying to decode geometry with wrong CRS: layer configured to crs=7203, but geometry has crs=9157",
				exception.getMessage());
	}

    /*

    @Test
    @Documented("update_a_WKT_geometry_in_a_layer")
    public void update_a_WKT_geometry_in_a_layer() {
        data.get();
        String geom = "geom";
        String response = post(Status.OK, "{\"layer\":\"" + geom + "\", \"format\":\"WKT\",\"nodePropertyName\":\"wkt\"}", ENDPOINT + "/graphdb/addEditableLayer");
        String wkt = "LINESTRING (15.2 60.1, 15.3 60.1)";
        String wkt2 = "LINESTRING (16.2 60.1, 15.3 60.1)";
        response = post(Status.OK, "{\"layer\":\"" + geom + "\", \"geometry\":\"" + wkt + "\"}", ENDPOINT + "/graphdb/addGeometryWKTToLayer");
        String self = (String) ((JSONObject) ((JSONArray) new JSONParser().parse(response)).get(0)).get("self");
        String geomId = self.substring(self.lastIndexOf("/") + 1);
        response = post(Status.OK, "{\"layer\":\"" + geom + "\", \"geometry\":\"" + wkt2 + "\",\"geometryNodeId\":" + geomId + "}", ENDPOINT + "/graphdb/updateGeometryFromWKT");

        assertTrue(response.contains(wkt2));
        assertTrue(response.contains("http://localhost:" + PORT + "/db/data/node/" + geomId));

    }

    @Test
    public void find_geometries_within__distance() {
        data.get();
        String response = post(Status.OK, "{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        response = post(Status.CREATED, "{\"name\":\"geom\", \"config\":{\"provider\":\"spatial\", \"geometry_type\":\"point\",\"lat\":\"lat\",\"lon\":\"lon\"}}", "http://localhost:" + PORT + "/db/data/index/node/");
        response = post(Status.CREATED, "{\"lat\":60.1, \"lon\":15.2}", "http://localhost:" + PORT + "/db/data/node");
        int nodeId = getNodeId(response);
        response = post(Status.OK, "{\"layer\":\"geom\", \"node\":\"http://localhost:" + PORT + "/db/data/node/" + nodeId + "\"}", ENDPOINT + "/graphdb/addNodeToLayer");
        response = post(Status.OK, "{\"layer\":\"geom\", \"pointX\":15.0,\"pointY\":60.0,\"distanceInKm\":100}", ENDPOINT + "/graphdb/findGeometriesWithinDistance");
        assertTrue(response.contains("60.1"));
    }


    @Test
    @Documented("add_a_wkt_node_to_the_spatial_index")
    public void add_a_wkt_node_to_the_spatial_index() {
        data.get();
        String response = post(Status.OK, "{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        //response = post(Status.CREATED,"{\"name\":\"geom_wkt\", \"config\":{\"provider\":\"spatial\", \"wkt\":\"wkt\"}}", "http://localhost:"+PORT+"/db/data/index/node/");
        response = post(Status.OK, "{\"layer\":\"geom_wkt\", \"format\":\"WKT\",\"nodePropertyName\":\"wkt\"}", ENDPOINT + "/graphdb/addEditableLayer");
        response = post(Status.CREATED, "{\"wkt\":\"POINT(15.2 60.1)\"}", "http://localhost:" + PORT + "/db/data/node");
        int nodeId = getNodeId(response);
        response = post(Status.OK, "{\"layer\":\"geom_wkt\", \"node\":\"http://localhost:" + PORT + "/db/data/node/" + nodeId + "\"}", ENDPOINT + "/graphdb/addNodeToLayer");
        assertTrue(findNodeInBox("geom_wkt", 15.0, 15.3, 60.0, 61.0).contains("60.1"));
        //update the node
        response = put(Status.NO_CONTENT, "{\"wkt\":\"POINT(31 61)\"}", "http://localhost:" + PORT + "/db/data/node/" + nodeId + "/properties");
        response = post(Status.OK, "{\"layer\":\"geom_wkt\", \"node\":\"http://localhost:" + PORT + "/db/data/node/" + nodeId + "\"}", ENDPOINT + "/graphdb/addNodeToLayer");
//        assertFalse(findNodeInBox("geom_wkt", 15.0, 15.3, 60.0, 61.0).contains("60.1"));
        assertTrue(findNodeInBox("geom_wkt", 30, 32, 60.0, 62.0).contains("31"));


    }

    @Test
    @Documented("Find geometries in a bounding box.")
    public void find_geometries_in_a_bounding_box_using_cypher() {
        data.get();
        String response = post(Status.OK, "{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        response = post(Status.CREATED, "{\"name\":\"geom\", \"config\":{\"provider\":\"spatial\", \"geometry_type\":\"point\",\"lat\":\"lat\",\"lon\":\"lon\"}}", "http://localhost:" + PORT + "/db/data/index/node/");
        response = post(Status.CREATED, "{\"lat\":60.1, \"lon\":15.2}", "http://localhost:" + PORT + "/db/data/node");
        int nodeId = getNodeId(response);
        // add domain-node via index, so that the geometry companion is created and added to the layer
        response = post(Status.CREATED, "{\"value\":\"dummy\",\"key\":\"dummy\", \"uri\":\"http://localhost:" + PORT + "/db/data/node/" + nodeId + "\"}", "http://localhost:" + PORT + "/db/data/index/node/geom");

        response = post(Status.OK, "{\"query\":\"start node = node:geom(\'bbox:[15.0,15.3,60.0,60.2]\') return node\"}", "http://localhost:" + PORT + "/db/data/cypher");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(response).get("data").get(0).get(0).get("data");
        assertEquals(15.2, node.get("lon").getDoubleValue());
        assertEquals(60.1, node.get("lat").getDoubleValue());
    }

    @Test
    @Documented("find_geometries_within__distance_using_cypher")
    public void find_geometries_within__distance_using_cypher() {
        data.get();
        String response = post(Status.OK, "{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        response = post(Status.CREATED, "{\"name\":\"geom\", \"config\":{\"provider\":\"spatial\", \"geometry_type\":\"point\",\"lat\":\"lat\",\"lon\":\"lon\"}}", "http://localhost:" + PORT + "/db/data/index/node/");
        response = post(Status.CREATED, "{\"lat\":60.1, \"lon\":15.2}", "http://localhost:" + PORT + "/db/data/node");
        int nodeId = getNodeId(response);

        // add domain-node via index, so that the geometry companion is created and added to the layer
        response = post(Status.CREATED, "{\"value\":\"dummy\",\"key\":\"dummy\", \"uri\":\"http://localhost:" + PORT + "/db/data/node/" + nodeId + "\"}", "http://localhost:" + PORT + "/db/data/index/node/geom");
        response = post(Status.OK, "{\"query\":\"start node = node:geom(\'withinDistance:[60.0,15.0, 100.0]\') return node\"}", "http://localhost:" + PORT + "/db/data/cypher");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(response).get("data").get(0).get(0).get("data");
        assertEquals(15.2, node.get("lon").getDoubleValue());
        assertEquals(60.1, node.get("lat").getDoubleValue());
    }

    */
}
