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
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gis.spatial.Constants.LABEL_LAYER;
import static org.neo4j.gis.spatial.Constants.PROP_CRS;
import static org.neo4j.gis.spatial.Constants.PROP_GEOMENCODER;
import static org.neo4j.gis.spatial.Constants.PROP_GEOMENCODER_CONFIG;
import static org.neo4j.gis.spatial.Constants.PROP_INDEX_CONFIG;
import static org.neo4j.gis.spatial.Constants.PROP_INDEX_TYPE;
import static org.neo4j.gis.spatial.Constants.PROP_LAYER;
import static org.neo4j.gis.spatial.Constants.PROP_LAYER_TYPE;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.SpatialRelationshipTypes;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.index.IndexManagerImpl;
import org.neo4j.gis.spatial.utilities.LayerTypePresetRegistry;
import org.neo4j.gis.spatial.utilities.ReferenceNodes;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.internal.kernel.api.security.SecurityContext;
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
			case 0 -> spatial.getOrCreateSimplePointLayer(tx, name, null, new SimplePointEncoder.Config("x", "y"),
					false);
			case 1 -> spatial.getOrCreateNativePointLayer(tx, name, null, null, false);
			default -> spatial.getOrCreateDefaultLayer(tx, name, false);
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

	/**
	 * a is inside the bbox and the distance, b only inside the polygon; nodes carry both property naming schemes
	 */
	@ParameterizedTest
	@ValueSource(strings = {
			"spatial.addPointLayer('geom')",
			"spatial.addPointLayer('geom','geohash')",
			"spatial.addPointLayerGeohash('geom')",
			"spatial.addPointLayerZOrder('geom')",
			"spatial.addPointLayerHilbert('geom')",
			"spatial.addPointLayerXY('geom','lon','lat')",
			"spatial.addPointLayerXY('geom','lon','lat','geohash')",
	})
	public void point_layer_add_search_and_remove(String createLayer) {
		testCall(db, "CALL " + createLayer, r -> assertEquals("geom", ((Node) r.get("node")).getProperty(PROP_LAYER)));
		Node a = createNode(
				"CREATE (n:Node {name:'a',latitude:60.1,longitude:15.2,lat:60.1,lon:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		createNode(
				"CREATE (n:Node {name:'b',latitude:60.3,longitude:15.5,lat:60.3,lon:15.5}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
				"node");
		testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0},{lon:15.3, lat:60.2})",
				r -> assertEquals(a, r.get("node")));
		testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},20)",
				r -> assertEquals(a, r.get("node")));
		String polygon = "POLYGON((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2))";
		testCall(db, "CALL spatial.intersects('geom','" + polygon + "') YIELD node RETURN node.name as name",
				r -> assertEquals("b", r.get("name")));
		execute("CALL spatial.removeLayer('geom')");
		testCallCount(db, "CALL spatial.layers()", null, 0);
	}

	// https://github.com/neo4j-contrib/spatial/issues/283
	@ParameterizedTest
	@ValueSource(strings = {
			"spatial.addPointLayer('geom')",
			"spatial.addPointLayerGeohash('geom')",
			"spatial.addPointLayerZOrder('geom')",
			"spatial.addPointLayerHilbert('geom')",
			"spatial.addWKTLayer('geom','wkt')",
	})
	public void remove_layer_keeps_added_domain_nodes(String createLayer) {
		execute("CALL " + createLayer);
		execute("""
				CREATE (a:Loc {name:'a', latitude:60.1, longitude:15.2, wkt:'POINT(15.2 60.1)'})-[:KNOWS]->(b:Loc {name:'b'})
				WITH a CALL spatial.addNode('geom', a) YIELD node RETURN node""");
		execute("CALL spatial.removeLayer('geom')");
		testCall(db, """
						MATCH (a:Loc {name:'a'})-[r]->(:Loc {name:'b'})
						RETURN type(r) AS type, a.latitude AS lat, COUNT { (a)--() } AS rels, labels(a) AS labels""",
				r -> {
					assertEquals("KNOWS", r.get("type"));
					assertEquals(60.1, r.get("lat"));
					assertEquals(1L, r.get("rels"));
					assertEquals(List.of("Loc"), r.get("labels"));
				});
	}

	// https://github.com/neo4j-contrib/spatial/issues/283
	@Test
	public void remove_layer_deletes_nodes_created_by_the_layer() {
		execute("CALL spatial.addWKTLayer('geom','wkt')");
		execute("CALL spatial.addWKT('geom','POINT(15.2 60.1)')");
		execute("CALL spatial.addWKTs('geom',['LINESTRING (15.2 60.1, 15.3 60.1)','POINT(15.5 60.3)'])");
		testCall(db, "MATCH (n:_SpatialManagedGeometry) RETURN count(n) AS count", r -> assertEquals(3L, r.get("count")));
		execute("CALL spatial.removeLayer('geom')");
		testCall(db, "MATCH (n) RETURN count(n) AS count", r -> assertEquals(0L, r.get("count")));
	}

	// the migration statement from spatial.removeLayer.adoc, for nodes created before _SpatialManagedGeometry existed
	@Test
	public void remove_layer_deletes_legacy_nodes_after_labelling_them() {
		execute("CALL spatial.addWKTLayer('geom','wkt')");
		execute("CALL spatial.addWKTs('geom',['POINT(15.2 60.1)','POINT(15.5 60.3)'])");
		execute("""
				CREATE (a:Loc {wkt:'POINT(15.3 60.2)'})-[:KNOWS]->(:Loc)
				WITH a CALL spatial.addNode('geom', a) YIELD node RETURN node""");
		execute("MATCH (n:_SpatialManagedGeometry) REMOVE n:_SpatialManagedGeometry");
		execute("""
				MATCH (:SpatialLayer {layer: $name})-[:RTREE_ROOT]->()-[:RTREE_CHILD*0..]->()-[:RTREE_REFERENCE]->(g)
				WHERE size(labels(g)) = 0 AND COUNT { (g)--() } = 1
				SET g:_SpatialManagedGeometry""", Map.of("name", "geom"));
		execute("CALL spatial.removeLayer('geom')");
		testCall(db, "MATCH (n) RETURN count(n) AS count", r -> assertEquals(2L, r.get("count")));
	}

	// https://github.com/neo4j-contrib/spatial/issues/254, #65, #178
	@ParameterizedTest
	@ValueSource(strings = {
			"spatial.addPointLayer('geom')",
			"spatial.addPointLayerGeohash('geom')",
			"spatial.addPointLayerZOrder('geom')",
			"spatial.addPointLayerHilbert('geom')",
			"spatial.addWKTLayer('geom','wkt')",
	})
	public void add_node_twice_indexes_it_once(String createLayer) {
		execute("CALL " + createLayer);
		execute("CREATE (:Loc {name:'a', latitude:60.1, longitude:15.2, wkt:'POINT(15.2 60.1)'})");
		execute("MATCH (n:Loc) CALL spatial.addNode('geom', n) YIELD node RETURN node");
		execute("MATCH (n:Loc) CALL spatial.addNode('geom', n) YIELD node RETURN node");
		assertIndexedOnce();
		execute("MATCH (n:Loc) CALL spatial.addNodes('geom', [n]) YIELD count RETURN count");
		assertIndexedOnce();
		execute("MATCH (n:Loc) CALL spatial.addNodes('geom', [n, n]) YIELD count RETURN count");
		assertIndexedOnce();
		execute("MATCH (n:Loc) CALL spatial.removeNode('geom', n) YIELD nodeId RETURN nodeId");
		testCall(db, "CALL spatial.getFeatureCount('geom') YIELD count", r -> assertEquals(0L, r.get("count")));
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:15.2,lat:60.1},10)", null, 0);
	}

	// https://github.com/neo4j-contrib/spatial/issues/254: both adds in one transaction
	@Test
	public void add_node_twice_in_one_transaction_indexes_it_once() {
		execute("CALL spatial.addPointLayer('geom')");
		execute("""
				CREATE (n:Loc {name:'a', latitude:60.1, longitude:15.2})
				WITH n CALL spatial.addNode('geom', n) YIELD node
				WITH node CALL spatial.addNode('geom', node) YIELD node AS again
				RETURN again""");
		assertIndexedOnce();
	}

	// an unchanged node added again keeps its reference: no remove and re-insert
	@Test
	public void add_unchanged_node_again_leaves_the_tree_untouched() {
		execute("CALL spatial.addPointLayer('geom')");
		execute("""
				CREATE (n:Loc {latitude:60.1, longitude:15.2})
				WITH n CALL spatial.addNode('geom', n) YIELD node RETURN node""");
		String refId = "MATCH (:Loc)<-[r:RTREE_REFERENCE]-() RETURN elementId(r) AS id";
		String[] before = new String[1];
		testCall(db, refId, r -> before[0] = (String) r.get("id"));
		execute("MATCH (n:Loc) CALL spatial.addNode('geom', n) YIELD node RETURN node");
		testCall(db, refId, r -> assertEquals(before[0], r.get("id")));
	}

	// https://github.com/neo4j-contrib/spatial/issues/229
	@ParameterizedTest
	@ValueSource(strings = {
			"spatial.addPointLayer('geom')",
			"spatial.addPointLayerGeohash('geom')",
			"spatial.addPointLayerZOrder('geom')",
			"spatial.addPointLayerHilbert('geom')",
			"spatial.addWKTLayer('geom','wkt')",
	})
	public void add_moved_node_again_updates_its_location(String createLayer) {
		execute("CALL " + createLayer);
		execute("""
				CREATE (n:Loc {name:'a', latitude:60.1, longitude:15.2, wkt:'POINT(15.2 60.1)'})
				WITH n CALL spatial.addNode('geom', n) YIELD node RETURN node""");
		execute("""
				MATCH (n:Loc) SET n.latitude = 10.1, n.longitude = 5.2, n.wkt = 'POINT(5.2 10.1)'
				WITH n CALL spatial.addNode('geom', n) YIELD node RETURN node""");
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:15.2,lat:60.1},10)", null, 0);
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:5.2,lat:10.1},10)", null, 1);
		testCall(db, "CALL spatial.getFeatureCount('geom') YIELD count", r -> assertEquals(1L, r.get("count")));
	}

	// the node carries an RTREE_REFERENCE from each layer
	@Test
	public void add_moved_node_again_to_one_of_two_layers() {
		execute("CALL spatial.addPointLayer('geom')");
		execute("CALL spatial.addPointLayer('other')");
		execute("""
				CREATE (n:Loc {latitude:60.1, longitude:15.2})
				WITH n CALL spatial.addNode('geom', n) YIELD node
				WITH n CALL spatial.addNode('other', n) YIELD node RETURN node""");
		execute("""
				MATCH (n:Loc) SET n.latitude = 10.1, n.longitude = 5.2
				WITH n CALL spatial.addNode('geom', n) YIELD node RETURN node""");
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:15.2,lat:60.1},10)", null, 0);
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:5.2,lat:10.1},10)", null, 1);
		testCall(db, "CALL spatial.getFeatureCount('other') YIELD count", r -> assertEquals(1L, r.get("count")));
		execute("MATCH (n:Loc) CALL spatial.removeNode('other', n) YIELD nodeId RETURN nodeId");
		testCall(db, "CALL spatial.getFeatureCount('other') YIELD count", r -> assertEquals(0L, r.get("count")));
		testCall(db, "CALL spatial.getFeatureCount('geom') YIELD count", r -> assertEquals(1L, r.get("count")));
	}

	// https://github.com/neo4j-contrib/spatial/issues/439
	@Test
	public void remove_node_from_one_of_two_wkt_layers() {
		execute("CALL spatial.addWKTLayer('Layer1', 'area')");
		execute("CALL spatial.addWKTLayer('Layer2', 'area')");
		execute("""
				CREATE (site:Site {area: 'POLYGON((-63.183 -17.784, -63.181 -17.784, -63.181 -17.7825, -63.183 -17.7825, -63.183 -17.784))'})
				WITH site CALL spatial.addNode('Layer1', site) YIELD node
				WITH site CALL spatial.addNode('Layer2', site) YIELD node RETURN node""");
		execute("MATCH (site:Site) CALL spatial.removeNode('Layer1', site) YIELD nodeId RETURN nodeId");
		String point = "{lon:-63.182, lat:-17.783}";
		testCallCount(db, "CALL spatial.intersects('Layer1', " + point + ")", null, 0);
		testCallCount(db, "CALL spatial.intersects('Layer2', " + point + ")", null, 1);
		execute("MATCH (site:Site) CALL spatial.removeNode('Layer2', site) YIELD nodeId RETURN nodeId");
		testCall(db, "MATCH (site:Site) RETURN COUNT { (site)<-[:RTREE_REFERENCE]-() } AS refs",
				r -> assertEquals(0L, r.get("refs")));
	}

	// older versions added a second reference on each repeated add
	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void add_node_again_drops_duplicate_reference_of_older_versions(boolean moved) {
		execute("CALL spatial.addPointLayer('geom')");
		execute("""
				CREATE (n:Loc {latitude:60.1, longitude:15.2})
				WITH n CALL spatial.addNode('geom', n) YIELD node RETURN node""");
		execute("MATCH (leaf)-[:RTREE_REFERENCE]->(n:Loc) CREATE (leaf)-[:RTREE_REFERENCE]->(n)");
		if (moved) {
			execute("MATCH (n:Loc) SET n.latitude = 10.1, n.longitude = 5.2");
		}
		execute("MATCH (n:Loc) CALL spatial.addNode('geom', n) YIELD node RETURN node");
		testCall(db, "MATCH (n:Loc) RETURN COUNT { (n)<-[:RTREE_REFERENCE]-() } AS refs",
				r -> assertEquals(1L, r.get("refs")));
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:15.2,lat:60.1},10)", null, moved ? 0 : 1);
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:5.2,lat:10.1},10)", null, moved ? 1 : 0);
	}

	private void assertIndexedOnce() {
		testCall(db, "MATCH (n:Loc) RETURN COUNT { (n)<-[:RTREE_REFERENCE]-() } AS refs", r -> assertThat(
				(Long) r.get("refs"), lessThanOrEqualTo(1L)));
		testCall(db, "CALL spatial.getFeatureCount('geom') YIELD count", r -> assertEquals(1L, r.get("count")));
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:15.2,lat:60.1},10)", null, 1);
	}

	// This tests issue https://github.com/neo4j-contrib/spatial/issues/298
	@ParameterizedTest
	@ValueSource(strings = {"addPointLayer", "addPointLayerGeohash"})
	public void add_node_point_layer_and_search_multiple_points_precision(String createLayer) {
		execute("CALL spatial." + createLayer + "('bar')");
		execute("create (n:Point) set n={latitude: 52.2029252, longitude: 0.0905302} with n call spatial.addNode('bar', n) yield node return node");
		execute("create (n:Point) set n={latitude: 52.202925, longitude: 0.090530} with n call spatial.addNode('bar', n) yield node return node");
		long countHigh = execute(
				"call spatial.withinDistance('bar', {latitude:52.2029252,longitude:0.0905302}, 100) YIELD node RETURN node");
		assertEquals(2L, countHigh, "Expected two nodes when using high precision");
	}

	@ParameterizedTest
	@CsvSource(delimiter = '|', value = {
			"SimplePointEncoder||xProperty=longitude,yProperty=latitude,bboxProperty=bbox",
			"SimplePointEncoder|x:y:mbr|xProperty=x,yProperty=y,bboxProperty=mbr",
			"NativePointEncoder||locationProperty=location,bboxProperty=bbox,crs=wgs84",
			"NativePointEncoder|pos:mbr|locationProperty=pos,bboxProperty=mbr,crs=wgs84",
			"NativePointEncoder|pos:mbr:Cartesian|locationProperty=pos,bboxProperty=mbr,crs=Cartesian",
			"NativePointEncoder|pos:mbr:WGS-84|locationProperty=pos,bboxProperty=mbr,crs=WGS-84",
	})
	public void create_a_pointlayer_using_named_encoder(String encoder, String config, String expectedConfig) {
		testCall(db, "CALL spatial.addLayerWithEncoder('geom',$encoder,$config)",
				Map.of("encoder", encoder, "config", config == null ? "" : config), (r) -> {
					Node node = (Node) r.get("node");
					assertEquals("geom", node.getProperty(PROP_LAYER));
					assertEquals(encoder, node.getProperty(PROP_GEOMENCODER));
					assertEquals("SimplePointLayer", node.getProperty(PROP_LAYER_TYPE));
					assertFalse(node.hasProperty(PROP_GEOMENCODER_CONFIG));
					Map<String, Object> expected = new LinkedHashMap<>();
					for (String entry : expectedConfig.split(",")) {
						String[] kv = entry.split("=");
						expected.put(kv[0], kv[1]);
					}
					assertEquals(expected, encoderConfig(node));
				});
	}

	private static Map<String, Object> encoderConfig(Node node) {
		String prefix = PROP_GEOMENCODER_CONFIG + ".";
		Map<String, Object> config = new LinkedHashMap<>();
		node.getAllProperties().forEach((key, value) -> {
			if (key.startsWith(prefix)) {
				config.put(key.substring(prefix.length()), value);
			}
		});
		return config;
	}

	//
	// Testing interaction between Neo4j Spatial and the Neo4j 3.0 Point type (point() and distance() functions)
	//

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
			assertEquals(Map.of("locationProperty", "pos", "bboxProperty", "mbr", "crs", "wgs84"),
					encoderConfig(node));
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
			assertEquals(Map.of("xProperty", "x", "yProperty", "y", "bboxProperty", "bbox"), encoderConfig(node));

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
	public void add_and_remove_multiple_layers() {
		int NUM_LAYERS = 10;
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
					"spatial.addLayer(name :: STRING, type :: STRING, encoderConfig :: ANY, indexConfig =  :: ANY) :: (node :: NODE)",
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
			Assertions.assertThat(layerTypes).hasSize(11);

			var simplePoint = Map.<String, Object>of("xProperty", "longitude", "yProperty", "latitude",
					"bboxProperty", "bbox");
			var nativePoint = Map.<String, Object>of("locationProperty", "location", "bboxProperty", "bbox",
					"crs", "wgs84");
			var singleProperty = Map.<String, Object>of("geomProperty", "geometry", "bboxProperty", "bbox");
			assertLayerType(layerTypes, "SimplePoint", "SimplePointEncoder", "SimplePointLayer", "rtree", simplePoint);
			assertLayerType(layerTypes, "NativePoint", "NativePointEncoder", "SimplePointLayer", "rtree", nativePoint);
			assertLayerType(layerTypes, "NativePoints", "NativePointsEncoder", "EditableLayer", "rtree",
					Map.of("geomProperty", "geometry", "bboxProperty", "bbox", "crs", "wgs84"));
			assertLayerType(layerTypes, "WKT", "WKTGeometryEncoder", "EditableLayer", "rtree", singleProperty);
			assertLayerType(layerTypes, "WKB", "WKBGeometryEncoder", "EditableLayer", "rtree", singleProperty);
			assertLayerType(layerTypes, "Geohash", "SimplePointEncoder", "SimplePointLayer", "geohash", simplePoint);
			assertLayerType(layerTypes, "ZOrder", "SimplePointEncoder", "SimplePointLayer", "zorder", simplePoint);
			assertLayerType(layerTypes, "Hilbert", "SimplePointEncoder", "SimplePointLayer", "hilbert", simplePoint);
			assertLayerType(layerTypes, "NativeGeohash", "NativePointEncoder", "SimplePointLayer", "geohash",
					nativePoint);
			assertLayerType(layerTypes, "NativeZOrder", "NativePointEncoder", "SimplePointLayer", "zorder",
					nativePoint);
			assertLayerType(layerTypes, "NativeHilbert", "NativePointEncoder", "SimplePointLayer", "hilbert",
					nativePoint);
		});
	}

	private void assertLayerType(Map<String, Map<String, Object>> layerTypes, String id, String encoder, String layer,
			String index, Map<String, Object> defaultConfig) {
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
	public void list_encoders() {
		assertIdsAndDescriptions("CALL spatial.encoders.list()", "NativePointEncoder", "NativePointsEncoder",
				"SimpleGraphEncoder", "SimplePointEncoder", "SimplePropertyEncoder", "WKBGeometryEncoder",
				"WKTGeometryEncoder");
	}

	@Test
	public void list_index_types() {
		assertIdsAndDescriptions("CALL spatial.indexTypes.list()", "geohash", "hilbert", "rtree", "zorder");
	}

	private void assertIdsAndDescriptions(String call, String... expectedIds) {
		testResult(db, call, res -> {
			var rows = res.stream().toList();
			// exact order: sorted by id, one row per component although each is registered under several ids
			Assertions.assertThat(rows).extracting(r -> r.get("id")).containsExactly((Object[]) expectedIds);
			Assertions.assertThat(rows).allSatisfy(r -> Assertions.assertThat((String) r.get("description")).isNotBlank());
		});
	}

	@Test
	public void encoder_config() {
		assertConfig("CALL spatial.encoders.config('SimplePointEncoder')",
				configParam("bboxProperty", "STRING", "bbox"),
				configParam("xProperty", "STRING", "longitude"),
				configParam("yProperty", "STRING", "latitude"));
	}

	@Test
	public void encoder_config_by_class_name() {
		testCallCount(db, "CALL spatial.encoders.config('org.neo4j.gis.spatial.encoders.SimplePointEncoder')", null, 3);
	}

	@Test
	public void index_type_config() {
		assertConfig("CALL spatial.indexTypes.config('rtree')",
				configParam("maxNodeReferences", "INTEGER", 100L),
				configParam("referenceRelationshipType", "STRING", "RTREE_REFERENCE"),
				configParam("shouldMergeTrees", "BOOLEAN", false),
				configParam("splitMode", "STRING", "greene"));
	}

	@Test
	public void index_type_config_of_non_configurable_index_is_empty() {
		testCallCount(db, "CALL spatial.indexTypes.config('hilbert')", null, 0);
	}

	@SafeVarargs
	private void assertConfig(String call, Map<String, Object>... expected) {
		testResult(db, call, res -> {
			var rows = res.stream().toList();
			Assertions.assertThat(rows).allSatisfy(r -> Assertions.assertThat((String) r.get("description")).isNotBlank());
			Assertions.assertThat(rows)
					.extracting(r -> Map.<String, Object>of("name", r.get("name"), "type", r.get("type"), "defaultValue",
							r.get("defaultValue")))
					.containsExactly(expected);
		});
	}

	private static Map<String, Object> configParam(String name, String type, Object defaultValue) {
		return Map.of("name", name, "type", type, "defaultValue", defaultValue);
	}

	@Test
	public void encoder_config_of_unknown_id_fails() {
		testCallFails(db, "CALL spatial.encoders.config('Nope')", null,
				"unknown encoder 'Nope' - supported are [NativePointEncoder, NativePointsEncoder, SimpleGraphEncoder");
	}

	@Test
	public void index_type_config_of_unknown_id_fails() {
		testCallFails(db, "CALL spatial.indexTypes.config('Nope')", null,
				"unknown index type 'Nope' - supported are [geohash, hilbert, rtree, zorder]");
	}

	@ParameterizedTest
	@CsvSource({"SimplePoint,rtree", "Geohash,geohash", "ZOrder,zorder", "Hilbert,hilbert", "NativePoint,rtree",
			"NativeGeohash,geohash", "NativeZOrder,zorder", "NativeHilbert,hilbert", "WKT,rtree"})
	public void add_layer_uses_the_preset_index(String type, String expectedIndex) {
		testCall(db, "CALL spatial.addLayer('layer', $type, null)", Map.of("type", type), r -> {
			Node node = (Node) r.get("node");
			assertEquals(expectedIndex, node.getProperty(PROP_INDEX_TYPE));
			// only space-filling-curve indexes get the preset CRS
			assertEquals(!expectedIndex.equals("rtree"), node.hasProperty(PROP_CRS));
		});
	}

	@ParameterizedTest
	@ValueSource(strings = {"'{\"maxNodeReferences\":20}'", "{maxNodeReferences: 20}"})
	public void add_layer_applies_index_config(String indexConfig) {
		testCall(db, "CALL spatial.addLayer('layer', 'SimplePoint', null, " + indexConfig + ") YIELD node "
				+ "RETURN node.`" + PROP_INDEX_CONFIG + ".maxNodeReferences` AS max", r -> assertEquals(20L, r.get("max")));
	}

	@Test
	public void add_layer_with_encoder_config_map() {
		testCall(db, "CALL spatial.addLayer('layer', 'SimplePoint', {xProperty: 'x', yProperty: 'y'})",
				r -> assertEquals(Map.of("xProperty", "x", "yProperty", "y", "bboxProperty", "bbox"),
						encoderConfig((Node) r.get("node"))));
	}

	@Test
	public void add_layer_with_encoder_and_config_maps() {
		testCall(db, "CALL spatial.addLayerWithEncoder('layer', 'NativePointEncoder', {locationProperty: 'pos'}, "
						+ "{maxNodeReferences: 20})",
				r -> {
					Node node = (Node) r.get("node");
					assertEquals(Map.of("locationProperty", "pos", "bboxProperty", "bbox", "crs", "wgs84"),
							encoderConfig(node));
					assertEquals(20L, node.getProperty(PROP_INDEX_CONFIG + ".maxNodeReferences"));
				});
	}

	@Test
	public void add_point_layer_with_config_map() {
		testCall(db, "CALL spatial.addPointLayerWithConfig('layer', {xProperty: 'lon', yProperty: 'lat'}, 'hilbert')",
				r -> {
					Node node = (Node) r.get("node");
					assertEquals(Map.of("xProperty", "lon", "yProperty", "lat", "bboxProperty", "bbox"),
							encoderConfig(node));
					// 'lat' hints a geographic CRS, as for the legacy string form
					assertTrue(node.hasProperty(PROP_CRS));
				});
	}

	@Test
	public void add_layer_rejects_unknown_config_option() {
		testCallFails(db, "CALL spatial.addLayer('layer', 'SimplePoint', {nope: 'x'})", null, "nope");
	}

	@Test
	public void add_layer_rejects_config_map_for_index_without_options() {
		testCallFails(db, "CALL spatial.addLayer('layer', 'Geohash', null, {maxNodeReferences: 20})", null,
				"index type 'geohash' has no config options, got: {maxNodeReferences=20}");
	}

	@Test
	public void add_layer_ignores_legacy_config_string_for_index_without_options() {
		testCallCount(db, "CALL spatial.addLayer('layer', 'Geohash', null, '{\"maxNodeReferences\":20}')", null, 1);
	}

	@Test
	public void add_layer_rejects_config_of_wrong_type_for_index_without_options() {
		testCallFails(db, "CALL spatial.addLayer('layer', 'Geohash', null, 42)", null,
				"config must be a MAP or STRING, got: Long");
	}

	@Test
	public void add_layer_rejects_config_of_wrong_type() {
		testCallFails(db, "CALL spatial.addLayer('layer', 'SimplePoint', null, 42)", null,
				"config must be a MAP or STRING, got: Long");
	}

	@Test
	public void add_layer_of_same_type_keeps_preset_default_config() {
		execute("CALL spatial.addLayer('a','SimplePoint',null)");
		execute("CALL spatial.addLayer('b','SimplePoint','x:y')");
		execute("CALL spatial.addLayer('c','SimplePoint',null)");
		try (Transaction tx = db.beginTx()) {
			SpatialDatabaseService spatial = new SpatialDatabaseService(
					new IndexManagerImpl((GraphDatabaseAPI) db, SecurityContext.AUTH_DISABLED));
			assertEquals("SimplePointEncoder(x='longitude', y='latitude', bbox='bbox')",
					spatial.getLayer(tx, "a", true).getGeometryEncoder().getSignature());
			assertEquals("SimplePointEncoder(x='x', y='y', bbox='bbox')",
					spatial.getLayer(tx, "b", true).getGeometryEncoder().getSignature());
			assertEquals("SimplePointEncoder(x='longitude', y='latitude', bbox='bbox')",
					spatial.getLayer(tx, "c", true).getGeometryEncoder().getSignature());
			assertEquals("longitude", ((SimplePointEncoder.Config) LayerTypePresetRegistry.INSTANCE
					.getRegisteredLayerType("SimplePoint").defaultEncoderConfig()).xProperty);
			tx.commit();
		}
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
		int count = 100;
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
		int count = 100;
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
		int count = 100;
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
		int count = 100;
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

	@ParameterizedTest
	@ValueSource(strings = {"../example-data/shp/highway.shp", "../example-data/shp/highway"})
	public void import_shapefile(String path) {
		testCountQuery("importShapefile", "CALL spatial.importShapefile($path)", 143, "count", Map.of("path", path));
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
	public void create_a_WKT_layer() {
		testCall(db, "CALL spatial.addWKTLayer('geom','wkt')",
				r -> assertEquals(Map.of("geomProperty", "wkt", "bboxProperty", "bbox"),
						encoderConfig((Node) r.get("node"))));
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

	@ParameterizedTest
	@ValueSource(strings = {"geohash", "zorder", "hilbert"})
	public void find_geometries_close_to_a_point(String layerType) {
		execute("CALL spatial.addLayer('geom',$type,'lon:lat')", Map.of("type", layerType));
		execute("CALL spatial.addWKT('geom','POINT (15.2 60.1)')");
		testCallCount(db, "CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)", null, 1);
	}

	@ParameterizedTest
	@ValueSource(strings = {"addPointLayerGeohash", "addPointLayerZOrder", "addPointLayerHilbert",
			"addNativePointLayerGeohash", "addNativePointLayerZOrder", "addNativePointLayerHilbert"})
	public void space_filling_curve_layer_counts_and_bounds(String procedure) {
		execute("CALL spatial." + procedure + "('geom')");
		execute("CALL spatial.addWKT('geom','POINT (15.2 60.1)')");
		execute("CALL spatial.addWKT('geom','POINT (16.2 61.1)')");
		testCall(db, "CALL spatial.getFeatureCount('geom')", r -> assertEquals(2L, r.get("count")));
		testCall(db, "CALL spatial.getLayerBoundingBox('geom')", r -> {
			assertEquals(15.2, r.get("minX"));
			assertEquals(60.1, r.get("minY"));
			assertEquals(16.2, r.get("maxX"));
			assertEquals(61.1, r.get("maxY"));
		});
		// distance 0: search radius derived from the layer bounding box
		testCallCount(db, "CALL spatial.closest('geom', {lon: 15.2, lat: 60.1}, 0.0)", null, 1);

		testCall(db, """
				CALL spatial.bbox('geom', {lon: 15.0, lat: 60.0}, {lon: 17.0, lat: 62.0}) YIELD node
				WITH collect(node) AS nodes
				CALL spatial.removeNodes('geom', nodes) YIELD count
				RETURN count""", r -> assertEquals(2L, r.get("count")));
		testCall(db, "CALL spatial.getFeatureCount('geom')", r -> assertEquals(0L, r.get("count")));
		// empty layer: closest must not derive a radius from the (absent) bounding box
		testCallCount(db, "CALL spatial.closest('geom', {lon: 15.2, lat: 60.1}, 0.0)", null, 0);
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
		// the line is about 157 km away
		testCallCount(db, "CALL spatial.closest('line',{lon:5.1, lat:4.1}, 200.0)", null, 1);
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

	@ParameterizedTest
	@ValueSource(strings = {"addPointLayer", "addPointLayerGeohash", "addPointLayerZOrder", "addPointLayerHilbert",
			"addNativePointLayer", "addNativePointLayerGeohash", "addNativePointLayerZOrder",
			"addNativePointLayerHilbert"})
	public void point_layer_meta(String procedure) {
		execute("CALL spatial." + procedure + "('geom')");
		// point layers know their type without searching the index, also when empty
		testCall(db, "CALL spatial.layerMeta('geom')",
				r -> assertEquals("org.locationtech.jts.geom.Point", r.get("geometryType")));
		execute("CALL spatial.addWKT('geom','POINT (15.2 60.1)')");
		testCall(db, "CALL spatial.layerMeta('geom')",
				r -> assertEquals("org.locationtech.jts.geom.Point", r.get("geometryType")));
	}

	@ParameterizedTest
	@ValueSource(strings = {"addPointLayer", "addPointLayerGeohash", "addPointLayerZOrder", "addPointLayerHilbert",
			"addNativePointLayer", "addNativePointLayerGeohash", "addNativePointLayerZOrder",
			"addNativePointLayerHilbert"})
	public void cql_on_point_layers(String procedure) {
		execute("CALL spatial." + procedure + "('geom')");
		execute("CALL spatial.addWKT('geom','POINT (15.2 60.1)')");
		execute("CALL spatial.addWKT('geom','POINT (16.2 61.1)')");
		testCallCount(db, "CALL spatial.cql('geom', 'BBOX(the_geom, 15, 60, 16, 61)')", null, 1);
		// no bounding envelope can be derived from these filters
		testCallCount(db, "CALL spatial.cql('geom', 'BBOX(the_geom, 15, 60, 15.5, 60.5) OR "
				+ "BBOX(the_geom, 16, 61, 16.5, 61.5)')", null, 2);
		testCallCount(db, "CALL spatial.cql('geom', 'NOT BBOX(the_geom, 15, 60, 16, 61)')", null, 1);
		testCallCount(db, "CALL spatial.cql('geom', 'INCLUDE')", null, 2);
	}

	@ParameterizedTest
	@ValueSource(strings = {"addPointLayer", "addPointLayerGeohash", "addPointLayerZOrder", "addPointLayerHilbert",
			"addNativePointLayer", "addNativePointLayerGeohash", "addNativePointLayerZOrder",
			"addNativePointLayerHilbert"})
	public void empty_point_layer_has_no_bounding_box(String procedure) {
		execute("CALL spatial." + procedure + "('geom')");
		testCallCount(db, "CALL spatial.getLayerBoundingBox('geom')", null, 0);
		execute("""
				CALL spatial.addWKT('geom','POINT (15.2 60.1)') YIELD node
				CALL spatial.removeNode('geom', node) YIELD nodeId
				RETURN nodeId""");
		testCallCount(db, "CALL spatial.getLayerBoundingBox('geom')", null, 0);
	}

	@ParameterizedTest
	@ValueSource(strings = {"addPointLayerGeohash", "addPointLayerZOrder", "addPointLayerHilbert"})
	public void index_property_is_no_feature_attribute(String procedure) {
		execute("CALL spatial." + procedure + "('geom')");
		execute("CALL spatial.addWKT('geom','POINT (15.2 60.1)')");
		testCall(db, "CALL spatial.layerMeta('geom')", r -> assertEquals(Map.of(), r.get("extraAttributes")));
		// stores written since 2025.07 list it on the layer node
		execute("""
				MATCH (l:SpatialLayer {layer: 'geom'}), (n)
				UNWIND [k IN keys(n) WHERE k STARTS WITH '_spatialindex_'] AS key
				SET l['extraProp.' + key] = 'java.lang.String'""");
		testCall(db, "CALL spatial.layerMeta('geom')", r -> assertEquals(Map.of(), r.get("extraAttributes")));
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

	@ParameterizedTest
	@ValueSource(strings = {
			"POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
			"MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((5 5, 6 5, 6 6, 5 6, 5 5)))",
			"MULTILINESTRING ((0 0, 1 1), (0 1, 1 0))"
	})
	public void closest_finds_any_geometry_type(String wkt) {
		execute("CALL spatial.addWKTLayer('geom','wkt')");
		execute("CALL spatial.addWKT('geom',$wkt)", Map.of("wkt", wkt));
		testCallCount(db, "CALL spatial.closest('geom',{lon:0.5,lat:0.5},10)", null, 1);
	}

	@Test
	public void closest_measures_distance_to_the_polygon_edge() {
		execute("CALL spatial.addWKTLayer('geom','wkt')");
		execute("CALL spatial.addWKT('geom','POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))')");
		// 0.05 degrees east of the edge at the equator is about 5.6 km
		testCallCount(db, "CALL spatial.closest('geom',{lon:1.05,lat:0.5},10)", null, 1);
		testCallCount(db, "CALL spatial.closest('geom',{lon:1.05,lat:0.5},5)", null, 0);
	}

	@Test
	public void closest_takes_the_distance_in_km() {
		execute("CALL spatial.addWKTLayer('geom','wkt')");
		execute("CALL spatial.addWKTs('geom',['POINT (15.2 60.1)','POINT (15.0 60.05)'])");
		// the points are about 15.6 km and 5.6 km away
		testCallCount(db, "CALL spatial.closest('geom',{lon:15.0,lat:60.0},1)", null, 0);
		testResult(db, "CALL spatial.closest('geom',{lon:15.0,lat:60.0},10) YIELD node RETURN node.wkt AS wkt",
				res -> assertEquals(List.of("POINT (15 60.05)"), res.stream().map(r -> r.get("wkt")).toList()));
		testResult(db, "CALL spatial.closest('geom',{lon:15.0,lat:60.0},20) YIELD node RETURN node.wkt AS wkt",
				res -> assertEquals(List.of("POINT (15 60.05)", "POINT (15.2 60.1)"),
						res.stream().map(r -> r.get("wkt")).toList()));
	}
}
