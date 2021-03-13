/*
 * Copyright (c) 2010-2020 "Neo4j,"
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

import org.junit.*;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.gis.spatial.Constants.*;

public class SpatialProceduresTest {
    private DatabaseManagementService databases;
    private GraphDatabaseService db;

    @Before
    public void setUp() throws KernelException {
        databases = new TestDatabaseManagementServiceBuilder(new File("target/procedures")).setConfig(GraphDatabaseSettings.procedure_unrestricted, List.of("spatial.*")).impermanent().build();
        db = databases.database(DEFAULT_DATABASE_NAME);
        registerProceduresAndFunctions(db, SpatialProcedures.class);
    }

    @After
    public void tearDown() {
        databases.shutdown();
    }

    public static void testCall(GraphDatabaseService db, String call, Consumer<Map<String, Object>> consumer) {
        testCall(db, call, null, consumer);
    }

    public static Map<String, Object> map(Object... values) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put(values[i].toString(), values[i + 1]);
        }
        return map;
    }

    public static void testCall(GraphDatabaseService db, String call, Map<String, Object> params, Consumer<Map<String, Object>> consumer) {
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
            assertThat(e.getMessage(), containsString(error));
        }
    }

    public static void testCall(GraphDatabaseService db, String call, Map<String, Object> params, Consumer<Map<String, Object>> consumer, boolean onlyOne) {
        testResult(db, call, params, (res) -> {
            Assert.assertTrue("Expect at least one result but got none: " + call, res.hasNext());
            Map<String, Object> row = res.next();
            consumer.accept(row);
            if (onlyOne) {
                Assert.assertFalse("Expected only one result, but there are more", res.hasNext());
            }
        });
    }

    public static void testCallCount(GraphDatabaseService db, String call, Map<String, Object> params, int count) {
        testResult(db, call, params, (res) -> {
            int numLeft = count;
            while (numLeft > 0) {
                assertTrue("Expected " + count + " results but found only " + (count - numLeft), res.hasNext());
                res.next();
                numLeft--;
            }
            Assert.assertFalse("Expected " + count + " results but there are more", res.hasNext());
        });
    }

    public static void testResult(GraphDatabaseService db, String call, Consumer<Result> resultConsumer) {
        testResult(db, call, null, resultConsumer);
    }

    public static void testResult(GraphDatabaseService db, String call, Map<String, Object> params, Consumer<Result> resultConsumer) {
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> p = (params == null) ? map() : params;
            resultConsumer.accept(tx.execute(call, p));
            tx.commit();
        }
    }

    public static void registerProceduresAndFunctions(GraphDatabaseService db, Class<?> procedure) throws KernelException {
        GlobalProcedures procedures = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(GlobalProcedures.class);
        procedures.registerProcedure(procedure);
        procedures.registerFunction(procedure);
    }

    @Test
    public void add_node_to_non_existing_layer() {
        execute("CALL spatial.addPointLayer('some_name')");
        Node node = createNode("CREATE (n:Point {latitude:60.1,longitude:15.2}) RETURN n", "n");
        testCallFails(db, "CALL spatial.addNode.byId('wrong_name',$nodeId)", map("nodeId", node.getId()), "No such layer 'wrong_name'");
    }

    @Test
    public void add_node_point_layer() {
        execute("CALL spatial.addPointLayer('points')");
        executeWrite("CREATE (n:Point {latitude:60.1,longitude:15.2})");
        Node node = createNode("MATCH (n:Point) WITH n CALL spatial.addNode('points',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.bbox('points',{longitude:15.0,latitude:60.0},{longitude:15.3, latitude:60.2})", r -> assertEquals(node, r.get("node")));
        testCall(db, "CALL spatial.withinDistance('points',{longitude:15.0,latitude:60.0},100)", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void add_node_and_search_bbox_and_distance() {
        execute("CALL spatial.addPointLayerXY('geom','lon','lat')");
        Node node = createNode("CREATE (n:Node {lat:60.1,lon:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0},{lon:15.3, lat:60.2})", r -> assertEquals(node, r.get("node")));
        testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", r -> assertEquals(node, r.get("node")));
    }

    @Test
    // This tests issue https://github.com/neo4j-contrib/spatial/issues/298
    public void add_node_point_layer_and_search_multiple_points_precision() {
        execute("CALL spatial.addPointLayer('bar')");
        execute("create (n:Point) set n={latitude: 52.2029252, longitude: 0.0905302} with n call spatial.addNode('bar', n) yield node return node");
        execute("create (n:Point) set n={latitude: 52.202925, longitude: 0.090530} with n call spatial.addNode('bar', n) yield node return node");
//        long countLow = execute("call spatial.withinDistance('bar', {latitude:52.202925,longitude:0.0905302}, 100) YIELD node RETURN node");
//        assertThat("Expected two nodes when using low precision", countLow, equalTo(2L));
        long countHigh = execute("call spatial.withinDistance('bar', {latitude:52.2029252,longitude:0.0905302}, 100) YIELD node RETURN node");
        assertThat("Expected two nodes when using high precision", countHigh, equalTo(2L));
    }

    @Test
    public void add_node_and_search_bbox_and_distance_geohash() {
        execute("CALL spatial.addPointLayerGeohash('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0},{lon:15.3, lat:60.2})", r -> assertEquals(node, r.get("node")));
        testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void add_node_and_search_bbox_and_distance_zorder() {
        execute("CALL spatial.addPointLayerZOrder('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0},{lon:15.3, lat:60.2})", r -> assertEquals(node, r.get("node")));
        testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void add_node_and_search_bbox_and_distance_hilbert() {
        execute("CALL spatial.addPointLayerHilbert('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0},{lon:15.3, lat:60.2})", r -> assertEquals(node, r.get("node")));
        testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", r -> assertEquals(node, r.get("node")));
    }

    @Test
    // This tests issue https://github.com/neo4j-contrib/spatial/issues/298
    public void add_node_point_layer_and_search_multiple_points_precision_geohash() {
        execute("CALL spatial.addPointLayerGeohash('bar')");
        execute("create (n:Point) set n={latitude: 52.2029252, longitude: 0.0905302} with n call spatial.addNode('bar', n) yield node return node");
        execute("create (n:Point) set n={latitude: 52.202925, longitude: 0.090530} with n call spatial.addNode('bar', n) yield node return node");
//        long countLow = execute("call spatial.withinDistance('bar', {latitude:52.202925,longitude:0.0905302}, 100) YIELD node RETURN node");
//        assertThat("Expected two nodes when using low precision", countLow, equalTo(2L));
        long countHigh = execute("call spatial.withinDistance('bar', {latitude:52.2029252,longitude:0.0905302}, 100) YIELD node RETURN node");
        assertThat("Expected two nodes when using high precision", countHigh, equalTo(2L));
    }

    //
    // Testing interaction between Neo4j Spatial and the Neo4j 3.0 Point type (point() and distance() functions)
    //

    @Test
    public void create_point_and_distance() {
        double distance = (Double) executeObject("WITH point({latitude: 5.0, longitude: 4.0}) as geometry RETURN distance(geometry, point({latitude: 5.0, longitude: 4.0})) as distance", "distance");
        System.out.println(distance);
    }

    @Test
    // TODO: Support this once procedures are able to return Geometry types
    public void create_point_geometry_and_distance() {
        double distance = (double) executeObject("WITH point({latitude: 5.0, longitude: 4.0}) as geom WITH spatial.asGeometry(geom) AS geometry RETURN distance(geometry, point({latitude: 5.0, longitude: 4.0})) as distance", "distance");
        System.out.println(distance);
    }

    @Test
    public void create_point_and_return() {
        Object geometry = executeObject("RETURN point({latitude: 5.0, longitude: 4.0}) as geometry", "geometry");
        assertThat("Should be Geometry type", geometry, instanceOf(Geometry.class));
    }

    @Test
    public void create_point_geometry_return() {
        Object geometry = executeObject("WITH point({latitude: 5.0, longitude: 4.0}) as geom RETURN spatial.asGeometry(geom) AS geometry", "geometry");
        assertThat("Should be Geometry type", geometry, instanceOf(Geometry.class));
    }

    @Test
    public void literal_geometry_return() {
        Object geometry = executeObject("WITH spatial.asGeometry({latitude: 5.0, longitude: 4.0}) AS geometry RETURN geometry", "geometry");
        assertThat("Should be Geometry type", geometry, instanceOf(Geometry.class));
    }

    @Test
    public void create_node_decode_to_geometry() {
        execute("CALL spatial.addWKTLayer('geom','geom')");
        Object actual = executeObject("CREATE (n:Node {geom:'POINT(4.0 5.0)'}) RETURN spatial.decodeGeometry('geom',n) AS geometry", "geometry");
        assertThat("Should be Geometry type", actual, instanceOf(Geometry.class));
    }

    @Test
    // TODO: Currently this only works for point geometries because Neo4k 3.4 can only return Point geometries from procedures
    public void create_node_and_convert_to_geometry() {
        execute("CALL spatial.addWKTLayer('geom','geom')");
        Geometry geom = (Geometry) executeObject("CREATE (n:Node {geom:'POINT(4.0 5.0)'}) RETURN spatial.decodeGeometry('geom',n) AS geometry", "geometry");
        double distance = (Double) executeObject("RETURN distance($geom, point({y: 6.0, x: 4.0})) as distance", map("geom", geom), "distance");
        assertThat("Expected the cartesian distance of 1.0", distance, closeTo(1.0, 0.00001));
    }

    @Test
    // TODO: Currently this only works for point geometries because Neo4j 3.4 can only return Point geometries from procedures
    public void create_point_and_pass_as_param() {
        Geometry geom = (Geometry) executeObject("RETURN point({latitude: 5.0, longitude: 4.0}) as geometry", "geometry");
        double distance = (Double) executeObject("WITH spatial.asGeometry($geom) AS geometry RETURN distance(geometry, point({latitude: 5.1, longitude: 4.0})) as distance", map("geom", geom), "distance");
        assertThat("Expected the geographic distance of 11132km", distance, closeTo(11132.0, 1.0));
    }

    private long execute(String statement) {
        try (Transaction tx = db.beginTx()) {
            long count = Iterators.count(tx.execute(statement));
            tx.commit();
            return count;
        }
    }

    private long execute(String statement, Map<String, Object> params) {
        try (Transaction tx = db.beginTx()) {
            long count = Iterators.count(tx.execute(statement, params));
            tx.commit();
            return count;
        }
    }

    private void executeWrite(String call) {
        try (Transaction tx = db.beginTx()) {
            tx.execute(call).accept(v -> true);
            tx.commit();
        }
    }

    private Node createNode(String call, String column) {
        Node node;
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Object> nodes = tx.execute(call).columnAs(column);
            node = (Node) nodes.next();
            nodes.close();
            tx.commit();
        }
        return node;
    }

    private Object executeObject(String call, String column) {
        Object obj;
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Object> values = tx.execute(call).columnAs(column);
            obj = values.next();
            values.close();
            tx.commit();
        }
        return obj;
    }

    private Object executeObject(String call, Map<String, Object> params, String column) {
        Object obj;
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> p = (params == null) ? map() : params;
            ResourceIterator<Object> values = tx.execute(call, p).columnAs(column);
            obj = values.next();
            values.close();
            tx.commit();
        }
        return obj;
    }

    @Test
    public void create_a_pointlayer_with_x_and_y() {
        testCall(db, "CALL spatial.addPointLayerXY('geom','lon','lat')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
    }

    @Test
    public void create_a_pointlayer_with_config() {
        testCall(db, "CALL spatial.addPointLayerWithConfig('geom','lon:lat')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
    }

    @Test
    public void create_a_pointlayer_with_config_on_existing_wkt_layer() {
        execute("CALL spatial.addWKTLayer('geom','wkt')");
        try {
            testCall(db, "CALL spatial.addPointLayerWithConfig('geom','lon:lat')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
            fail("Expected exception to be thrown");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Cannot create existing layer"));
        }
    }

    @Test
    public void create_a_pointlayer_with_config_on_existing_osm_layer() {
        execute("CALL spatial.addLayer('geom','OSM','')");
        try {
            testCall(db, "CALL spatial.addPointLayerWithConfig('geom','lon:lat')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
            fail("Expected exception to be thrown");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Cannot create existing layer"));
        }
    }

    @Test
    public void create_a_pointlayer_with_rtree() {
        testCall(db, "CALL spatial.addPointLayer('geom')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
    }

    @Test
    public void create_a_pointlayer_with_geohash() {
        testCall(db, "CALL spatial.addPointLayerGeohash('geom')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
    }

    @Test
    public void create_a_pointlayer_with_zorder() {
        testCall(db, "CALL spatial.addPointLayerZOrder('geom')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
    }

    @Test
    public void create_a_pointlayer_with_hilbert() {
        testCall(db, "CALL spatial.addPointLayerHilbert('geom')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
    }

    @Test
    public void create_and_delete_a_pointlayer_with_rtree() {
        testCall(db, "CALL spatial.addPointLayer('geom')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
        testCallCount(db, "CALL spatial.layers()", null, 1);
        execute("CALL spatial.removeLayer('geom')");
        testCallCount(db, "CALL spatial.layers()", null, 0);
    }

    @Test
    public void create_and_delete_a_pointlayer_with_geohash() {
        testCall(db, "CALL spatial.addPointLayerGeohash('geom')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
        testCallCount(db, "CALL spatial.layers()", null, 1);
        execute("CALL spatial.removeLayer('geom')");
        testCallCount(db, "CALL spatial.layers()", null, 0);
    }

    @Test
    public void create_and_delete_a_pointlayer_with_zorder() {
        testCall(db, "CALL spatial.addPointLayerZOrder('geom')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
        testCallCount(db, "CALL spatial.layers()", null, 1);
        execute("CALL spatial.removeLayer('geom')");
        testCallCount(db, "CALL spatial.layers()", null, 0);
    }

    @Test
    public void create_and_delete_a_pointlayer_with_hilbert() {
        testCall(db, "CALL spatial.addPointLayerHilbert('geom')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
        testCallCount(db, "CALL spatial.layers()", null, 1);
        execute("CALL spatial.removeLayer('geom')");
        testCallCount(db, "CALL spatial.layers()", null, 0);
    }

    @Test
    public void create_a_simple_pointlayer_using_named_encoder() {
        testCall(db, "CALL spatial.addLayerWithEncoder('geom','SimplePointEncoder','')", (r) -> {
            Node node = dump((Node) r.get("node"));
            assertEquals("geom", node.getProperty("layer"));
            assertEquals("org.neo4j.gis.spatial.encoders.SimplePointEncoder", node.getProperty("geomencoder"));
            assertEquals("org.neo4j.gis.spatial.SimplePointLayer", node.getProperty("layer_class"));
            assertFalse(node.hasProperty(PROP_GEOMENCODER_CONFIG));
        });
    }

    @Test
    public void create_a_simple_pointlayer_using_named_and_configured_encoder() {
        testCall(db, "CALL spatial.addLayerWithEncoder('geom','SimplePointEncoder','x:y:mbr')", (r) -> {
            Node node = dump((Node) r.get("node"));
            assertEquals("geom", node.getProperty(PROP_LAYER));
            assertEquals("org.neo4j.gis.spatial.encoders.SimplePointEncoder", node.getProperty(PROP_GEOMENCODER));
            assertEquals("org.neo4j.gis.spatial.SimplePointLayer", node.getProperty(PROP_LAYER_CLASS));
            assertEquals("x:y:mbr", node.getProperty(PROP_GEOMENCODER_CONFIG));
        });
    }

    @Test
    public void create_a_native_pointlayer_using_named_encoder() {
        testCall(db, "CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','')", (r) -> {
            Node node = dump((Node) r.get("node"));
            assertEquals("geom", node.getProperty(PROP_LAYER));
            assertEquals("org.neo4j.gis.spatial.encoders.NativePointEncoder", node.getProperty(PROP_GEOMENCODER));
            assertEquals("org.neo4j.gis.spatial.SimplePointLayer", node.getProperty(PROP_LAYER_CLASS));
            assertFalse(node.hasProperty(PROP_GEOMENCODER_CONFIG));
        });
    }

    @Test
    public void create_a_native_pointlayer_using_named_and_configured_encoder() {
        testCall(db, "CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','pos:mbr')", (r) -> {
            Node node = dump((Node) r.get("node"));
            assertEquals("geom", node.getProperty(PROP_LAYER));
            assertEquals("org.neo4j.gis.spatial.encoders.NativePointEncoder", node.getProperty(PROP_GEOMENCODER));
            assertEquals("org.neo4j.gis.spatial.SimplePointLayer", node.getProperty(PROP_LAYER_CLASS));
            assertEquals("pos:mbr", node.getProperty(PROP_GEOMENCODER_CONFIG));
        });
    }

    @Test
    public void create_a_native_pointlayer_using_named_and_configured_encoder_with_cartesian() {
        testCall(db, "CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','pos:mbr:Cartesian')", (r) -> {
            Node node = dump((Node) r.get("node"));
            assertEquals("geom", node.getProperty(PROP_LAYER));
            assertEquals("org.neo4j.gis.spatial.encoders.NativePointEncoder", node.getProperty(PROP_GEOMENCODER));
            assertEquals("org.neo4j.gis.spatial.SimplePointLayer", node.getProperty(PROP_LAYER_CLASS));
            assertEquals("pos:mbr:Cartesian", node.getProperty(PROP_GEOMENCODER_CONFIG));
        });
    }

    @Test
    public void create_a_native_pointlayer_using_named_and_configured_encoder_with_geographic() {
        testCall(db, "CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','pos:mbr:WGS-84')", (r) -> {
            Node node = dump((Node) r.get("node"));
            assertEquals("geom", node.getProperty(PROP_LAYER));
            assertEquals("org.neo4j.gis.spatial.encoders.NativePointEncoder", node.getProperty(PROP_GEOMENCODER));
            assertEquals("org.neo4j.gis.spatial.SimplePointLayer", node.getProperty(PROP_LAYER_CLASS));
            assertEquals("pos:mbr:WGS-84", node.getProperty(PROP_GEOMENCODER_CONFIG));
        });
    }

    @Test
    public void create_a_wkt_layer_using_know_format() {
        testCall(db, "CALL spatial.addLayer('geom','WKT',null)", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
    }

    @Test
    public void list_layer_names() {
        String wkt = "LINESTRING (15.2 60.1, 15.3 60.1)";
        execute("CALL spatial.addWKTLayer('geom','wkt')");
        execute("CALL spatial.addWKT('geom',$wkt)", map("wkt", wkt));

        testCall(db, "CALL spatial.layers()", (r) -> {
            assertEquals("geom", r.get("name"));
            assertEquals("EditableLayer(name='geom', encoder=WKTGeometryEncoder(geom='wkt', bbox='bbox'))", r.get("signature"));
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
            execute("CALL spatial.addWKTLayer($layerName,'wkt')", map("layerName", name));
            execute("CALL spatial.addWKT($layerName,$wkt)", map("wkt", wkt, "layerName", name));
            testCallCount(db, "CALL spatial.layers()", null, i + 1);
        }
        for (int i = 0; i < NUM_LAYERS; i++) {
            String name = "wktLayer_" + i;
            testCallCount(db, "CALL spatial.layers()", null, NUM_LAYERS - i);
            execute("CALL spatial.removeLayer($layerName)", map("layerName", name));
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
                System.out.println(key + ": " + procs.get(key));
            }
            assertEquals("spatial.procedures() :: (name :: STRING?, signature :: STRING?)", procs.get("spatial.procedures"));
            assertEquals("spatial.layers() :: (name :: STRING?, signature :: STRING?)", procs.get("spatial.layers"));
            assertEquals("spatial.layer(name :: STRING?) :: (node :: NODE?)", procs.get("spatial.layer"));
            assertEquals("spatial.addLayer(name :: STRING?, type :: STRING?, encoderConfig :: STRING?) :: (node :: NODE?)", procs.get("spatial.addLayer"));
            assertEquals("spatial.addNode(layerName :: STRING?, node :: NODE?) :: (node :: NODE?)", procs.get("spatial.addNode"));
            assertEquals("spatial.addWKT(layerName :: STRING?, geometry :: STRING?) :: (node :: NODE?)", procs.get("spatial.addWKT"));
            assertEquals("spatial.intersects(layerName :: STRING?, geometry :: ANY?) :: (node :: NODE?)", procs.get("spatial.intersects"));
        });
    }

    @Test
    public void list_layer_types() {
        testResult(db, "CALL spatial.layerTypes()", (res) -> {
            Map<String, String> procs = new LinkedHashMap<>();
            while (res.hasNext()) {
                Map<String, Object> r = res.next();
                procs.put(r.get("name").toString(), r.get("signature").toString());
            }
            for (String key : procs.keySet()) {
                System.out.println(key + ": " + procs.get(key));
            }
            assertEquals("RegisteredLayerType(name='SimplePoint', geometryEncoder=SimplePointEncoder, layerClass=SimplePointLayer, index=LayerRTreeIndex, crs='WGS84(DD)', defaultConfig='longitude:latitude')", procs.get("simplepoint"));
            assertEquals("RegisteredLayerType(name='NativePoint', geometryEncoder=NativePointEncoder, layerClass=SimplePointLayer, index=LayerRTreeIndex, crs='WGS84(DD)', defaultConfig='location')", procs.get("nativepoint"));
            assertEquals("RegisteredLayerType(name='WKT', geometryEncoder=WKTGeometryEncoder, layerClass=EditableLayerImpl, index=LayerRTreeIndex, crs='WGS84(DD)', defaultConfig='geometry')", procs.get("wkt"));
            assertEquals("RegisteredLayerType(name='WKB', geometryEncoder=WKBGeometryEncoder, layerClass=EditableLayerImpl, index=LayerRTreeIndex, crs='WGS84(DD)', defaultConfig='geometry')", procs.get("wkb"));
            assertEquals("RegisteredLayerType(name='Geohash', geometryEncoder=SimplePointEncoder, layerClass=SimplePointLayer, index=LayerGeohashPointIndex, crs='WGS84(DD)', defaultConfig='longitude:latitude')", procs.get("geohash"));
            assertEquals("RegisteredLayerType(name='ZOrder', geometryEncoder=SimplePointEncoder, layerClass=SimplePointLayer, index=LayerZOrderPointIndex, crs='WGS84(DD)', defaultConfig='longitude:latitude')", procs.get("zorder"));
            assertEquals("RegisteredLayerType(name='Hilbert', geometryEncoder=SimplePointEncoder, layerClass=SimplePointLayer, index=LayerHilbertPointIndex, crs='WGS84(DD)', defaultConfig='longitude:latitude')", procs.get("hilbert"));
            assertEquals("RegisteredLayerType(name='NativeGeohash', geometryEncoder=NativePointEncoder, layerClass=SimplePointLayer, index=LayerGeohashPointIndex, crs='WGS84(DD)', defaultConfig='location')", procs.get("nativegeohash"));
            assertEquals("RegisteredLayerType(name='NativeZOrder', geometryEncoder=NativePointEncoder, layerClass=SimplePointLayer, index=LayerZOrderPointIndex, crs='WGS84(DD)', defaultConfig='location')", procs.get("nativezorder"));
            assertEquals("RegisteredLayerType(name='NativeHilbert', geometryEncoder=NativePointEncoder, layerClass=SimplePointLayer, index=LayerHilbertPointIndex, crs='WGS84(DD)', defaultConfig='location')", procs.get("nativehilbert"));
        });
    }

    @Test
    public void find_layer() {
        String wkt = "LINESTRING (15.2 60.1, 15.3 60.1)";
        execute("CALL spatial.addWKTLayer('geom','wkt')");
        execute("CALL spatial.addWKT('geom',$wkt)", map("wkt", wkt));

        testCall(db, "CALL spatial.layer('geom')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
        testCallFails(db, "CALL spatial.layer('badname')", null, "No such layer 'badname'");
    }

    @Test
    public void add_a_node_to_the_spatial_rtree_index_for_simple_points() {
        execute("CALL spatial.addPointLayer('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) RETURN n", "n");
        testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", r -> Assert.assertEquals(node, r.get("node")));
    }

    @Test
    public void add_a_node_to_the_spatial_geohash_index_for_simple_points() {
        execute("CALL spatial.addPointLayerGeohash('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) RETURN n", "n");
        testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", r -> Assert.assertEquals(node, r.get("node")));
    }

    @Test
    public void add_a_node_to_the_spatial_zorder_index_for_simple_points() {
        execute("CALL spatial.addPointLayerZOrder('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) RETURN n", "n");
        testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", r -> Assert.assertEquals(node, r.get("node")));
    }

    @Test
    public void add_a_node_to_the_spatial_hilbert_index_for_simple_points() {
        execute("CALL spatial.addPointLayerHilbert('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) RETURN n", "n");
        testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", r -> Assert.assertEquals(node, r.get("node")));
    }

    @Test
    public void add_a_node_to_multiple_different_indexes_for_both_simple_and_native_points() {
        String[] encoders = new String[]{"Simple", "Native"};
        String[] indexes = new String[]{"Geohash", "ZOrder", "Hilbert", "RTree"};
        for (String encoder : encoders) {
            String procName = (encoder.equalsIgnoreCase("Native")) ? "addNativePointLayer" : "addPointLayer";
            for (String indexType : indexes) {
                String layerName = (encoder + indexType).toLowerCase();
                String query = "CALL spatial." + procName + (indexType.equals("RTree") ? "" : indexType) + "('" + layerName + "')";
                execute(query);
            }
        }
        testResult(db, "CALL spatial.layers()", (res) -> {
            while (res.hasNext()) {
                Map<String, Object> r = res.next();
                String encoder = r.get("name").toString().contains("native") ? "NativePointEncoder" : "SimplePointEncoder";
                assertThat("Expect simple:native encoders to appear in simple:native layers", r.get("signature").toString(), containsString(encoder));
            }
        });
        testCallCount(db, "CALL spatial.layers()", null, indexes.length * encoders.length);
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) SET n.location=point(n) RETURN n", "n");
        for (String encoder : encoders) {
            for (String indexType : indexes) {
                String layerName = (encoder + indexType).toLowerCase();
                testCall(db, "MATCH (node:Node) RETURN node", r -> Assert.assertEquals(node, r.get("node")));
                testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('" + layerName + "',n) YIELD node RETURN node", r -> Assert.assertEquals(node, r.get("node")));
                testCall(db, "CALL spatial.withinDistance('" + layerName + "',{lon:15.0,lat:60.0},100)", r -> assertEquals(node, r.get("node")));
            }
        }
        for (String encoder : encoders) {
            for (String indexType : indexes) {
                String layerName = (encoder + indexType).toLowerCase();
                execute("CALL spatial.removeLayer('" + layerName + "')");
            }
        }
        testCallCount(db, "CALL spatial.layers()", null, 0);
    }


    @Test
    public void testDistanceNode() {
        execute("CALL spatial.addPointLayer('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void testDistanceNodeWithGeohashIndex() {
        execute("CALL spatial.addPointLayer('geom','geohash')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void testDistanceNodeGeohash() {
        execute("CALL spatial.addPointLayerGeohash('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void testDistanceNodeZOrder() {
        execute("CALL spatial.addPointLayerZOrder('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void testDistanceNodeHilbert() {
        execute("CALL spatial.addPointLayerHilbert('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void add_a_node_to_the_spatial_index_short() {
        execute("CALL spatial.addPointLayerXY('geom','lon','lat')");
        Node node = createNode("CREATE (n:Node {lat:60.1,lon:15.2}) RETURN n", "n");
        testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", r -> Assert.assertEquals(node, r.get("node")));
    }

    @Test
    public void add_a_node_to_the_spatial_index_short_with_geohash() {
        execute("CALL spatial.addPointLayerXY('geom','lon','lat','geohash')");
        Node node = createNode("CREATE (n:Node {lat:60.1,lon:15.2}) RETURN n", "n");
        testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", r -> Assert.assertEquals(node, r.get("node")));
    }

    @Test
    public void add_two_nodes_to_the_spatial_layer() {
        execute("CALL spatial.addPointLayerXY('geom','lon','lat')");
        long node1;
        long node2;
        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute("CREATE (n1:Node {lat:60.1,lon:15.2}),(n2:Node {lat:60.1,lon:15.3}) WITH n1,n2 CALL spatial.addNodes('geom',[n1,n2]) YIELD count RETURN n1,n2,count");
            Map<String, Object> row = result.next();
            node1 = ((Node) row.get("n1")).getId();
            node2 = ((Node) row.get("n2")).getId();
            long count = (Long) row.get("count");
            Assert.assertEquals(2L, count);
            result.close();
            tx.commit();
        }
        testResult(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", res -> {
            assertTrue(res.hasNext());
            assertEquals(node1, ((Node) res.next().get("node")).getId());
            assertTrue(res.hasNext());
            assertEquals(node2, ((Node) res.next().get("node")).getId());
            assertFalse(res.hasNext());
        });
        try (Transaction tx = db.beginTx()) {
            Node node = (Node) tx.execute("MATCH (node) WHERE id(node) = $nodeId RETURN node", map("nodeId", node1)).next().get("node");
            Result removeResult = tx.execute("CALL spatial.removeNode('geom',$node) YIELD nodeId RETURN nodeId", map("node", node));
            Assert.assertEquals(node1, removeResult.next().get("nodeId"));
            removeResult.close();
            tx.commit();
        }
        testResult(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", res -> {
            assertTrue(res.hasNext());
            assertEquals(node2, ((Node) res.next().get("node")).getId());
            assertFalse(res.hasNext());
        });
        try (Transaction tx = db.beginTx()) {
            Result removeResult = tx.execute("CALL spatial.removeNode.byId('geom',$nodeId) YIELD nodeId RETURN nodeId", map("nodeId", node2));
            Assert.assertEquals(node2, removeResult.next().get("nodeId"));
            removeResult.close();
            tx.commit();
        }
        testResult(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", res -> {
            assertFalse(res.hasNext());
        });
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
        testCountQuery("addNodes", query, count, "count", map("count", count));
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
        testCountQuery("addNode", query, count, "count(node)", map("count", count));
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
        testCountQuery("addNodes", query, count, "count", map("count", count));
        testRemoveNodes("native_poi", count);
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
        testCountQuery("addNode", query, count, "count(node)", map("count", count));
        testRemoveNode("native_poi", count);
    }

    private void testRemoveNode(String layer, int count) {
        // Check all nodes are there
        testCountQuery("withinDistance", "CALL spatial.withinDistance('" + layer + "',{lon:15.0,lat:60.0},1000) YIELD node RETURN count(node)", count, "count(node)", null);
        // Now remove half the points
        String remove = "UNWIND range(1,$count) as i\n" +
                "MATCH (n:Point {id:i})\n" +
                "WITH n\n" +
                "CALL spatial.removeNode('" + layer + "',n) YIELD nodeId\n" +
                "RETURN count(nodeId)";
        testCountQuery("removeNode", remove, count / 2, "count(nodeId)", map("count", count / 2));
        // Check that only half remain
        testCountQuery("withinDistance", "CALL spatial.withinDistance('" + layer + "',{lon:15.0,lat:60.0},1000) YIELD node RETURN count(node)", count / 2, "count(node)", null);
    }

    private void testRemoveNodes(String layer, int count) {
        // Check all nodes are there
        testCountQuery("withinDistance", "CALL spatial.withinDistance('" + layer + "',{lon:15.0,lat:60.0},1000) YIELD node RETURN count(node)", count, "count(node)", null);
        // Now remove half the points
        String remove = "UNWIND range(1,$count) as i\n" +
                "MATCH (n:Point {id:i})\n" +
                "WITH collect(n) as points\n" +
                "CALL spatial.removeNodes('" + layer + "',points) YIELD count\n" +
                "RETURN count";
        testCountQuery("removeNodes", remove, count / 2, "count", map("count", count / 2));
        // Check that only half remain
        testCountQuery("withinDistance", "CALL spatial.withinDistance('" + layer + "',{lon:15.0,lat:60.0},1000) YIELD node RETURN count(node)", count / 2, "count(node)", null);
    }

    @Test
    public void import_shapefile() {
        testCountQuery("importShapefile", "CALL spatial.importShapefile('shp/highway.shp')", 143, "count", null);
        testCallCount(db, "CALL spatial.layers()", null, 1);
    }

    @Test
    public void import_shapefile_without_extension() {
        testCountQuery("importShapefile", "CALL spatial.importShapefile('shp/highway')", 143, "count", null);
        testCallCount(db, "CALL spatial.layers()", null, 1);
    }

    @Test
    public void import_shapefile_to_layer() {
        execute("CALL spatial.addWKTLayer('geom','wkt')");
        testCountQuery("importShapefileToLayer", "CALL spatial.importShapefileToLayer('geom','shp/highway.shp')", 143, "count", null);
        testCallCount(db, "CALL spatial.layers()", null, 1);
    }

    @Test
    public void import_osm() {
        testCountQuery("importOSM", "CALL spatial.importOSM('map.osm')", 55, "count", null);
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
        testCountQuery("importShapefileToLayer", "CALL spatial.importOSMToLayer('geom','map.osm')", 55, "count", null);
        testCallCount(db, "CALL spatial.layers()", null, 1);
    }

    @Ignore
    public void import_cracow_to_layer() {
        execute("CALL spatial.addLayer('geom','OSM','')");
        testCountQuery("importCracowToLayer", "CALL spatial.importOSMToLayer('geom','issue-347/cra.osm')", 256253, "count", null);
        testCallCount(db, "CALL spatial.layers()", null, 1);
    }

    @Test
    public void import_osm_to_layer_without_changesets() {
        execute("CALL spatial.addLayer('osm_example','OSM','')");
        testCountQuery("importOSMToLayerWithoutChangesets", "CALL spatial.importOSMToLayer('osm_example','sample.osm')", 1, "count", null);
        testCallCount(db, "CALL spatial.layers()", null, 1);
    }

    @Test
    public void import_osm_and_add_geometry() {
        execute("CALL spatial.addLayer('geom','OSM','')");
        testCountQuery("importOSMToLayerAndAddGeometry", "CALL spatial.importOSMToLayer('geom','map.osm')", 55, "count", null);
        testCallCount(db, "CALL spatial.layers()", null, 1);
        testCallCount(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},100)", null, 0);
        testCallCount(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},10000)", null, 217);

        // Adding a point to the layer
        Node node = createNode("CALL spatial.addWKT('geom', 'POINT(6.3740429666 50.93676351666)') YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},100)", r -> assertEquals(node, r.get("node")));
        testCallCount(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},100)", null, 1);
        testCallCount(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},10000)", null, 218);
    }

    @Test
    public void import_osm_and_polygons_withinDistance() {
        Map<String, Object> params = map("osmFile", "withinDistance.osm", "busShelterID", 2938842290L);
        execute("CALL spatial.addLayer('geom','OSM','')");
        testCountQuery("importOSMAndPolygonsWithinDistance", "CALL spatial.importOSMToLayer('geom',$osmFile)", 74, "count", params);
        testCallCount(db, "CALL spatial.layers()", null, 1);
        testCallCount(db, "MATCH (n) WHERE n.node_osm_id = $busShelterID CALL spatial.withinDistance('geom',n,100) YIELD node, distance RETURN node, distance", params, 516);
        testResult(db, "MATCH (n) WHERE n.node_osm_id = $busShelterID CALL spatial.withinDistance('geom',n,100) YIELD node, distance WITH node, distance ORDER BY distance LIMIT 20 MATCH (node)<-[:GEOM]-(osmNode) RETURN node, distance, osmNode, properties(osmNode) as props",
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
                        Map<String,Object> props = (Map) r.get("props");
                        System.out.println("(node[" + node.getId() + "])<-[:GEOM {distance:" + distance + "}]-(osmNode[" + osmNode.getId() + "] " + props + ") ");
                        assertThat("Node should have either way_osm_id or node_osm_id", props, anyOf(hasKey("node_osm_id"), hasKey("way_osm_id")));
                    }
                });
        testResult(db, "MATCH (n) WHERE n.node_osm_id = $busShelterID CALL spatial.withinDistance('geom',n,100) YIELD node, distance WITH node, distance ORDER BY distance LIMIT 20 MATCH (n) WHERE id(n)=id(node) RETURN node, distance, spatial.decodeGeometry('geom',n) AS geometry",
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
                            assertThat("Point has 2D coordinates", ((Point) geometry).getCoordinate().getCoordinate().size(), equalTo(2));
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
            Result results = tx.execute("EXPLAIN " + query, params == null ? map() : params);
            results.close();
            tx.commit();
        }
        long start = System.currentTimeMillis();
        testResult(db, query, params, res -> {
                    assertTrue("Expected a single result", res.hasNext());
                    long c = (Long) res.next().get(column);
                    assertFalse("Expected a single result", res.hasNext());
                    Assert.assertEquals("Expected count of " + count + " nodes but got " + c, count, c);
                }
        );
        System.out.println(name + " query took " + (System.currentTimeMillis() - start) + "ms - " + params);
    }

    @Test
    public void find_geometries_in_a_bounding_box_short() {
        execute("CALL spatial.addPointLayerXY('geom','lon','lat')");
        Node node = createNode("CREATE (n:Node {lat:60.1,lon:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void find_geometries_in_a_bounding_box() {
        execute("CALL spatial.addPointLayer('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void find_geometries_in_a_polygon() {
        execute("CALL spatial.addPointLayer('geom')");
        executeWrite("UNWIND [{name:'a',latitude:60.1,longitude:15.2},{name:'b',latitude:60.3,longitude:15.5}] as point CREATE (n:Node) SET n += point WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node.name as name");
        String polygon = "POLYGON((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2))";
        testCall(db, "CALL spatial.intersects('geom','" + polygon + "') YIELD node RETURN node.name as name", r -> assertEquals("b", r.get("name")));
    }

    @Test
    public void find_geometries_in_a_bounding_box_geohash() {
        execute("CALL spatial.addPointLayerGeohash('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void find_geometries_in_a_polygon_geohash() {
        execute("CALL spatial.addPointLayerGeohash('geom')");
        executeWrite("UNWIND [{name:'a',latitude:60.1,longitude:15.2},{name:'b',latitude:60.3,longitude:15.5}] as point CREATE (n:Node) SET n += point WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node.name as name");
        String polygon = "POLYGON((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2))";
        testCall(db, "CALL spatial.intersects('geom','" + polygon + "') YIELD node RETURN node.name as name", r -> assertEquals("b", r.get("name")));
    }

    @Test
    public void find_geometries_in_a_bounding_box_zorder() {
        execute("CALL spatial.addPointLayerZOrder('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void find_geometries_in_a_polygon_zorder() {
        execute("CALL spatial.addPointLayerZOrder('geom')");
        executeWrite("UNWIND [{name:'a',latitude:60.1,longitude:15.2},{name:'b',latitude:60.3,longitude:15.5}] as point CREATE (n:Node) SET n += point WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node.name as name");
        String polygon = "POLYGON((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2))";
        testCall(db, "CALL spatial.intersects('geom','" + polygon + "') YIELD node RETURN node.name as name", r -> assertEquals("b", r.get("name")));
    }

    @Test
    public void find_geometries_in_a_bounding_box_hilbert() {
        execute("CALL spatial.addPointLayerHilbert('geom')");
        Node node = createNode("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", "node");
        testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void find_geometries_in_a_polygon_hilbert() {
        execute("CALL spatial.addPointLayerHilbert('geom')");
        executeWrite("UNWIND [{name:'a',latitude:60.1,longitude:15.2},{name:'b',latitude:60.3,longitude:15.5}] as point CREATE (n:Node) SET n += point WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node.name as name");
        String polygon = "POLYGON((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2))";
        testCall(db, "CALL spatial.intersects('geom','" + polygon + "') YIELD node RETURN node.name as name", r -> assertEquals("b", r.get("name")));
    }

    @Test
    public void create_a_WKT_layer() {
        testCall(db, "CALL spatial.addWKTLayer('geom','wkt')", r -> assertEquals("wkt", dump(((Node) r.get("node"))).getProperty("geomencoder_config")));
    }

    private static Node dump(Node n) {
        System.out.printf("id %d props %s%n", n.getId(), n.getAllProperties());
        System.out.flush();
        return n;
    }

    @Test
    public void add_a_WKT_geometry_to_a_layer() {
        String lineString = "LINESTRING (15.2 60.1, 15.3 60.1)";

        execute("CALL spatial.addWKTLayer('geom','wkt')");
        testCall(db, "CALL spatial.addWKT('geom',$wkt)", map("wkt", lineString),
                r -> assertEquals(lineString, dump(((Node) r.get("node"))).getProperty("wkt")));
    }

    @Test
    public void find_geometries_close_to_a_point_wkt() {
        String lineString = "LINESTRING (15.2 60.1, 15.3 60.1)";
        execute("CALL spatial.addLayer('geom','WKT','wkt')");
        execute("CALL spatial.addWKT('geom',$wkt)", map("wkt", lineString));
        testCall(db, "CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)", r -> assertEquals(lineString, (dump((Node) r.get("node"))).getProperty("wkt")));
    }

    @Test
    public void find_geometries_close_to_a_point_geohash() {
        String lineString = "POINT (15.2 60.1)";
        execute("CALL spatial.addLayer('geom','geohash','lon:lat')");
        execute("CALL spatial.addWKT('geom',$wkt)", map("wkt", lineString));
        testCallCount(db, "CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)", null, 1);
    }

    @Test
    public void find_geometries_close_to_a_point_zorder() {
        String lineString = "POINT (15.2 60.1)";
        execute("CALL spatial.addLayer('geom','zorder','lon:lat')");
        execute("CALL spatial.addWKT('geom',$wkt)", map("wkt", lineString));
        testCallCount(db, "CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)", null, 1);
    }

    @Test
    public void find_geometries_close_to_a_point_hilbert() {
        String lineString = "POINT (15.2 60.1)";
        execute("CALL spatial.addLayer('geom','hilbert','lon:lat')");
        execute("CALL spatial.addWKT('geom',$wkt)", map("wkt", lineString));
        testCallCount(db, "CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)", null, 1);
    }

    @Test
    public void find_no_geometries_using_closest_on_empty_layer() {
        execute("CALL spatial.addLayer('geom','WKT','wkt')");
        testCallCount(db, "CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)", null, 0);
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
