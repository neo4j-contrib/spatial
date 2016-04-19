/**
 * Copyright (c) 2010-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j.
 * <p>
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.procedures;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import static junit.framework.Assert.assertEquals;

public class SpatialProceduresTest {
    private GraphDatabaseService db;

    @Before
    public void setUp() throws KernelException {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        registerProcedure(db, SpatialProcedures.class);
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
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
        testResult(db, call, params, (res) -> {
            if (res.hasNext()) {
                Map<String, Object> row = res.next();
                consumer.accept(row);
            }
            Assert.assertFalse(res.hasNext());
        });
    }

    public static void testResult(GraphDatabaseService db, String call, Consumer<Result> resultConsumer) {
        testResult(db, call, null, resultConsumer);
    }

    public static void testResult(GraphDatabaseService db, String call, Map<String, Object> params, Consumer<Result> resultConsumer) {
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> p = (params == null) ? map() : params;
            resultConsumer.accept(db.execute(call, p));
            tx.success();
        }
    }

    public static void registerProcedure(GraphDatabaseService db, Class<?> procedure) throws KernelException {
        ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class).register(procedure);
    }

    @Test
    public void querying_with_cypher() {
        execute("CALL spatial.addPointLayerNamed('geom','lat','lon')");
        ResourceIterator<Object> nodes = db.execute("CREATE (n:Node {lat:60.1,lon:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node").columnAs("node");
        Node node = (Node) nodes.next();
        nodes.close();
        testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0},{lon:15.3, lat:60.2})", r -> assertEquals(node, r.get("node")));
        testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", r -> assertEquals(node, r.get("node")));
    }

    private void execute(String statement) {
        Iterators.count(db.execute(statement));
    }
    private void execute(String statement,Map<String,Object> params) {
        Iterators.count(db.execute(statement,params));
    }

    @Test
    public void create_a_pointlayer_named() {
        testCall(db, "CALL spatial.addPointLayerNamed('geom','lat','lon')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
    }

    @Test
    public void create_a_pointlayer() {
        testCall(db, "CALL spatial.addPointLayer('geom')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
    }

    @Test
    public void list_layer_names() {
        String wkt = "LINESTRING (15.2 60.1, 15.3 60.1)";
        execute( "CALL spatial.addWKTLayer('geom','wkt')" );
        execute( "CALL spatial.addWKT('geom',{wkt})", map( "wkt", wkt ) );

        testCall( db, "CALL spatial.layers()", ( r ) -> assertEquals( "geom", (String) r.get( "name" ) ) );
    }

    @Test
    public void find_layer() {
        String wkt = "LINESTRING (15.2 60.1, 15.3 60.1)";
        execute("CALL spatial.addWKTLayer('geom','wkt')");
        execute("CALL spatial.addWKT('geom',{wkt})", map("wkt", wkt));

        testCall(db, "CALL spatial.layer('geom')", (r) -> assertEquals("geom", (dump((Node) r.get("node"))).getProperty("layer")));
    }

    @Test
    public void add_a_node_to_the_spatial_index() throws Exception {
        execute("CALL spatial.addPointLayer('geom')");
        ResourceIterator<Object> nodes = db.execute("CREATE (n:Node {latitude:60.1,longitude:15.2}) RETURN n").columnAs("n");
        Node node = (Node) nodes.next();
        nodes.close();
        testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", r -> Assert.assertEquals(node, r.get("node")));
    }

    @Test
    public void testDistanceNode() throws Exception {
        execute("CALL spatial.addPointLayer('geom')");
        ResourceIterator<Object> nodes = db.execute("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node").columnAs("node");
        Node node = (Node) nodes.next();
        nodes.close();
        testCall(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", r -> assertEquals(node, r.get("node")));
    }

    @Test
    public void add_a_node_to_the_spatial_index_short() throws Exception {
        execute("CALL spatial.addPointLayerNamed('geom','lat','lon')");
        ResourceIterator<Object> nodes = db.execute("CREATE (n:Node {lat:60.1,lon:15.2}) RETURN n").columnAs("n");
        Node node = (Node) nodes.next();
        nodes.close();
        testCall(db, "MATCH (n:Node) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node", r -> Assert.assertEquals(node, r.get("node")));
    }

    @Test
    public void add_many_nodes_to_the_spatial_layer() throws Exception {
        execute("CALL spatial.addPointLayerNamed('geom','lat','lon')");
        ResourceIterator<Node> nodes = db.execute("CREATE (n1:Node {lat:60.1,lon:15.2}),(n2:Node {lat:60.1,lon:15.3}) WITH n1,n2 CALL spatial.addNodes('geom',[n1,n2]) YIELD node RETURN node").columnAs("node");
        Node node1 = nodes.next();
        Node node2 = nodes.next();
        nodes.close();
        testResult(db, "CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)", res -> {
                    assertEquals(true, res.hasNext());
                    assertEquals(node1, res.next().get("node"));
                    assertEquals(true, res.hasNext());
                    assertEquals(node2, res.next().get("node"));
                    assertEquals(false, res.hasNext());
                }
        );
    }

    @Test
    public void find_geometries_in_a_bounding_box_short() throws Exception {
        execute("CALL spatial.addPointLayerNamed('geom','lat','lon')");
        ResourceIterator<Object> nodes = db.execute("CREATE (n:Node {lat:60.1,lon:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node").columnAs("node");
        Node node = (Node) nodes.next();
        nodes.close();
        testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})",r -> assertEquals(node,r.get("node")));
    }
    @Test
    public void find_geometries_in_a_bounding_box() throws Exception {
        execute("CALL spatial.addPointLayer('geom')");
        ResourceIterator<Object> nodes = db.execute("CREATE (n:Node {latitude:60.1,longitude:15.2}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node").columnAs("node");
        Node node = (Node) nodes.next();
        nodes.close();
        testCall(db, "CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})",r -> assertEquals(node,r.get("node")));
    }


    @Test
    public void create_a_WKT_layer() throws Exception {
        testCall(db, "CALL spatial.addWKTLayer('geom','wkt')", r -> assertEquals("wkt",dump(((Node)r.get("node"))).getProperty("geomencoder_config")));
    }

    private static Node dump(Node n) {
        System.out.printf("id %d props %s%n",n.getId(),n.getAllProperties());
        System.out.flush();
        return n;
    }

    @Test
    public void add_a_WKT_geometry_to_a_layer() throws Exception {
        String lineString = "LINESTRING (15.2 60.1, 15.3 60.1)";

        execute("CALL spatial.addWKTLayer('geom','wkt')");
        testCall(db, "CALL spatial.addWKT('geom',{wkt})", map("wkt",lineString),
                r -> assertEquals(lineString,dump(((Node)r.get("node"))).getProperty("wkt")));
    }

    @Test
    public void find_geometries_close_to_a_point() throws Exception {
        String lineString = "LINESTRING (15.2 60.1, 15.3 60.1)";
        execute("CALL spatial.addConfiguredLayer('geom','WKT','wkt')");
        execute("CALL spatial.addWKT('geom',{wkt})", map("wkt",lineString));
        testCall(db, "CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)", r -> assertEquals(lineString, (dump((Node)r.get("node"))).getProperty("wkt")));
    }

    /*

    @Test
    @Documented("update_a_WKT_geometry_in_a_layer")
    public void update_a_WKT_geometry_in_a_layer() throws Exception {
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
    public void find_geometries_within__distance() throws Exception {
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
    public void add_a_wkt_node_to_the_spatial_index() throws Exception {
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
    public void find_geometries_in_a_bounding_box_using_cypher() throws Exception {
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
    public void find_geometries_within__distance_using_cypher() throws Exception {
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
