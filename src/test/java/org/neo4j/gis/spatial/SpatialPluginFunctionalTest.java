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

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import junit.framework.Assert;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.test.ImpermanentGraphDatabase;

public class SpatialPluginFunctionalTest extends AbstractRestFunctionalTestBase
{
    private static final int PORT = 7575;
    private static final String ENDPOINT = "http://localhost:"+PORT+"/db/data/ext/SpatialPlugin";
    JSONParser jsonParser = new JSONParser();
    

    /** The server variable in SharedServerTestBase is disabled, and we use an alternative */
    private static NeoServer altServer;
	
    /**
     * Disable normal server creation, so we can override port number. 
     * @throws IOException
     */
    @BeforeClass
    public static void allocateServer() throws IOException {
        altServer = CommunityServerBuilder.server().onPort( PORT ).build();
        altServer.start();
    }

	/** Since we created a different server, we need to release that explicitly */
	@AfterClass
	public static final void releaseAltServer() {
		altServer.stop();
		altServer = null;
	}

	/** Use the alternative server with different port number */
	@Override
	public GraphDatabaseService graphdb() {
		return altServer.getDatabase().getGraph();
	}

    /**
     * The Neo4j Spatial Server plugin, if
     * installed, will be announced in the root representation
     * for the Neo4j Server REST API.
     */
    @Test
    @Documented
    public void finding_the_plugin() throws UnsupportedEncodingException
    {
        gen.get().expectedStatus( Status.OK.getStatusCode() );
        String response = gen.get().get( ENDPOINT ).entity();
        assertTrue( response.contains( "graphdb" ) );
    }

    /**
     * This allows cypher querying a bounding box
     */
    @Test
    @Documented
    public void querying_with_cypher() throws UnsupportedEncodingException, ParseException {
        data.get();
        String response = post(Status.OK,"{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        response = post(Status.CREATED,"{\"lat\":60.1, \"lon\":15.2}", "http://localhost:"+PORT+"/db/data/node");
        int nodeId = getNodeId(response);
        response = post(Status.CREATED,"{\"name\":\"geom\", \"config\":{\"provider\":\"spatial\", \"geometry_type\":\"point\",\"lat\":\"lat\",\"lon\":\"lon\"}}", "http://localhost:"+PORT+"/db/data/index/node/");
        response = post(Status.OK,"{\"layer\":\"geom\", \"node\":\"http://localhost:"+PORT+"/db/data/node/"+nodeId+"\"}", ENDPOINT + "/graphdb/addNodeToLayer");
        response = post(Status.OK,"{\"layer\":\"geom\", \"minx\":15.0,\"maxx\":15.3,\"miny\":60.0,\"maxy\":60.2}", ENDPOINT + "/graphdb/findGeometriesInBBox");
        assertTrue(response.contains( "60.1" ));
        response = post(Status.OK,"{\"layer\":\"geom\", \"pointX\":15.0,\"pointY\":60.0,\"distanceInKm\":100}", ENDPOINT + "/graphdb/findGeometriesWithinDistance");
        assertTrue(response.contains( "60.1" ));
        response = post(Status.OK,"{\"query\":\"start node = node:geom(\'bbox:[15.0,15.3,60.0,60.2]\') return node\"}", "http://localhost:"+PORT+"/db/data/cypher");
        //in this  version not supported future feature
        assertFalse(response.contains("60.1"));
        response = post(Status.OK,"{\"query\":\"start node = node:geom(\'withinDistance:[15.0,60.0, 100.0]\') return node\"}", "http://localhost:"+PORT+"/db/data/cypher");
        assertFalse(response.contains( "60.1" ));

    }

    private int getNodeId(String response) throws ParseException {
        JSONObject o = (JSONObject) jsonParser.parse(response);
//        JSONArray array = (JSONArray) o;
        String self = (String)  o.get("self");
        String res = self.substring(self.lastIndexOf("/")+1);
        return Integer.parseInt(res);
    }

    /**
     * Firstly, create a point layer, specifying the `lon` and `lat` node properties as the ones carrying the
     * spatial information.
     */
    @Test
    @Documented
    public void create_a_pointlayer() throws UnsupportedEncodingException
    {
        data.get();
        String response = post(Status.OK,"{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
    }

    /**
     * Now, add that node to the spatial
     * layer.
     */
    @Test
    @Documented
    public void create_a_spatial_index() throws UnsupportedEncodingException
    {
        data.get();
        String response = post(Status.OK,"{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        response = post(Status.CREATED,"{\"name\":\"geom\", \"config\":{\"provider\":\"spatial\", \"geometry_type\":\"point\",\"lat\":\"lat\",\"lon\":\"lon\"}}", "http://localhost:"+PORT+"/db/data/index/node/");
    }
    /**
     * Create a node with some spatial data like `lon` and `lat` 
     * attached.
     */
    @Test
    @Documented
    public void create_a_node_with_spatial_data() throws UnsupportedEncodingException
    {
        data.get();
        String response = post(Status.OK,"{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        response = post(Status.CREATED,"{\"name\":\"geom\", \"config\":{\"provider\":\"spatial\", \"geometry_type\":\"point\",\"lat\":\"lat\",\"lon\":\"lon\"}}", "http://localhost:"+PORT+"/db/data/index/node/");
        response = post(Status.CREATED,"{\"lat\":60.1, \"lon\":15.2}", "http://localhost:"+PORT+"/db/data/node");
    }

    /**
     * Add the node we created to the spatial 
     * index.
     */
    @Test
    @Documented
    public void add_a_node_to_the_spatial_index() throws Exception {
        data.get();
        String response = post(Status.OK,"{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        response = post(Status.CREATED,"{\"name\":\"geom\", \"config\":{\"provider\":\"spatial\", \"geometry_type\":\"point\",\"lat\":\"lat\",\"lon\":\"lon\"}}", "http://localhost:"+PORT+"/db/data/index/node/");
        response = post(Status.CREATED,"{\"lat\":60.1, \"lon\":15.2}", "http://localhost:"+PORT+"/db/data/node");
        int nodeId = getNodeId(response);
        response = post(Status.OK,"{\"layer\":\"geom\", \"node\":\"http://localhost:"+PORT+"/db/data/node/"+nodeId+"\"}", ENDPOINT + "/graphdb/addNodeToLayer");
    }

    /**
     * Add a node to an index created as a WKT
     */
    @Test
    @Documented
    public void add_a_wkt_node_to_the_spatial_index() throws Exception {
        data.get();
        String response = post(Status.OK,"{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        response = post(Status.CREATED,"{\"name\":\"geom_wkt\", \"config\":{\"provider\":\"spatial\", \"wkt\":\"wkt\"}}", "http://localhost:"+PORT+"/db/data/index/node/");
        response = post(Status.CREATED,"{\"wkt\":\"POINT(15.2 60.1)\"}", "http://localhost:"+PORT+"/db/data/node");
        int nodeId = getNodeId(response);
        response = post(Status.OK,"{\"layer\":\"geom_wkt\", \"node\":\"http://localhost:"+PORT+"/db/data/node/"+nodeId+"\"}", ENDPOINT + "/graphdb/addNodeToLayer");
        assertTrue(findNodeInBox("geom_wkt",15.0, 15.3, 60.0, 61.0).contains( "60.1" ));
        //update the node
        response = put(Status.NO_CONTENT,"{\"wkt\":\"POINT(31 61)\"}", "http://localhost:"+PORT+"/db/data/node/"+nodeId+"/properties");
        response = post(Status.OK,"{\"layer\":\"geom_wkt\", \"node\":\"http://localhost:"+PORT+"/db/data/node/"+nodeId+"\"}", ENDPOINT + "/graphdb/addNodeToLayer");
//        assertFalse(findNodeInBox("geom_wkt", 15.0, 15.3, 60.0, 61.0).contains("60.1"));
        assertTrue(findNodeInBox("geom_wkt",30, 32, 60.0, 62.0).contains( "31" ));


    }

    /**
     * Find geometries in a bounding 
     * box.
     */
    @Test
    @Documented
    public void find_geometries_in_a_bounding_box() throws Exception
    {
        data.get();
        String response = post(Status.OK,"{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        response = post(Status.CREATED,"{\"name\":\"geom\", \"config\":{\"provider\":\"spatial\", \"geometry_type\":\"point\",\"lat\":\"lat\",\"lon\":\"lon\"}}", "http://localhost:"+PORT+"/db/data/index/node/");
        response = post(Status.CREATED,"{\"lat\":60.1, \"lon\":15.2}", "http://localhost:"+PORT+"/db/data/node");
        int nodeId = getNodeId(response);
        response = post(Status.OK,"{\"layer\":\"geom\", \"node\":\"http://localhost:"+PORT+"/db/data/node/"+nodeId+"\"}", ENDPOINT + "/graphdb/addNodeToLayer");
        assertTrue(findNodeInBox("geom",15.0, 15.3, 60.0, 61.0).contains("60.1"));

    }

    private String findNodeInBox(String layer_name, double lon1, double lon2, double lat1, double lat2) {
        String response;
        return response = post(Status.OK,String.format("{\"layer\":\"%s\", \"minx\":%s,\"maxx\":%s,\"miny\":%s,\"maxy\":%s}",layer_name, lon1, lon2, lat1, lat2), ENDPOINT + "/graphdb/findGeometriesInBBox");
        
    }

    /**
     * Find geometries within a 
     * distance.
     */
    @Test
    @Documented
    public void find_geometries_within__distance() throws Exception
    {
        data.get();
        String response = post(Status.OK,"{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        response = post(Status.CREATED,"{\"name\":\"geom\", \"config\":{\"provider\":\"spatial\", \"geometry_type\":\"point\",\"lat\":\"lat\",\"lon\":\"lon\"}}", "http://localhost:"+PORT+"/db/data/index/node/");
        response = post(Status.CREATED,"{\"lat\":60.1, \"lon\":15.2}", "http://localhost:"+PORT+"/db/data/node");
        int nodeId = getNodeId(response);
        response = post(Status.OK,"{\"layer\":\"geom\", \"node\":\"http://localhost:"+PORT+"/db/data/node/"+nodeId+"\"}", ENDPOINT + "/graphdb/addNodeToLayer");
        response = post(Status.OK,"{\"layer\":\"geom\", \"pointX\":15.0,\"pointY\":60.0,\"distanceInKm\":100}", ENDPOINT + "/graphdb/findGeometriesWithinDistance");
        assertTrue(response.contains( "60.1" ));
    }

    /**
     * Find geometries in a bounding 
     * box.
     */
    @Test
    @Documented
    public void find_geometries_in_a_bounding_box_using_cypher() throws Exception
    {
        data.get();
        String response = post(Status.OK,"{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        response = post(Status.CREATED,"{\"name\":\"geom\", \"config\":{\"provider\":\"spatial\", \"geometry_type\":\"point\",\"lat\":\"lat\",\"lon\":\"lon\"}}", "http://localhost:"+PORT+"/db/data/index/node/");
        response = post(Status.CREATED,"{\"lat\":60.1, \"lon\":15.2}", "http://localhost:"+PORT+"/db/data/node");
        int nodeId = getNodeId(response);
        // add domain-node via index, so that the geometry companion is created and added to the layer
        response = post(Status.CREATED,"{\"value\":\"dummy\",\"key\":\"dummy\", \"uri\":\"http://localhost:"+PORT+"/db/data/node/"+nodeId+"\"}", "http://localhost:"+PORT+"/db/data/index/node/geom");

        response = post(Status.OK,"{\"query\":\"start node = node:geom(\'bbox:[15.0,15.3,60.0,60.2]\') return node\"}", "http://localhost:"+PORT+"/db/data/cypher");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(response).get("data").get(0).get(0).get("data");
        assertEquals(15.2, node.get("lon").getDoubleValue());
        assertEquals(60.1, node.get("lat").getDoubleValue());
    }

    /**
     * Find geometries within a distance, using 
     * Cypher.
     */
    @Test
    @Documented
    public void find_geometries_within__distance_using_cypher() throws Exception
    {
        data.get();
        String response = post(Status.OK,"{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        response = post(Status.CREATED,"{\"name\":\"geom\", \"config\":{\"provider\":\"spatial\", \"geometry_type\":\"point\",\"lat\":\"lat\",\"lon\":\"lon\"}}", "http://localhost:"+PORT+"/db/data/index/node/");
        response = post(Status.CREATED,"{\"lat\":60.1, \"lon\":15.2}", "http://localhost:"+PORT+"/db/data/node");
        int nodeId = getNodeId(response);

        // add domain-node via index, so that the geometry companion is created and added to the layer
        response = post(Status.CREATED,"{\"value\":\"dummy\",\"key\":\"dummy\", \"uri\":\"http://localhost:"+PORT+"/db/data/node/"+nodeId+"\"}", "http://localhost:"+PORT+"/db/data/index/node/geom");
        response = post(Status.OK,"{\"query\":\"start node = node:geom(\'withinDistance:[60.0,15.0, 100.0]\') return node\"}", "http://localhost:"+PORT+"/db/data/cypher");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(response).get("data").get(0).get(0).get("data");
        assertEquals(15.2, node.get("lon").getDoubleValue());
        assertEquals(60.1, node.get("lat").getDoubleValue());
    }

    
    private String post(Status status, String payload, String endpoint) {
        return gen().expectedStatus( status.getStatusCode() ).payload( payload ).post( endpoint).entity();
    }
    private String put(Status status, String payload, String endpoint) {
        return gen().expectedStatus( status.getStatusCode() ).payload( payload ).put(endpoint).entity();
    }
    
    @Before
    public void cleanContent()
    {
        ImpermanentGraphDatabase graphdb = (ImpermanentGraphDatabase) graphdb();
        graphdb.cleanContent(true);
        //clean
        gen.get().setGraph( graphdb() );
        
    }
    
}
