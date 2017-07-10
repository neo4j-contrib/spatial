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

import static org.junit.Assert.assertTrue;
import static org.neo4j.test.GraphDatabaseServiceCleaner.cleanDatabaseContent;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import javax.ws.rs.core.Response.Status;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.doc.tools.AbstractRestFunctionalTestBase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.test.ImpermanentGraphDatabase;

public class SpatialPluginFunctionalDocTest extends AbstractRestFunctionalTestBase
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
        altServer = CommunityServerBuilder.server().onAddress( new ListenSocketAddress( "localhost", PORT ) ).build();
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
     * The Neo4j Spatial Server plugin, if installed, will be announced in the
     * root representation for the Neo4j Server REST API.
     */
    @Test
    @Documented("finding_the_plugin")
    public void finding_the_plugin() throws UnsupportedEncodingException
    {
        gen.get().expectedStatus( Status.OK.getStatusCode() );
        String response = gen.get().get( ENDPOINT ).entity();
        assertTrue( response.contains( "graphdb" ) );
    }

    private int getNodeId(String response) throws ParseException {
        JSONObject o = (JSONObject) jsonParser.parse(response);
//        JSONArray array = (JSONArray) o;
        String self = (String)  o.get("self");
        String res = self.substring(self.lastIndexOf("/")+1);
        return Integer.parseInt(res);
    }

    /**
     * Create a point layer, specifying the `lon` and `lat` node properties as the ones carrying the
     * spatial information.
     */
    @Test
    @Documented("create_a_pointlayer")
    public void create_a_pointlayer() throws UnsupportedEncodingException
    {
        data.get();
        String response = post(Status.OK,"{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
    }

    /**
     * Find a layer by its name, returning the layer.
     */
    @Test
    @Documented("find_layer")
    public void find_layer() throws UnsupportedEncodingException
    {
        data.get();
        String geom = "geom";
        String response = post(Status.OK,"{\"layer\":\""+geom+"\", \"format\":\"WKT\",\"nodePropertyName\":\"wkt\"}", ENDPOINT + "/graphdb/addEditableLayer");
        String wkt = "LINESTRING (15.2 60.1, 15.3 60.1)";
        response = post(Status.OK,"{\"layer\":\""+geom+ "\", \"geometry\":\"" + wkt + "\"}", ENDPOINT+ "/graphdb/addGeometryWKTToLayer");
        response = post(Status.OK,"{\"layer\":\""+geom+ "\"}", ENDPOINT+ "/graphdb/getLayer");
        assertTrue(response.contains(geom));

    }

    /**
     * Add multiple nodes to the spatial layer.
     */
    @Test
    @Documented("add_many_nodes_to_the_spatial_layer")
    public void add_many_nodes_to_the_spatial_layer() throws Exception {
        data.get();
        String response = post(Status.OK,"{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        int nodeId = getNodeId(post(Status.CREATED, "{\"lat\":60.1, \"lon\":15.2}", "http://localhost:" + PORT + "/db/data/node"));
        int nodeId2 = getNodeId(post(Status.CREATED,"{\"lat\":60.1, \"lon\":15.3}", "http://localhost:"+PORT+"/db/data/node"));
        response = post(Status.OK,"{\"layer\":\"geom\", \"nodes\": [\"http://localhost:"+PORT+"/db/data/node/"+nodeId+"\",\"http://localhost:"+PORT+"/db/data/node/"+nodeId2+"\"]}", ENDPOINT + "/graphdb/addNodesToLayer");
        System.out.println("response = " + response);
        response = post(Status.OK,String.format("{\"layer\":\"%s\", \"pointX\":%s,\"pointY\":%s,\"distanceInKm\":%s}","geom",15.2,60.1,10.0 ), ENDPOINT + "/graphdb/findGeometriesWithinDistance");
        System.out.println("response = " + response);
        assertTrue(response.contains("15.2"));
        assertTrue(response.contains("15.3"));
    }

    /**
     * Creates a layer with nodes that have a property containing WKT or WKB,
     * returns the layer node containing the configuration for the newly created layer.
     */
    @Test
    @Documented("create_a_WKT_layer")
    public void create_a_WKT_layer() throws Exception
    {
        data.get();
        String geom = "geom";
        String response = post(Status.OK,"{\"layer\":\""+geom+"\", \"format\":\"WKT\",\"nodePropertyName\":\"wkt\"}", ENDPOINT + "/graphdb/addEditableLayer");
        assertTrue(response.contains("wkt"));

    }

    /**
     * Add a geometry, encoded in WKT, to a layer.
     */
    @Test
    @Documented("add_a_WKT_geometry_to_a_layer")
    public void add_a_WKT_geometry_to_a_layer() throws Exception
    {
        data.get();
        String geom = "geom";
        String response = post(Status.OK,"{\"layer\":\""+geom+"\", \"format\":\"WKT\",\"nodePropertyName\":\"wkt\"}", ENDPOINT + "/graphdb/addEditableLayer");
        String wkt = "LINESTRING (15.2 60.1, 15.3 60.1)";
        response = post(Status.OK,"{\"layer\":\""+geom+ "\", \"geometry\":\"" + wkt + "\"}", ENDPOINT+ "/graphdb/addGeometryWKTToLayer");
        assertTrue(response.contains(wkt));

    }

    /**
     * Update a geometry, encoded in WKT, on an existing geometry in a layer.
     */
    @Test
    @Documented("update_a_WKT_geometry_in_a_layer")
    public void update_a_WKT_geometry_in_a_layer() throws Exception
    {
        data.get();
        String geom = "geom";
        String response = post(Status.OK,"{\"layer\":\""+geom+"\", \"format\":\"WKT\",\"nodePropertyName\":\"wkt\"}", ENDPOINT + "/graphdb/addEditableLayer");
        String wkt = "LINESTRING (15.2 60.1, 15.3 60.1)";
        String wkt2 = "LINESTRING (16.2 60.1, 15.3 60.1)";
        response = post(Status.OK,"{\"layer\":\""+geom+ "\", \"geometry\":\"" + wkt + "\"}", ENDPOINT+ "/graphdb/addGeometryWKTToLayer");
        String self = (String) ((JSONObject)((JSONArray) new JSONParser().parse(response)).get(0)).get("self");
        String geomId=self.substring(self.lastIndexOf("/")+1);
        response = post(Status.OK,"{\"layer\":\""+geom+ "\", \"geometry\":\"" + wkt2 + "\",\"geometryNodeId\":"+geomId+"}", ENDPOINT+ "/graphdb/updateGeometryFromWKT");

        assertTrue(response.contains(wkt2));
        assertTrue(response.contains("http://localhost:"+PORT+"/db/data/node/"+geomId));

    }

    /**
     * Find geometries which clostest edges are less than a certain distance from a point.
     */
    @Test
    @Documented("find_geometries_close_to_a_point")
    public void find_geometries_close_to_a_point() throws Exception
    {
        data.get();
        String geom = "geom";
        String response = post(Status.OK,"{\"layer\":\""+geom+"\", \"format\":\"WKT\",\"nodePropertyName\":\"wkt\"}", ENDPOINT + "/graphdb/addEditableLayer");
        response = post(Status.OK,"{\"layer\":\""+geom+"\", \"geometry\":\"LINESTRING(15.2 60.1, 15.3 60.1)\"}", ENDPOINT+ "/graphdb/addGeometryWKTToLayer");
        response = post(Status.OK,String.format("{\"layer\":\"%s\", \"pointX\":%s,\"pointY\":%s,\"distanceInKm\":%s}",geom,15.2, 60.1, 1.0 ), ENDPOINT + "/graphdb/findClosestGeometries");
//        dumpDB();
        assertTrue(response.contains("60.1"));

    }

    private String findNodeInBox(String layer_name, double lon1, double lon2, double lat1, double lat2) {
        String response;
        return response = post(Status.OK,String.format("{\"layer\":\"%s\", \"minx\":%s,\"maxx\":%s,\"miny\":%s,\"maxy\":%s}",layer_name, lon1, lon2, lat1, lat2), ENDPOINT + "/graphdb/findGeometriesInBBox");
        
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
        cleanDatabaseContent(graphdb());
    }
    
}
