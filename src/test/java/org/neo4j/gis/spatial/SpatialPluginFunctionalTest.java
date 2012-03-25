/**
 * Copyright (c) 2010-2012 "Neo Technology,"
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

import java.io.UnsupportedEncodingException;

import javax.ws.rs.core.Response.Status;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.test.ImpermanentGraphDatabase;

public class SpatialPluginFunctionalTest extends AbstractRestFunctionalTestBase
{
    private static final String ENDPOINT = "http://localhost:7474/db/data/ext/SpatialPlugin";

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
     * allows cypher querying a bounding box
     */
    @Test
    @Documented
    public void querying_with_cypher() throws UnsupportedEncodingException
    {
        data.get();
        String response = post(Status.OK,"{\"layer\":\"geom\", \"lat\":\"lat\", \"lon\":\"lon\"}", ENDPOINT + "/graphdb/addSimplePointLayer");
        response = post(Status.CREATED,"{\"lat\":60.1, \"lon\":15.2}", "http://localhost:7474/db/data/node");
        response = post(Status.CREATED,"{\"name\":\"geom\", \"config\":{\"provider\":\"spatial\"}}", "http://localhost:7474/db/data/index/node/");
        response = post(Status.OK,"{\"layer\":\"geom\", \"node\":\"http://localhost:7474/db/data/node/5\"}", ENDPOINT + "/graphdb/addNodeToLayer");
        response = post(Status.OK,"{\"layer\":\"geom\", \"minx\":15.0,\"maxx\":15.3,\"miny\":60.0,\"maxy\":60.2}", ENDPOINT + "/graphdb/findGeometriesInBBox");
        assertTrue(response.contains( "60.1" ));
        response = post(Status.OK,"{\"layer\":\"geom\", \"pointX\":15.0,\"pointY\":60.0,\"distanceInKm\":100}", ENDPOINT + "/graphdb/findGeometriesWithinDistance");
        assertTrue(response.contains( "60.1" ));
        response = post(Status.OK,"{\"query\":\"start node = node:geom(\'bbox:[15.0,15.3,60.0,60.2]\') return node\"}", "http://localhost:7474/db/data/cypher");
        assertTrue(response.contains( "node" ));
        
    }
    
    private String post(Status status, String payload, String endpoint) {
        return gen().expectedStatus( status.getStatusCode() ).payload( payload ).post( endpoint).entity();
    }
    
    @Before
    public void cleanContent()
    {
        ((ImpermanentGraphDatabase)graphdb()).cleanContent(true);
        gen.get().setGraph( graphdb() );
    }
    
}
