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
/**
 ' * Copyright (c) 2002-2011 "Neo Technology,"
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


import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.gis.spatial.indexprovider.SpatialIndexProvider;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.server.WrappingNeoServer;
import org.neo4j.test.ImpermanentGraphDatabase;


import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WithinDistanceUsingPointConfigTest {
    private ImpermanentGraphDatabase db;
    private WrappingNeoServer server;

    @Before
    public void setup() throws Exception {
        db = new ImpermanentGraphDatabase();
        server = new WrappingNeoServer(db);
        server.start();
    }

    @After
    public void tearDown() {
        server.stop();
    }

    @Test
    public void testWithinDistanceShouldWorkFromCypherDirectlyAndViaRest() throws URISyntaxException {
        Map<String, String> config = SpatialIndexProvider.SIMPLE_POINT_CONFIG;
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes("geom", config);

        db.beginTx();
        Node n1 = db.createNode();
        n1.setProperty("lat", 60.1);
        n1.setProperty("lon", 15.2);
        index.add(n1, "dummy", "value");

        ExecutionEngine engine = new ExecutionEngine(db);
        ExecutionResult result = engine.execute("start n=node:geom('withinDistance:[60.0, 15.0, 100.0]') return n");

        Iterator<Object> rows = result.columnAs("n");
        boolean hasRow = rows.hasNext();
        assertTrue(hasRow);

        NodeProxy row = (NodeProxy) rows.next();
        assertEquals(60.1, row.getProperty("lat"));
        assertEquals(15.2, row.getProperty("lon"));

        ClientRequest request = ClientRequest.
                create().
                accept(MediaType.APPLICATION_JSON_TYPE).
                type(MediaType.APPLICATION_JSON_TYPE).
                entity("{\"query\":\"start node = node:geom(\'withinDistance:[60.0,15.0, 100.0]\') return node\"}").
                build(new URI("http://localhost:7474/db/data/cypher"), "POST");


        ClientResponse response = new Client().handle(request);
        String responseAsString = response.getEntity(String.class);

        System.out.println("responseAsString = " + responseAsString);
    }
}
