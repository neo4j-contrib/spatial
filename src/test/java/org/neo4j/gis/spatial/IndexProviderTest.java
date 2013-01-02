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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.SimpleBindings;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.SyntaxException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.gis.spatial.indexprovider.LayerNodeIndex;
import org.neo4j.gis.spatial.indexprovider.SpatialIndexProvider;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.test.ImpermanentGraphDatabase;

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;

public class IndexProviderTest
{

    private ImpermanentGraphDatabase db;

    @Before
    public void setup() throws Exception
    {
        db = new ImpermanentGraphDatabase();
    }

    /**
     * Test that we can create and retrieve indexes
     */
    @Test
    public void testLoadIndex()
    {
        Map<String, String> config = SpatialIndexProvider.SIMPLE_POINT_CONFIG;
        IndexManager indexMan = db.index();
        // Create a new node index with our settings
        Index<Node> index = indexMan.forNodes( "layer1", config );
        // Retrieve an existing node index, enforcing compatible settings
        index = indexMan.forNodes( "layer1", config );
        assertNotNull( index );

    }

    @Test
    public void testNodeIndex() throws SyntaxException, Exception
    {
        Map<String, String> config = SpatialIndexProvider.SIMPLE_POINT_CONFIG;
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes( "layer1", config );
        assertNotNull( index );
        Transaction tx = db.beginTx();
        Node n1 = db.createNode();
        n1.setProperty( "lat", (double) 56.2 );
        n1.setProperty( "lon", (double) 15.3 );
        index.add( n1, "dummy", "value" );
        tx.success();
        tx.finish();
        Map<String, Object> params = new HashMap<String, Object>();
        //within Envelope
        params.put( LayerNodeIndex.ENVELOPE_PARAMETER, new Double[] { 15.0,
                16.0, 56.0, 57.0 } );
        IndexHits<Node> hits = index.query( LayerNodeIndex.WITHIN_QUERY, params );
        assertTrue( hits.hasNext() );
        
        // within BBOX
        hits = index.query( LayerNodeIndex.BBOX_QUERY,
                "[15.0, 16.0, 56.0, 57.0]" );
        assertTrue( hits.hasNext() );
        
        //within any WKT geometry
        hits = index.query( LayerNodeIndex.WITHIN_WKT_GEOMETRY_QUERY,
                "POLYGON ((15 56, 15 57, 16 57, 16 56, 15 56))" );
        assertTrue( hits.hasNext() );
        //polygon with hole, excluding n1
        hits = index.query( LayerNodeIndex.WITHIN_WKT_GEOMETRY_QUERY,
                "POLYGON ((15 56, 15 57, 16 57, 16 56, 15 56)," +
                "(15.1 56.1, 15.1 56.3, 15.4 56.3, 15.4 56.1, 15.1 56.1))" );
        assertFalse( hits.hasNext() );
        
        
        //within distance
        params.clear();
        params.put(LayerNodeIndex.POINT_PARAMETER, new Double[]{56.5, 15.5});
        params.put(LayerNodeIndex.DISTANCE_IN_KM_PARAMETER, 100.0);
        hits = index.query( LayerNodeIndex.WITHIN_DISTANCE_QUERY,
                params );
        assertTrue( hits.hasNext() );
        // test Cypher query
        ExecutionEngine engine = new ExecutionEngine( db );
//        ExecutionResult result = engine.execute(  "start n=node:layer1('bbox:[15.0, 16.0, 56.0, 57.0]') match (n) -[r] - (x) return n, type(r), x.layer?, x.bbox?"  );
        
        ExecutionResult result = engine.execute(  "start n=node:layer1('bbox:[15.0, 16.0, 56.0, 57.0]') return n"  );
        System.out.println( result.toString() );

        // test Gremlin
        ScriptEngine gremlinEngine = new ScriptEngineManager().getEngineByName( "gremlin-groovy" );
        final Bindings bindings = new SimpleBindings();
        final Neo4jGraph graph = new Neo4jGraph( db, false );
        bindings.put( "g", graph );
        gremlinEngine.setBindings( bindings, ScriptContext.ENGINE_SCOPE );
//        assertEquals(
//                2L,
//                gremlinEngine.eval( "g.idx('layer1')[[bbox:'[15.0, 16.0, 56.0, 57.0]']].in().count()" ) );

		// Rather than counting the incoming vertices, we just count the nodes
		// of which there are one, with no incoming edges
        assertEquals(
                1L,
                gremlinEngine.eval( "g.idx('layer1')[[bbox:'[15.0, 16.0, 56.0, 57.0]']].count()" ) );        

    }

    @Test
    public void testWithinDistanceIndex()
    {
        Map<String, String> config = SpatialIndexProvider.SIMPLE_POINT_CONFIG_WKT;
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes( "layer2", config );
        Transaction tx = db.beginTx();
        Node batman = db.createNode();
        batman.setProperty( "wkt", "POINT(41.14 37.88 )");
        batman.setProperty( "name", "batman" );
        index.add( batman, "dummy", "value" );
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( LayerNodeIndex.POINT_PARAMETER,
                new Double[] { 37.87, 41.13 } );
        params.put( LayerNodeIndex.DISTANCE_IN_KM_PARAMETER, 2.0 );
        IndexHits<Node> hits = index.query(
                LayerNodeIndex.WITHIN_DISTANCE_QUERY, params );
        tx.success();
        tx.finish();
        Node node = hits.getSingle();
        /* assertTrue( spatialRecord.getProperty( "distanceInKm" ).equals(
                1.416623647558699 ) ); */

        //We not longer need this as the node we get back already a 'Real' node
//        Node node = db.getNodeById( (Long) spatialRecord.getProperty( "id" ) );
        assertTrue( node.getProperty( "name" ).equals( "batman" ) );

    }
    
    @Test
    public void testWithinDistanceIndexViaCypher()
    {
        Map<String, String> config = SpatialIndexProvider.SIMPLE_POINT_CONFIG_WKT;
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes( "layer3", config );
        Transaction tx = db.beginTx();
        Node batman = db.createNode();
        batman.setProperty( "wkt", "POINT(44.44 33.33 )");
        batman.setProperty( "name", "robin" );
        index.add( batman, "dummy", "value" );
        
        ExecutionEngine engine = new ExecutionEngine( db );
        ExecutionResult result = engine.execute(  "start n=node:layer3('withinDistance:[44.44, 33.32, 5.0]') return n"  );
        System.out.println( result.toString() );
    }
    
    /**
     * Test the performance of LayerNodeIndex.add()
     * Insert up to 100K nodes into the database, and into the index, randomly distributed over [-80,+80][-170+170]
     * Calculate speed over 100-node groups, fail if speed falls under 50 adds/second (typical max speed here 500 adds/second)
     */
// Uncomment the next line once the add performance bug is fixed.
// See https://github.com/neo4j/spatial/issues/72
//    @Test
    public void testAddPerformance()
    {
        Map<String, String> config = SpatialIndexProvider.SIMPLE_POINT_CONFIG;
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes( "pointslayer", config );

        Transaction tx = db.beginTx();
        try {
            Random r = new Random();
            final int stepping = 100;
            long start = System.currentTimeMillis();
            long previous = start;
            for (int i=1; i<=100000; i++) {
                Node newnode = db.createNode();
                newnode.setProperty( "lat", (double) r.nextDouble()*160-80 );
                newnode.setProperty( "lon", (double) r.nextDouble()*340-170 );
                
                index.add(newnode, "dummy", "value");
                
                if ( i%stepping == 0) {
                    long now = System.currentTimeMillis();
                    long duration = now-start;
                    long stepDuration = now-previous;
                    double speed = stepping / (stepDuration/1000.0);
                    double linearity = (double)stepDuration/i;
                    System.out.println("testAddPerformance(): "+ stepping +" nodes added in "+ stepDuration +"ms, total "+ i +" in "+ duration +"ms, speed: "+ speed +" adds per second, "+linearity+" ms per step per node in index");
                    final double targetSpeed = 50.0;	// Quite conservative, max speed here 500 adds per second
                    assertTrue("add is too slow at size:"+i+" ("+speed+" adds per second <= "+targetSpeed+")", speed > targetSpeed);

                    previous = now;
                }
            }
            tx.success();
        } finally {
            System.out.println("testAddPerformance(): finishing transaction");
            tx.finish();
        }
    }
}
