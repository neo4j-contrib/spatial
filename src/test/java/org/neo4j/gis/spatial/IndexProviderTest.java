/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.SyntaxException;
import org.neo4j.cypher.commands.Query;
import org.neo4j.cypher.javacompat.CypherParser;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.gis.spatial.indexprovider.LayerNodeIndex;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
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

    @Test
    public void testLoadIndex() throws Exception
    {
        Map<String, String> config = Collections.unmodifiableMap( MapUtil.stringMap(
                "provider", "spatial" ) );
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes( "layer1", config );
        assertNotNull( index );

    }

    @Test
    public void testNodeIndex() throws SyntaxException
    {
        Map<String, String> config = Collections.unmodifiableMap( MapUtil.stringMap(
                "provider", "spatial" ) );
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
        params.put( LayerNodeIndex.ENVELOPE_PARAMETER, new Double[] { 15.0,
                16.0, 56.0, 57.0 } );
        IndexHits<Node> hits = index.query( LayerNodeIndex.WITHIN_QUERY, params );
        assertTrue( hits.hasNext() );
        // test String search
        hits = index.query( LayerNodeIndex.BBOX_QUERY,
                "[15.0, 16.0, 56.0, 57.0]" );
        assertTrue( hits.hasNext() );
        // test Cypher query
        CypherParser parser = new CypherParser();
        ExecutionEngine engine = new ExecutionEngine( db );
         Query query = parser.parse(
         "start n=(layer1,'bbox:[15.0, 16.0, 56.0, 57.0]') match (n) -[r] - (x) return n, type(r), x.layer?, x.bbox?"
         );
//        Query query = parser.parse( "start n=(0) where 1=1 return n" );
        ExecutionResult result = engine.execute( query );
        System.out.println( result.toString() );

        // test Gremlin
        ScriptEngine gremlinEngine = new ScriptEngineManager().getEngineByName( "gremlin" );
        final Bindings bindings = new SimpleBindings();
        final Neo4jGraph graph = new Neo4jGraph( db );
        bindings.put( "g", graph );
        gremlinEngine.setBindings( bindings, ScriptContext.ENGINE_SCOPE );
        try
        {
            assertEquals(2L, gremlinEngine.eval( "g.idx('layer1').get('bbox','[15.0, 16.0, 56.0, 57.0]')._().in().count()"));
//            System.out.print( gremlinEngine.eval( "g.idx('layer1').get('bbox','[15.0, 16.0, 56.0, 57.0]').in('GEOM').name"));
        }
        catch ( ScriptException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    @Test
    public void testWithinDistanceIndex()
    {
        LayerNodeIndex index = new LayerNodeIndex( "layer1", db,
                new HashMap<String, String>() );
        Transaction tx = db.beginTx();
        Node batman = db.createNode();
        batman.setProperty( "lat", (double) 37.88 );
        batman.setProperty( "lon", (double) 41.14 );
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
        Node spatialRecord = hits.getSingle();
        /* assertTrue( spatialRecord.getProperty( "distanceInKm" ).equals(
                1.416623647558699 ) ); */
        Node node = db.getNodeById( (Long) spatialRecord.getProperty( "id" ) );
        assertTrue( node.getProperty( "name" ).equals( "batman" ) );

    }
}
