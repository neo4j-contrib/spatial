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
package org.neo4j.gis.spatial;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.gis.spatial.indexprovider.LayerNodeIndex;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class IndexProviderTest
{
    
    private EmbeddedGraphDatabase db;
    @Before
    public void setup() throws Exception {
        db = new EmbeddedGraphDatabase( createTempDir() );
    }

    @Ignore
    @Test
    public void testLoadIndex() throws Exception {
        Map<String, String> config = Collections.unmodifiableMap( MapUtil.stringMap(
                "provider", "spatial" ) );
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes( "layer1", config );
        assertNotNull(index);
        
    }
    
    @Test
    public void testNodeIndex() {
        LayerNodeIndex index = new LayerNodeIndex( "layer1", db, new HashMap<String, String>() );
        Transaction tx = db.beginTx();
        Node n1 = db.createNode();
        n1.setProperty( "lat", (double)56.2 );
        n1.setProperty( "lon", (double)15.3 );
        index.add( n1, "dummy", "value" );
        tx.success();
        tx.finish();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put(LayerNodeIndex.ENVELOPE_PARAMETER, new Double[]{ 15.0, 16.0, 56.0, 57.0} );
        IndexHits<Node> hits = index.query( LayerNodeIndex.WITHIN_QUERY, params );
        assertTrue(hits.hasNext());
        
        
    }

    private static String createTempDir() throws IOException {
        File d = File.createTempFile( "neo4j-test", "dir" );
        if ( !d.delete() )
        {
            throw new RuntimeException( "temp config directory pre-delete failed" );
        }
        if ( !d.mkdirs() )
        {
            throw new RuntimeException( "temp config directory not created" );
        }
        d.deleteOnExit();
        return d.getAbsolutePath();
    }
}
