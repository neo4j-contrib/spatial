package org.neo4j.gis.spatial;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;


public class IndexProviderTest
{

    @Test
    public void testLoadIndex() throws Exception {
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( createTempDir() );
        Map<String, String> config = Collections.unmodifiableMap( MapUtil.stringMap(
                "provider", "spatial" ) );
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes( "layer1", config );
        assertNotNull(index);
        
    }
    
    private static String createTempDir() throws IOException
    {

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
