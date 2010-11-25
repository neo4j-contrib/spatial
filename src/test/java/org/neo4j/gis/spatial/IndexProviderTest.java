package org.neo4j.gis.spatial;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;


public class IndexProviderTest
{

    @Test
    public void testLoadIndex() {
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( "target/db" );
        Map<String, String> config = new HashMap<String, String>();
        config.put( "encoder", "dummy" );
        Index<Node> index = db.index().forNodes( "spatial", config );
        assertNotNull(index);
    }
}
