package org.neo4j.gis.spatial.indexprovider;

import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.graphdb.index.RelationshipIndex;

public class SpatialIndexProvider extends IndexProvider
{

    protected SpatialIndexProvider( String key )
    {
        super( key );
        
    }

    @Override
    public String getDataSourceName()
    {
        return "spatial";
    }

    @Override
    public Index<Node> nodeIndex( String indexName, Map<String, String> config )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelationshipIndex relationshipIndex( String indexName,
            Map<String, String> config )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> fillInDefaults( Map<String, String> config )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean configMatches( Map<String, String> storedConfig,
            Map<String, String> config )
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    protected void load( KernelData kernel )
    {
        System.out.println( "loading spatial index" );
    }

}
