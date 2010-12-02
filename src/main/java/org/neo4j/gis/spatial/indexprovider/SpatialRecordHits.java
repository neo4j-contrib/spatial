package org.neo4j.gis.spatial.indexprovider;

import java.util.List;

import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.graphdb.Node;
import org.neo4j.index.impl.lucene.AbstractIndexHits;

public class SpatialRecordHits extends AbstractIndexHits<Node>
{
    private final int size;
    private final List<SpatialDatabaseRecord> hits;
    private int index;
    
    public SpatialRecordHits( List<SpatialDatabaseRecord> hits )
    {
        this.size = hits.size();
        this.hits = hits;
    }

    @Override
    protected Node fetchNextOrNull()
    {
        int i = index++;
        return i < size() ? hits.get( i ).getGeomNode() : null;
    }
    
    public int size()
    {
        return this.size;
    }

    public float currentScore()
    {
        return 0;
    }
}

