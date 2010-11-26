package org.neo4j.gis.spatial.indexprovider;

import java.util.List;
import java.util.Map;

import javax.naming.OperationNotSupportedException;

import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.Search;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.query.SearchWithin;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

public class LayerNodeIndex implements Index<Node>
{

    public static final String LON_PROPERTY_KEY = "lon";
    public static final String LAT_PROPERTY_KEY = "lat";
    public static final String WITHIN_QUERY = "within";
    public static final Object ENVELOPE_PARAMETER = "envelope";
    private final String layerName;
    private final GraphDatabaseService db;
    private SpatialDatabaseService spatialDB;
    private EditableLayer layer;

    public LayerNodeIndex( String indexName, GraphDatabaseService db,
            Map<String, String> config )
    {
        this.layerName = indexName;
        this.db = db;
        spatialDB = new SpatialDatabaseService( this.db );
        layer = (EditableLayer) spatialDB.getOrCreateEditableLayer( layerName );

    }

    @Override
    public String getName()
    {
        return layerName;
    }

    @Override
    public Class<Node> getEntityType()
    {
        return Node.class;
    }

    /**
     * right now we are assuming only Points with "lat" and "long" properties
     */
    @Override
    public void add( Node geometry, String key, Object value )
    {
        double lon = (Double) geometry.getProperty( LON_PROPERTY_KEY );
        double lat = (Double) geometry.getProperty( LAT_PROPERTY_KEY );
        layer.add( layer.getGeometryFactory().createPoint(
                new Coordinate( lon, lat ) ) );

    }

    @Override
    public void remove( Node entity, String key, Object value )
    {
       layer.delete( entity.getId() );

    }

    @Override
    public void delete()
    {
        

    }

    /**
     * Not supported at the moment
     */
    @Override
    public IndexHits<Node> get( String key, Object value )
    {
        return null;
    }

    @Override
    public IndexHits<Node> query( String key, Object params )
    {
        if ( key.equals( WITHIN_QUERY ) )
        {
            Map p = (Map) params;
            Double[] bounds = (Double[]) p.get( ENVELOPE_PARAMETER );
            SearchWithin withinQuery = new SearchWithin(
                    layer.getGeometryFactory().toGeometry(
                            new Envelope( bounds[0], bounds[1], bounds[2], bounds[3] ) ) );
            layer.getIndex().executeSearch( withinQuery );
            List<SpatialDatabaseRecord> res = withinQuery.getResults();
            IndexHits<Node> results = new SpatialRecordHits( res );
            return results;
        }
        else
        {
            throw new UnsupportedOperationException( String.format(
                    "only %s is implemented.", WITHIN_QUERY ) );
        }
    }

    @Override
    public IndexHits<Node> query( Object queryOrQueryObject )
    {
        return query(layerName, queryOrQueryObject);
    }

}
