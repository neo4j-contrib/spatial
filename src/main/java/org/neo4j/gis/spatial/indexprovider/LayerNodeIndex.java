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
package org.neo4j.gis.spatial.indexprovider;

import java.util.List;
import java.util.Map;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.internal.matchers.SubstringMatcher;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.Search;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.query.SearchPointsWithinOrthodromicDistance;
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
    public static final String CQL_QUERY = "CQL";
    public static final String WITHIN_DISTANCE_QUERY = "withinDistance";
    public static final String BBOX_QUERY = "bbox";
    public static final String ENVELOPE_PARAMETER = "envelope";
    public static final String POINT_PARAMETER = "point";
    public static final String DISTANCE_IN_KM_PARAMETER = "distanceInKm";
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

    public String getName()
    {
        return layerName;
    }

    public Class<Node> getEntityType()
    {
        return Node.class;
    }

    /**
     * right now we are assuming only Points with "lat" and "lon" properties
     */
    public void add( Node geometry, String key, Object value )
    {
        double lon = (Double) geometry.getProperty( LON_PROPERTY_KEY );
        double lat = (Double) geometry.getProperty( LAT_PROPERTY_KEY );
        layer.add(
                layer.getGeometryFactory().createPoint(
                        new Coordinate( lon, lat ) ), new String[] { "id" },
                new Object[] { geometry.getId() } );

    }

    public void remove( Node entity, String key, Object value )
    {
        layer.delete( entity.getId() );
    }

    public void delete()
    {
    }

    /**
     * Not supported at the moment
     */
    public IndexHits<Node> get( String key, Object value )
    {
        return null;
    }

    public IndexHits<Node> query( String key, Object params )
    {
//        System.out.println( key + "," + params );
        if ( key.equals( WITHIN_QUERY ) )
        {
            Map<?, ?> p = (Map<?, ?>) params;
            Double[] bounds = (Double[]) p.get( ENVELOPE_PARAMETER );
            SearchWithin withinQuery = new SearchWithin(
                    layer.getGeometryFactory().toGeometry(
                            new Envelope( bounds[0], bounds[1], bounds[2],
                                    bounds[3] ) ) );
            layer.getIndex().executeSearch( withinQuery );
            List<SpatialDatabaseRecord> res = withinQuery.getResults();
            IndexHits<Node> results = new SpatialRecordHits( res );
            return results;
        }
        else if ( key.equals( WITHIN_DISTANCE_QUERY ) )
        {
            Map<?, ?> p = (Map<?, ?>) params;
            Double[] point = (Double[]) p.get( POINT_PARAMETER );
            Double distance = (Double) p.get( DISTANCE_IN_KM_PARAMETER );
            Search withinDistanceQuery = new SearchPointsWithinOrthodromicDistance(
                    new Coordinate( point[1], point[0] ), distance, true );
            layer.getIndex().executeSearch( withinDistanceQuery );
            List<SpatialDatabaseRecord> res = withinDistanceQuery.getResults();
            IndexHits<Node> results = new SpatialRecordHits( res );
            return results;
        }
        else if ( key.equals( BBOX_QUERY ) )
        {
            List<Double> coords;
            try
            {
                coords = (List<Double>) new JSONParser().parse( (String) params );
                SearchWithin withinQuery = new SearchWithin(
                        layer.getGeometryFactory().toGeometry(
                                new Envelope( coords.get( 0 ), coords.get( 1 ),
                                        coords.get( 2 ), coords.get( 3 ) ) ) );
                layer.getIndex().executeSearch( withinQuery );
                List<SpatialDatabaseRecord> res = withinQuery.getResults();
                IndexHits<Node> results = new SpatialRecordHits( res );
                return results;
            }
            catch ( ParseException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        else
        {
            throw new UnsupportedOperationException( String.format(
                    "only %s, %S and %s are implemented.", WITHIN_QUERY,
                    WITHIN_DISTANCE_QUERY, BBOX_QUERY ) );
        }
        return null;
    }

    public IndexHits<Node> query( Object queryOrQueryObject )
    {

        String queryString = (String) queryOrQueryObject;
        return query( queryString.substring( 0, queryString.indexOf( ":" ) ),
                queryString.substring( queryString.indexOf( ":" ) + 1 ) );
    }

    public void remove( Node node, String s )
    {
        layer.delete( node.getId() );
    }

    public void remove( Node node )
    {
        layer.delete( node.getId() );
    }
}
