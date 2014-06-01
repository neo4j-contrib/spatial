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
package org.neo4j.gis.spatial.indexprovider;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import java.util.List;
import java.util.Map;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.neo4j.gis.spatial.rtree.NullListener;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

public class LayerNodeIndex implements Index<Node>
{

    public static final String LON_PROPERTY_KEY = "lon";	// Config parameter key: longitude property name for nodes in point layers
    public static final String LAT_PROPERTY_KEY = "lat";	// Config parameter key: latitude property name for nodes in point layers
    public static final String WKT_PROPERTY_KEY = "wkt";	// Config parameter key: wkt property name for nodes
    public static final String WKB_PROPERTY_KEY = "wkb";	// Config parameter key: wkb property name for nodes
    
    public static final String POINT_GEOMETRY_TYPE = "point";	// Config parameter value: Layer can contain points
    
    public static final String WITHIN_QUERY = "within";							// Query type
    public static final String WITHIN_WKT_GEOMETRY_QUERY = "withinWKTGeometry";	// Query type
    public static final String WITHIN_DISTANCE_QUERY = "withinDistance";		// Query type
    public static final String BBOX_QUERY = "bbox";								// Query type
    public static final String CQL_QUERY = "CQL";								// Query type (unused)
    
    public static final String ENVELOPE_PARAMETER = "envelope";					// Query parameter key: envelope for within query
    public static final String DISTANCE_IN_KM_PARAMETER = "distanceInKm";		// Query parameter key: distance for withinDistance query
    public static final String POINT_PARAMETER = "point";						// Query parameter key: relative to this point for withinDistance query
    
    private String nodeLookupIndexName;
    
    private final String layerName;
    private final GraphDatabaseService db;
    private SpatialDatabaseService spatialDB;
    private EditableLayer layer;
    private Index<Node> idLookup;
    
    /**
     * This implementation is going to create a new layer if there is no
     * existing one.
     * 
     * @param indexName
     * @param db
     * @param config
     */
    public LayerNodeIndex( String indexName, GraphDatabaseService db,
            Map<String, String> config )
    {
        this.layerName = indexName;
        this.db = db;
        this.nodeLookupIndexName = indexName + "__neo4j-spatial__LayerNodeIndex__internal__spatialNodeLookup__";
        this.idLookup = db.index().forNodes(nodeLookupIndexName);
        spatialDB = new SpatialDatabaseService( this.db );
        if ( config.containsKey( SpatialIndexProvider.GEOMETRY_TYPE )
             && POINT_GEOMETRY_TYPE.equals(config.get( SpatialIndexProvider.GEOMETRY_TYPE ))
             && config.containsKey( LayerNodeIndex.LAT_PROPERTY_KEY )
             && config.containsKey( LayerNodeIndex.LON_PROPERTY_KEY ) )
        {
            layer = (EditableLayer) spatialDB.getOrCreatePointLayer(
                    indexName,
                    config.get( LayerNodeIndex.LON_PROPERTY_KEY ),
                    config.get( LayerNodeIndex.LAT_PROPERTY_KEY ) );
        }
        else if ( config.containsKey( LayerNodeIndex.WKT_PROPERTY_KEY ) )
        {
            layer = (EditableLayer) spatialDB.getOrCreateEditableLayer(
                    indexName, "WKT", config.get( LayerNodeIndex.WKT_PROPERTY_KEY ) );
        }
        else if ( config.containsKey( LayerNodeIndex.WKB_PROPERTY_KEY ) )
        {
            layer = (EditableLayer) spatialDB.getOrCreateEditableLayer(
                    indexName, "WKB", config.get( LayerNodeIndex.WKB_PROPERTY_KEY ) );
        }
        else
        {
            throw new IllegalArgumentException( "Need to provide (geometry_type=point and lat/lon), wkt or wkb property config" );
        }
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

    @Override
    public void add( Node geometry, String key, Object value )
    {
        Geometry decodeGeometry = layer.getGeometryEncoder().decodeGeometry( geometry );

        // check if node already exists in layer
        Node matchingNode = findExistingNode( geometry );

        if (matchingNode == null)
        {
          SpatialDatabaseRecord newNode = layer.add(
                decodeGeometry, new String[] { "id" },
                new Object[] { geometry.getId() } );

	  // index geomNode with node of geometry
	  idLookup.add(newNode.getGeomNode(), "id", geometry.getId());
	}
        else
        {
          // update existing geoNode
          layer.update(matchingNode.getId(), decodeGeometry);      
        }

    }

    private Node findExistingNode( Node geometry ) {
	return idLookup.query("id", geometry.getId()).getSingle();
    }

    @Override
    public void remove( Node entity, String key, Object value )
    {
        remove( entity );
    }

    @Override
    public void delete()
    {
        spatialDB.deleteLayer( layer.getName(), new NullListener() );;
    }

    /**
     * Not supported at the moment
     */
    @Override
    public IndexHits<Node> get( String key, Object value )
    {
        return query( key, value );
    }

    @Override
    public IndexHits<Node> query( String key, Object params )
    {
        IndexHits<Node> results;
        // System.out.println( key + "," + params );
        if ( key.equals( WITHIN_QUERY ) )
        {
            Map<?, ?> p = (Map<?, ?>) params;
            Double[] bounds = (Double[]) p.get( ENVELOPE_PARAMETER );

            List<SpatialDatabaseRecord> res = GeoPipeline.startWithinSearch(
                    layer,
                    layer.getGeometryFactory().toGeometry(
                            new Envelope( bounds[0], bounds[1], bounds[2],
                                    bounds[3] ) ) ).toSpatialDatabaseRecordList();

            results = new SpatialRecordHits(res, layer);
            return results;
        }
        
        if ( key.equals( WITHIN_WKT_GEOMETRY_QUERY ) )
        {
            WKTReader reader = new WKTReader( layer.getGeometryFactory() );
            Geometry geometry;
            try
            {
                geometry = reader.read( (String)params);
                List<SpatialDatabaseRecord> res = GeoPipeline.startWithinSearch(
                        layer,geometry ).toSpatialDatabaseRecordList();
                
                results = new SpatialRecordHits(res, layer);
                return results;
            }
            catch ( com.vividsolutions.jts.io.ParseException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
        
        else if ( key.equals( WITHIN_DISTANCE_QUERY ) )
        {
            Double[] point = null;
            Double distance = null;
            
            // this one should enable distance searches using cypher query lang
            // by using: withinDistance:[7.0, 10.0, 100.0]  (long, lat. distance)
            if (params.getClass() == String.class)
            {
                try
                {
                    @SuppressWarnings("unchecked")
					List<Double> coordsAndDistance = (List<Double>) new JSONParser().parse( (String) params );
                    point = new Double[2];
                    point[0] = coordsAndDistance.get(0);
                    point[1] = coordsAndDistance.get(1);
                    distance = coordsAndDistance.get(2);
                }
                catch ( ParseException e )
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            
            else
            {
                Map<?, ?> p = (Map<?, ?>) params;
                point = (Double[]) p.get( POINT_PARAMETER );
                distance = (Double) p.get( DISTANCE_IN_KM_PARAMETER );
            }

            Coordinate start = new Coordinate(point[1], point[0]);
            List<GeoPipeFlow> res = GeoPipeline.startNearestNeighborLatLonSearch(
                    layer, start, distance).sort(
                    "OrthodromicDistance").toList();

            results = new GeoPipeFlowHits(res, layer);
            return results;
        }
        else if ( key.equals( BBOX_QUERY ) )
        {
            try
            {
                @SuppressWarnings("unchecked")
				List<Double> coords = (List<Double>) new JSONParser().parse( (String) params );

                List<SpatialDatabaseRecord> res = GeoPipeline.startWithinSearch(
                        layer,
                        layer.getGeometryFactory().toGeometry(
                                new Envelope( coords.get( 0 ), coords.get( 1 ),
                                        coords.get( 2 ), coords.get( 3 ) ) ) ).toSpatialDatabaseRecordList();

				results = new SpatialRecordHits(res, layer);
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
                    "only %s, %s and %s are implemented.", WITHIN_QUERY,
                    WITHIN_DISTANCE_QUERY, BBOX_QUERY ) );
        }
        return null;
    }
    
    @Override
    public IndexHits<Node> query( Object queryOrQueryObject )
    {

        String queryString = (String) queryOrQueryObject;
        IndexHits<Node> indexHits = query(queryString.substring(0, queryString.indexOf(":")),
                queryString.substring(queryString.indexOf(":") + 1));
        return indexHits;
    }

    @Override
    public void remove( Node node, String s )
    {
        remove(node);
    }

    @Override
    public void remove( Node node )
    {
        try {
            layer.removeFromIndex( node.getId() );
	    idLookup.remove(((SpatialDatabaseRecord) node).getGeomNode());
        } catch (Exception e) {
            //could not remove
        }
    }

    @Override
    public boolean isWriteable()
    {
        return true;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return db;
    }

    @Override
    public Node putIfAbsent( Node entity, String key, Object value )
    {
        throw new UnsupportedOperationException();
    }    
}
