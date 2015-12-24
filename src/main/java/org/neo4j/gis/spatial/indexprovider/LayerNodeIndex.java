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
import com.vividsolutions.jts.linearref.LocationIndexedLine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.GeodeticCalculator;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.neo4j.gis.spatial.rtree.NullListener;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.GeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;


public class LayerNodeIndex implements Index<Node>
{

	public static final String LON_PROPERTY_KEY = "lon";	// Config parameter key: longitude property name for nodes in point layers
	public static final String LAT_PROPERTY_KEY = "lat";	// Config parameter key: latitude property name for nodes in point layers
	public static final String WKT_PROPERTY_KEY = "wkt";	// Config parameter key: wkt property name for nodes
	public static final String WKB_PROPERTY_KEY = "wkb";	// Config parameter key: wkb property name for nodes

	public static final String POINT_GEOMETRY_TYPE = "point";	// Config parameter value: Layer can contain points

	public static final String WITHIN_QUERY = "within";									// Query type
	public static final String WITHIN_WKT_GEOMETRY_QUERY = "withinWKTGeometry";			// Query type
	public static final String WITHIN_DISTANCE_QUERY = "withinDistance";				// Query type
	public static final String BBOX_QUERY = "bbox";										// Query type
	public static final String LINE_LOCATE_POINT = "lineLocatePoint";					// Query type
	public static final String GET_CLOSEST_NODE = "getClosestNode";						// Query type
	public static final String GET_LAT_LON = "getLatLon";								// Query type
	public static final String GET_NODES_IN_INTERSECTION = "getNodesInIntersection";	// Query type
	public static final String CQL_QUERY = "CQL";										// Query type (unused)

	public static final String ENVELOPE_PARAMETER = "envelope";					// Query parameter key: envelope for within query
	public static final String DISTANCE_IN_KM_PARAMETER = "distanceInKm";		// Query parameter key: distance for withinDistance query
	public static final String POINT_PARAMETER = "point";						// Query parameter key: relative to this point for withinDistance query

	private final HashMap<Integer, String> wkt = new HashMap<Integer, String>(2);
	private final String wkt4326 = "GEOGCS[\"WGS 84\", "
			+ "DATUM[\"World Geodetic System 1984\", "
			+ "SPHEROID[\"WGS 84\", 6378137.0, 298.257223563, AUTHORITY[\"EPSG\",\"7030\"]], "
			+ "AUTHORITY[\"EPSG\",\"6326\"]], "
			+ "PRIMEM[\"Greenwich\", 0.0, AUTHORITY[\"EPSG\",\"8901\"]], "
			+ "UNIT[\"degree\", 0.017453292519943295], "
			+ "AXIS[\"Geodetic longitude\", EAST], "
			+ "AXIS[\"Geodetic latitude\", NORTH], "
			+ "AUTHORITY[\"EPSG\",\"4326\"]]";

	private final String wkt3857 = "PROJCS[\"WGS 84 / Pseudo-Mercator\", "
			+ "GEOGCS[\"WGS 84\", "
			+ "DATUM[\"World Geodetic System 1984\", "
			+ "SPHEROID[\"WGS 84\", 6378137.0, 298.257223563, AUTHORITY[\"EPSG\",\"7030\"]], "
			+ "AUTHORITY[\"EPSG\",\"6326\"]], "
			+ "PRIMEM[\"Greenwich\", 0.0, AUTHORITY[\"EPSG\",\"8901\"]], "
			+ "UNIT[\"degree\", 0.017453292519943295], "
			+ "AXIS[\"Geodetic longitude\", EAST], "
			+ "AXIS[\"Geodetic latitude\", NORTH], "
			+ "AUTHORITY[\"EPSG\",\"4326\"]], "
			+ "PROJECTION[\"Popular Visualisation Pseudo Mercator\", AUTHORITY[\"EPSG\",\"1024\"]], "
			+ "PARAMETER[\"semi_minor\", 6378137.0], "
			+ "PARAMETER[\"latitude_of_origin\", 0.0], "
			+ "PARAMETER[\"central_meridian\", 0.0],"
			+ "PARAMETER[\"scale_factor\", 1.0],"
			+ "PARAMETER[\"false_easting\", 0.0],"
			+ "PARAMETER[\"false_northing\", 0.0],"
			+ "UNIT[\"m\", 1.0],"
			+ "AXIS[\"Easting\", EAST],"
			+ "AXIS[\"Northing\", NORTH],"
			+ "AUTHORITY[\"EPSG\",\"3857\"]]";

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
		wkt.put(4326, wkt4326);
		wkt.put(3857, wkt3857);
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
			if(distance == 0.0){
				distance = 0.01;
			}
			List<GeoPipeFlow> res = GeoPipeline.startNearestNeighborSearch(
					layer, start, distance).sort(
							"Distance").toList();

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
		else if ( key.equals( LINE_LOCATE_POINT ) )
		{

			final String prefix;
			final String pointString;
			final int pointSRID;
			final int mode;
			final int databaseSRID;
			try
			{
				@SuppressWarnings("unchecked")
				List<String> args = (List<String>) new JSONParser().parse( (String) params );
				prefix = (String) args.get(0);
				pointString = (String) args.get(1);
				pointSRID = Integer.parseInt(args.get(2));
				mode = Integer.parseInt(args.get(3));

				ResourceIterator<Node> SRIDNodes = db.findNodes(DynamicLabel.label(prefix + "_SRID"));
				if(SRIDNodes.hasNext()){
					databaseSRID = Integer.parseInt(SRIDNodes.next().getProperty("srid").toString());
				} else {
					databaseSRID = 3857;
				}

				ResourceIterator<Node> edgeNodes = db.findNodes(DynamicLabel.label(prefix + "_Edge"), "mode", mode);
				final WKTReader wktReader = new WKTReader(layer.getGeometryFactory());
				Geometry geomPoint = wktReader.read(pointString);

				if (databaseSRID != pointSRID)  {
					System.setProperty("org.geotools.referencing.forceXY", "true");
					final CoordinateReferenceSystem sourceCRS = CRS.parseWKT(wkt.get(pointSRID));
					final CoordinateReferenceSystem targetCRS = CRS.parseWKT(wkt.get(databaseSRID));
					final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, false);
					geomPoint = JTS.transform(geomPoint, transform);
				}

				final GeometryEncoder encoder = layer.getGeometryEncoder();
				List<SpatialDatabaseRecord> list = new ArrayList<SpatialDatabaseRecord>();
				while(edgeNodes.hasNext()){
					Node edgeNode = edgeNodes.next();
					Geometry geometry = encoder.decodeGeometry( edgeNode );

					double distance = geomPoint.distance(geometry);
					LocationIndexedLine line = new LocationIndexedLine(geometry);        		
					double geometryLength = geometry.getLength();
					Coordinate closestPoint = line.extractPoint(line.project(geomPoint.getCoordinate()));
					Coordinate[] coordinates = geometry.getCoordinates();
					double offset = 0;
					for(int i = 0; i < coordinates.length-1; i++ ){
						double distanceToClosest = coordinates[i].distance(closestPoint);
						double distanceToNext = coordinates[i].distance(coordinates[i+1]);
						if(distanceToClosest < distanceToNext){
							offset += distanceToClosest;
							offset = offset / geometryLength;
							edgeNode.setProperty("distance", distance);
							edgeNode.setProperty("offset", offset);
							list.add(new SpatialDatabaseRecord(layer, findExistingNode(edgeNode)));
							break;
						}else{
							offset += distanceToNext;
						}
					}
				}

				results = new SpatialRecordHits(list, layer);
				return results;
			} catch (ParseException e ) {
				e.printStackTrace();
			} catch (com.vividsolutions.jts.io.ParseException e) {
				e.printStackTrace();
			} catch (FactoryException e) {
				e.printStackTrace();
			} catch (MismatchedDimensionException e) {
				e.printStackTrace();
			} catch (TransformException e) {
				e.printStackTrace();
			} 
		}
		else if ( key.equals( GET_CLOSEST_NODE ) )
		{
			final String node_label;
			final String pointString;
			final int mode;
			try
			{
				@SuppressWarnings("unchecked")
				List<String> args = (List<String>) new JSONParser().parse( (String) params );
				node_label = (String) args.get(0);
				pointString = (String) args.get(1);
				mode = Integer.parseInt(args.get(2));
				ResourceIterator<Node> nodes;
				if(mode == -1){
					nodes = db.findNodes(DynamicLabel.label(node_label));
				}else{
					nodes = db.findNodes(DynamicLabel.label(node_label), "mode", mode);
				}
				final WKTReader wktReader = new WKTReader(layer.getGeometryFactory());
				Geometry geomPoint = wktReader.read(pointString);
				Node closestNode = null;
				double closestDistance = Double.MAX_VALUE;
				System.setProperty("org.geotools.referencing.forceXY", "true");
				final CoordinateReferenceSystem sourceCRS = CRS.parseWKT(wkt4326);
				final CoordinateReferenceSystem targetCRS = CRS.parseWKT(wkt3857);
				MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, false);
				geomPoint = JTS.transform(geomPoint, transform);
				while(nodes.hasNext()){
					Node node = nodes.next();
					if(node.hasProperty("geometry")){
						Geometry geometry = layer.getGeometryEncoder().decodeGeometry( node );
						double distance = geometry.distance(geomPoint);
						if(distance < closestDistance){
							closestNode = node;
							closestDistance = distance;
						}
					}
				}
				List<SpatialDatabaseRecord> list = new ArrayList<SpatialDatabaseRecord>();
				list.add(new SpatialDatabaseRecord(layer, findExistingNode(closestNode)));

				results = new SpatialRecordHits(list, layer);
				return results;
			} catch ( ParseException e ) {
				e.printStackTrace();
			} catch ( com.vividsolutions.jts.io.ParseException e ) {
				e.printStackTrace();
			} catch (NoSuchAuthorityCodeException e) {
				e.printStackTrace();
			} catch (FactoryException e) {
				e.printStackTrace();
			} catch (MismatchedDimensionException e) {
				e.printStackTrace();
			} catch (TransformException e) {
				e.printStackTrace();
			} 
		} else if ( key.equals( GET_LAT_LON ) )
		{

			final String node_label;
			final int nodeId;
			try
			{
				@SuppressWarnings("unchecked")
				List<String> args = (List<String>) new JSONParser().parse( (String) params );
				node_label = (String) args.get(0);
				nodeId =  Integer.parseInt(args.get(1));

				ResourceIterator<Node> nodes;

				nodes = db.findNodes(DynamicLabel.label(node_label), "id", nodeId);
				if(!nodes.hasNext()){
					return null;
				}
				Node node = nodes.next();
				Geometry geometry = layer.getGeometryEncoder().decodeGeometry( node );

				Coordinate coords = geometry.getCoordinate();
				node.setProperty("lat", coords.x);
				node.setProperty("lon", coords.y);
				List<SpatialDatabaseRecord> list = new ArrayList<SpatialDatabaseRecord>();
				list.add(new SpatialDatabaseRecord(layer, findExistingNode(node)));
				results = new SpatialRecordHits(list, layer);
				return results;
			}
			catch ( ParseException e )
			{
				e.printStackTrace();
			}
		}
		else if ( key.equals( GET_NODES_IN_INTERSECTION ) )
		{

			final String prefix;
			final String pointString;
			final double distance;
			final String differencePointString;
			final double differenceDistance;
			final String unionPointString;
			final double unionDistance;
			try
			{
				@SuppressWarnings("unchecked")
				List<String> args = (List<String>) new JSONParser().parse( (String) params );
				prefix = args.get(0);
				pointString =  args.get(1);
				distance = Double.parseDouble(args.get(2));
				differencePointString = args.get(3);
				differenceDistance = Double.parseDouble(args.get(4));
				unionPointString = args.get(5);
				unionDistance = args.get(6).equals("") ? 0.0 : Double.parseDouble(args.get(6));
								
				ResourceIterator<Node> nodes;

				nodes = db.findNodes(DynamicLabel.label(prefix + "_Node"));
				if(!nodes.hasNext()){
					return null;
				}
				
				final WKTReader wktReader = new WKTReader(layer.getGeometryFactory());
				Geometry geomPoint = wktReader.read(pointString);
				Geometry buffer = geomPoint.buffer(distance);

				Geometry differencePoint = wktReader.read(differencePointString);
				Geometry differenceBuffer = differencePoint.buffer(differenceDistance);

				if(!unionPointString.equals("")){
					Geometry unionPoint = wktReader.read(unionPointString);
					Geometry unionBuffer = unionPoint.buffer(unionDistance);
					differenceBuffer= differenceBuffer.union(unionBuffer);
				}
				Geometry difference = buffer.difference(differenceBuffer);

				List<SpatialDatabaseRecord> list = new ArrayList<SpatialDatabaseRecord>();
				while(nodes.hasNext()){
					Node node = nodes.next();
					if(node.hasProperty("geometry")){
						Geometry geometry = layer.getGeometryEncoder().decodeGeometry( node );
						if(difference.intersects(geometry)){
							list.add(new SpatialDatabaseRecord(layer, findExistingNode(node)));
						}
					}
				}
				results = new SpatialRecordHits(list, layer);
				return results;
			}
			catch ( ParseException e )
			{
				e.printStackTrace();
			} catch (com.vividsolutions.jts.io.ParseException e) {
				e.printStackTrace();
			}catch (MismatchedDimensionException e) {
				e.printStackTrace();
			}
		}
		else
		{
			throw new UnsupportedOperationException( String.format(
					"only %s, %s, %s, %s, %s, %s and %s are implemented.", WITHIN_QUERY,
					WITHIN_DISTANCE_QUERY, BBOX_QUERY, LINE_LOCATE_POINT, GET_CLOSEST_NODE, 
					GET_LAT_LON, GET_NODES_IN_INTERSECTION ) );
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
