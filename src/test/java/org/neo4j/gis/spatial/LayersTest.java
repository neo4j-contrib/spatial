package org.neo4j.gis.spatial;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.neo4j.gis.spatial.encoders.SimpleGraphEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.encoders.SimplePropertyEncoder;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.query.SearchContain;
import org.neo4j.gis.spatial.query.SearchIntersect;
import org.neo4j.gis.spatial.query.SearchWithin;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;

public class LayersTest extends Neo4jTestCase
{

    @Test
    public void testBasicLayerOperations()
    {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(
                graphDb() );
        Layer layer = spatialService.getLayer( "test" );
        assertNull( layer );
        layer = spatialService.createLayer( "test" );
        assertNotNull( layer );
        assertTrue( "Should be a default layer", layer instanceof DefaultLayer );
        spatialService.deleteLayer( layer.getName(), new NullListener() );
        assertNull( spatialService.getLayer( layer.getName() ) );
    }

    @Test
    public void testPointLayer()
    {
        SpatialDatabaseService db = new SpatialDatabaseService( graphDb() );
        EditableLayer layer = (EditableLayer) db.createLayer("test", SimplePointEncoder.class, EditableLayerImpl.class, "lon:lat");
        assertNotNull( layer );
        SpatialDatabaseRecord record = layer.add( layer.getGeometryFactory().createPoint(
                new Coordinate( 15.3, 56.2 ) ) );
        assertNotNull( record );
        // finds geometries that contain the given geometry
        SearchContain searchQuery = new SearchContain(
                layer.getGeometryFactory().toGeometry(
                        new Envelope( 15.0, 16.0, 56.0, 57.0 ) ) );
        layer.getIndex().executeSearch( searchQuery );
        List<SpatialDatabaseRecord> results = searchQuery.getResults();
        // should not be contained
        assertEquals( 0, results.size() );
        SearchWithin withinQuery = new SearchWithin(
                layer.getGeometryFactory().toGeometry(
                        new Envelope( 15.0, 16.0, 56.0, 57.0 ) ) );
        layer.getIndex().executeSearch( withinQuery );
        results = withinQuery.getResults();
        assertEquals( 1, results.size() );
    }

    @Test
    public void testEditableLayer()
    {
        SpatialDatabaseService db = new SpatialDatabaseService( graphDb() );
        EditableLayer layer = (EditableLayer) db.getOrCreateEditableLayer( "test" );
        assertNotNull( layer );
        SpatialDatabaseRecord record = layer.add( layer.getGeometryFactory().createPoint(
                new Coordinate( 15.3, 56.2 ) ) );
        assertNotNull( record );
        // finds geometries that contain the given geometry
        SearchContain searchQuery = new SearchContain(
                layer.getGeometryFactory().toGeometry(
                        new Envelope( 15.0, 16.0, 56.0, 57.0 ) ) );
        layer.getIndex().executeSearch( searchQuery );
        List<SpatialDatabaseRecord> results = searchQuery.getResults();
        // should not be contained
        assertEquals( 0, results.size() );
        SearchWithin withinQuery = new SearchWithin(
                layer.getGeometryFactory().toGeometry(
                        new Envelope( 15.0, 16.0, 56.0, 57.0 ) ) );
        layer.getIndex().executeSearch( withinQuery );
        results = withinQuery.getResults();
        assertEquals( 1, results.size() );
    }

    @Test
    public void testSnapToLine()
    {
        SpatialDatabaseService db = new SpatialDatabaseService( graphDb() );
        EditableLayer layer = (EditableLayer) db.getOrCreateEditableLayer( "roads" );
        Coordinate crossing_bygg_förstadsgatan = new Coordinate( 13.0171471,
                55.6074148 );
        Coordinate[] waypoints_förstadsgatan = {
                new Coordinate( 13.0201511, 55.6066846 ),
                crossing_bygg_förstadsgatan };
        LineString östra_förstadsgatan_malmö = layer.getGeometryFactory().createLineString(
                waypoints_förstadsgatan );
        Coordinate[] waypoints_byggmästaregatan = {
                crossing_bygg_förstadsgatan,
                new Coordinate( 13.0182092, 55.6088238 ) };
        LineString byggmästaregatan_malmö = layer.getGeometryFactory().createLineString(
                waypoints_byggmästaregatan );
        LineString[] test_way_segments = { byggmästaregatan_malmö,
                östra_förstadsgatan_malmö };
        MultiLineString test_way = layer.getGeometryFactory().createMultiLineString(
                test_way_segments );
        Coordinate slussgatan14 = new Coordinate( 13.0181127, 55.608236 );
        //TODO now determine the nearest point on test_way to slussis

    }

    @Test
    public void testEditableLayers()
    {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(
                graphDb() );
        testSpecificEditableLayer( spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test dynamic layer with property encoder",
                        SimplePropertyEncoder.class, DynamicLayer.class ) );
        testSpecificEditableLayer( spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test dynamic layer with graph encoder",
                        SimpleGraphEncoder.class, DynamicLayer.class ) );
        testSpecificEditableLayer( spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test OSM layer with OSM encoder",
                        OSMGeometryEncoder.class, OSMLayer.class ) );
        testSpecificEditableLayer( spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test editable layer with property encoder",
                        SimplePropertyEncoder.class, EditableLayerImpl.class ) );
        testSpecificEditableLayer( spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test editable layer with graph encoder",
                        SimpleGraphEncoder.class, EditableLayerImpl.class ) );
        testSpecificEditableLayer( spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test editable layer with OSM encoder",
                        OSMGeometryEncoder.class, EditableLayerImpl.class ) );
    }

    private Layer testSpecificEditableLayer(
            SpatialDatabaseService spatialService, EditableLayer layer )
    {
        assertNotNull( layer );
        assertTrue( "Should be a dynamic layer", layer instanceof EditableLayer );
        layer = (EditableLayer) spatialService.getLayer( layer.getName() );
        assertNotNull( layer );
        assertTrue( "Should be a dynamic layer", layer instanceof EditableLayer );

        CoordinateList coordinates = new CoordinateList();
        coordinates.add( new Coordinate( 13.1, 56.2 ), false );
        coordinates.add( new Coordinate( 13.2, 56.0 ), false );
        coordinates.add( new Coordinate( 13.3, 56.2 ), false );
        coordinates.add( new Coordinate( 13.2, 56.0 ), false );
        coordinates.add( new Coordinate( 13.1, 56.2 ), false );
        coordinates.add( new Coordinate( 13.0, 56.0 ), false );
        layer.add( layer.getGeometryFactory().createLineString(
                coordinates.toCoordinateArray() ) );

        coordinates = new CoordinateList();
        coordinates.add( new Coordinate( 14.1, 56.0 ), false );
        coordinates.add( new Coordinate( 14.3, 56.1 ), false );
        coordinates.add( new Coordinate( 14.2, 56.1 ), false );
        coordinates.add( new Coordinate( 14.0, 56.0 ), false );
        layer.add( layer.getGeometryFactory().createLineString(
                coordinates.toCoordinateArray() ) );

        doSearch(
                layer,
                new SearchIntersect( layer.getGeometryFactory().toGeometry(
                        new Envelope( 13.2, 14.1, 56.1, 56.2 ) ) ) );
        doSearch(
                layer,
                new SearchContain( layer.getGeometryFactory().toGeometry(
                        new Envelope( 12.0, 15.0, 55.0, 57.0 ) ) ) );

        // spatialService.deleteLayer(layer.getName(), new NullListener());
        // assertNull(spatialService.getLayer(layer.getName()));

        return layer;
    }

    private void doSearch( Layer layer, Search searchQuery )
    {
        System.out.println( "Testing search intersection:" );
        layer.getIndex().executeSearch( searchQuery );
        List<SpatialDatabaseRecord> results = searchQuery.getResults();
        System.out.println( "\tTesting layer '" + layer.getName() + "' (class "
                            + layer.getClass() + "), found results: "
                            + results.size() );
        for ( SpatialDatabaseRecord r : results )
        {
            System.out.println( "\t\tGeometry: " + r );
        }
    }

    @Test
    public void testShapefileExport() throws Exception
    {
        ShapefileExporter exporter = new ShapefileExporter( graphDb() );
        exporter.setExportDir( "target/export" );
        ArrayList<Layer> layers = new ArrayList<Layer>();
        SpatialDatabaseService spatialService = new SpatialDatabaseService(
                graphDb() );
        layers.add( testSpecificEditableLayer( spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test dynamic layer with property encoder",
                        SimplePropertyEncoder.class, DynamicLayer.class ) ) );
        layers.add( testSpecificEditableLayer( spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test dynamic layer with graph encoder",
                        SimpleGraphEncoder.class, DynamicLayer.class ) ) );
        layers.add( testSpecificEditableLayer( spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test dynamic layer with OSM encoder",
                        OSMGeometryEncoder.class, OSMLayer.class ) ) );
        Exception osmExportException = null;
        try
        {
            for ( Layer layer : layers )
            {
                exporter.exportLayer( layer.getName() );
            }
        }
        catch ( Exception e )
        {
            if ( e.getMessage().contains(
                    "com.vividsolutions.jts.geom.Geometry" ) )
            {
                osmExportException = e;
            }
            else
            {
                throw e;
            }
        }
        assertNotNull(
                "Missing expected shapefile export exception from multi-geometry OSM layer",
                osmExportException );
    }
}
