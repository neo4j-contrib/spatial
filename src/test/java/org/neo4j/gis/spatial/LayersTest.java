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
package org.neo4j.gis.spatial;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.neo4j.gis.spatial.rtree.NullListener;
import org.neo4j.gis.spatial.encoders.SimpleGraphEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.encoders.SimplePropertyEncoder;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.pipes.GeoPipeline;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.LineString;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;


public class LayersTest extends Neo4jTestCase
{

    @Test
    public void testBasicLayerOperations()
    {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(
                graphDb() );
        Layer layer = spatialService.getLayer( "test" );
        assertNull( layer );
        layer = spatialService.createWKBLayer( "test" );
        assertNotNull( layer );
        assertTrue( "Should be a default layer", layer instanceof DefaultLayer );
        spatialService.deleteLayer( layer.getName(), new NullListener() );
        assertNull( spatialService.getLayer( layer.getName() ) );
    }

    @Test
    public void testBulkInsertion() throws Exception {

        File dbPath = new File("target/var/BulkTest");
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath.getCanonicalPath());

        SpatialDatabaseService sdbs = new SpatialDatabaseService(db);
        int N = 20000;
        int Q = 10;
        Random random = new Random();

        long totalTimeStart = System.currentTimeMillis();
        for(int j =0; j < Q; j++){
            System.out.println("BulkLoadingTestRun " + j);
            try(Transaction tx = db.beginTx()){
                ArrayList<Node> coords = new ArrayList<>(N);

                EditableLayer layer = sdbs.getOrCreatePointLayer("BulkLoader", "lat", "lon");

                for(int i=0; i<N; i++){
                    Node n = db.createNode(DynamicLabel.label("Coordinate"));
                    n.setProperty("lat", random.nextDouble()*90.0);
                    n.setProperty("lon", random.nextDouble()*90.0);
                    coords.add(n);
                    //                   layer.add(n);
                }
                long time = System.currentTimeMillis();

                layer.addAllNodes(coords);
                System.out.println("********************** time taken to load "+N+" records" + (System.currentTimeMillis() - time));
                tx.success();
            }
        }
        System.out.println("Total Time for "+(N*Q)+" Nodes in "+Q+" Batches of "+N+" is: ");
        System.out.println(((System.currentTimeMillis() - totalTimeStart)/1000) + " seconds");
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
        try (Transaction tx = graphDb().beginTx()) {
            List<SpatialDatabaseRecord> results = GeoPipeline
        	.startContainSearch(layer, layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)))
        	.toSpatialDatabaseRecordList();
        
            // should not be contained
            assertEquals( 0, results.size() );

            results = GeoPipeline
                .startWithinSearch(layer, layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)))
                .toSpatialDatabaseRecordList();

            assertEquals( 1, results.size() );
            tx.success();
        }
    }

    @Test
    public void testDeleteGeometry()
    {
        SpatialDatabaseService db = new SpatialDatabaseService( graphDb() );
        EditableLayer layer = (EditableLayer) db.createLayer("test", SimplePointEncoder.class, EditableLayerImpl.class, "lon:lat");
        assertNotNull( layer );
        SpatialDatabaseRecord record = layer.add( layer.getGeometryFactory().createPoint(
                new Coordinate( 15.3, 56.2 ) ) );
        assertNotNull( record );
        // try to remove the geometry
        layer.delete( record.getNodeId() );
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

        try (Transaction tx = graphDb().beginTx()) {

            // finds geometries that contain the given geometry
            List<SpatialDatabaseRecord> results = GeoPipeline
                .startContainSearch(layer, layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)))
                .toSpatialDatabaseRecordList();

            // should not be contained
            assertEquals( 0, results.size() );

            results = GeoPipeline
                .startWithinSearch(layer, layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)))
                .toSpatialDatabaseRecordList();

            assertEquals( 1, results.size() );
            tx.success();
        }
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
        /* MultiLineString test_way = */ layer.getGeometryFactory().createMultiLineString(
                test_way_segments );
        // Coordinate slussgatan14 = new Coordinate( 13.0181127, 55.608236 );
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

        // TODO this test is not complete

        try (Transaction tx = graphDb().beginTx()) {
            printResults(layer, GeoPipeline
                    .startIntersectSearch(layer, layer.getGeometryFactory().toGeometry(new Envelope(13.2, 14.1, 56.1, 56.2)))
                    .toSpatialDatabaseRecordList());

            printResults(layer, GeoPipeline
                    .startContainSearch(layer, layer.getGeometryFactory().toGeometry(new Envelope(12.0, 15.0, 55.0, 57.0)))
                    .toSpatialDatabaseRecordList());
            tx.success();
        }
        return layer;
    }

    private void printResults(Layer layer, List<SpatialDatabaseRecord> results)
    {
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
        try (Transaction tx = graphDb().beginTx())
        {
            for ( Layer layer : layers )
            {
                exporter.exportLayer( layer.getName() );
            }
            tx.success();
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
