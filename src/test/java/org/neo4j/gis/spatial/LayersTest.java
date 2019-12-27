/*
 * Copyright (c) 2010-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j Spatial.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LineString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.gis.spatial.encoders.NativePointEncoder;
import org.neo4j.gis.spatial.encoders.SimpleGraphEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.encoders.SimplePropertyEncoder;
import org.neo4j.gis.spatial.index.LayerGeohashPointIndex;
import org.neo4j.gis.spatial.index.LayerIndexReader;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.gis.spatial.rtree.ProgressLoggingListener;
import org.neo4j.graphdb.*;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class LayersTest {
    private GraphDatabaseService graphDb;

    @Before
    public void setup() throws KernelException {
        graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(SpatialProcedures.class);
    }

    @After
    public void teardown() {
        graphDb.shutdown();
    }

    @Test
    public void testBasicLayerOperations() {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb);
        Layer layer = spatialService.getLayer("test");
        assertNull(layer);
        layer = spatialService.createWKBLayer("test");
        assertNotNull(layer);
        assertThat("Should be a default layer", layer instanceof DefaultLayer);
        spatialService.deleteLayer(layer.getName(), new ProgressLoggingListener("deleting layer '" + layer.getName() + "'", System.out));
        assertNull(spatialService.getLayer(layer.getName()));
    }

    @Test
    public void testSimplePointLayerWithRTree() {
        testPointLayer(LayerRTreeIndex.class, SimplePointEncoder.class);
    }

    @Test
    public void testSimplePointLayerWithGeohash() {
        testPointLayer(LayerGeohashPointIndex.class, SimplePointEncoder.class);
    }

    @Test
    public void testNativePointLayerWithRTree() {
        testPointLayer(LayerRTreeIndex.class, NativePointEncoder.class);
    }

    @Test
    public void testNativePointLayerWithGeohash() {
        testPointLayer(LayerGeohashPointIndex.class, NativePointEncoder.class);
    }

    private void testPointLayer(Class<? extends LayerIndexReader> indexClass, Class<? extends GeometryEncoder> encoderClass) {
        SpatialDatabaseService db = new SpatialDatabaseService(graphDb);
        EditableLayer layer = (EditableLayer) db.createLayer("test", encoderClass, EditableLayerImpl.class, indexClass, null);
        assertNotNull(layer);
        SpatialDatabaseRecord record = layer.add(layer.getGeometryFactory().createPoint(
                new Coordinate(15.3, 56.2)));
        assertNotNull(record);
        // finds geometries that contain the given geometry
        try (Transaction tx = graphDb.beginTx()) {
            List<SpatialDatabaseRecord> results = GeoPipeline
                    .startContainSearch(layer, layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)))
                    .toSpatialDatabaseRecordList();

            // should not be contained
            assertEquals(0, results.size());

            results = GeoPipeline
                    .startWithinSearch(layer, layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)))
                    .toSpatialDatabaseRecordList();

            assertEquals(1, results.size());
            tx.success();
        }
        db.deleteLayer(layer.getName(), new ProgressLoggingListener("deleting layer '" + layer.getName() + "'", System.out));
        assertNull(db.getLayer(layer.getName()));
    }

    @Test
    public void testDeleteSimplePointGeometry() {
        testDeleteGeometry(SimplePointEncoder.class);
    }

    @Test
    public void testDeleteNativePointGeometry() {
        testDeleteGeometry(NativePointEncoder.class);
    }

    private void testDeleteGeometry(Class<? extends GeometryEncoder> encoderClass) {
        SpatialDatabaseService db = new SpatialDatabaseService(graphDb);
        EditableLayer layer = (EditableLayer) db.createLayer("test", encoderClass, EditableLayerImpl.class, null, null);
        assertNotNull(layer);
        SpatialDatabaseRecord record = layer.add(layer.getGeometryFactory().createPoint(
                new Coordinate(15.3, 56.2)));
        assertNotNull(record);
        // try to remove the geometry
        layer.delete(record.getNodeId());
    }

    @Test
    public void testEditableLayer() {
        SpatialDatabaseService db = new SpatialDatabaseService(graphDb);
        EditableLayer layer = (EditableLayer) db.getOrCreateEditableLayer("test");
        assertNotNull(layer);
        SpatialDatabaseRecord record = layer.add(layer.getGeometryFactory().createPoint(
                new Coordinate(15.3, 56.2)));
        assertNotNull(record);

        try (Transaction tx = graphDb.beginTx()) {

            // finds geometries that contain the given geometry
            List<SpatialDatabaseRecord> results = GeoPipeline
                    .startContainSearch(layer, layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)))
                    .toSpatialDatabaseRecordList();

            // should not be contained
            assertEquals(0, results.size());

            results = GeoPipeline
                    .startWithinSearch(layer, layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)))
                    .toSpatialDatabaseRecordList();

            assertEquals(1, results.size());
            tx.success();
        }
    }

    @Test
    public void testSnapToLine() {
        SpatialDatabaseService db = new SpatialDatabaseService(graphDb);
        EditableLayer layer = (EditableLayer) db.getOrCreateEditableLayer("roads");
        Coordinate crossing_bygg_forstadsgatan = new Coordinate(13.0171471, 55.6074148);
        Coordinate[] waypoints_forstadsgatan = {new Coordinate(13.0201511, 55.6066846), crossing_bygg_forstadsgatan};
        LineString ostra_forstadsgatan_malmo = layer.getGeometryFactory().createLineString(waypoints_forstadsgatan);
        Coordinate[] waypoints_byggmastaregatan = {crossing_bygg_forstadsgatan, new Coordinate(13.0182092, 55.6088238)};
        LineString byggmastaregatan_malmo = layer.getGeometryFactory().createLineString(waypoints_byggmastaregatan);
        LineString[] test_way_segments = {byggmastaregatan_malmo, ostra_forstadsgatan_malmo};
        /* MultiLineString test_way = */
        layer.getGeometryFactory().createMultiLineString(test_way_segments);
        // Coordinate slussgatan14 = new Coordinate( 13.0181127, 55.608236 );
        //TODO now determine the nearest point on test_way to slussis

    }

    @Test
    public void testEditableLayers() {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb);
        testSpecificEditableLayer(spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test dynamic layer with property encoder",
                        SimplePropertyEncoder.class, DynamicLayer.class));
        testSpecificEditableLayer(spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test dynamic layer with graph encoder",
                        SimpleGraphEncoder.class, DynamicLayer.class));
        testSpecificEditableLayer(spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test OSM layer with OSM encoder",
                        OSMGeometryEncoder.class, OSMLayer.class));
        testSpecificEditableLayer(spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test editable layer with property encoder",
                        SimplePropertyEncoder.class, EditableLayerImpl.class));
        testSpecificEditableLayer(spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test editable layer with graph encoder",
                        SimpleGraphEncoder.class, EditableLayerImpl.class));
        testSpecificEditableLayer(spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test editable layer with OSM encoder",
                        OSMGeometryEncoder.class, EditableLayerImpl.class));
    }

    private Layer testSpecificEditableLayer(SpatialDatabaseService spatialService, EditableLayer layer) {
        assertNotNull(layer);
        assertTrue("Should be a dynamic layer", layer instanceof EditableLayer);
        layer = (EditableLayer) spatialService.getLayer(layer.getName());
        assertNotNull(layer);
        assertTrue("Should be a dynamic layer", layer instanceof EditableLayer);

        CoordinateList coordinates = new CoordinateList();
        coordinates.add(new Coordinate(13.1, 56.2), false);
        coordinates.add(new Coordinate(13.2, 56.0), false);
        coordinates.add(new Coordinate(13.3, 56.2), false);
        coordinates.add(new Coordinate(13.2, 56.0), false);
        coordinates.add(new Coordinate(13.1, 56.2), false);
        coordinates.add(new Coordinate(13.0, 56.0), false);
        layer.add(layer.getGeometryFactory().createLineString(
                coordinates.toCoordinateArray()));

        coordinates = new CoordinateList();
        coordinates.add(new Coordinate(14.1, 56.0), false);
        coordinates.add(new Coordinate(14.3, 56.1), false);
        coordinates.add(new Coordinate(14.2, 56.1), false);
        coordinates.add(new Coordinate(14.0, 56.0), false);
        layer.add(layer.getGeometryFactory().createLineString(
                coordinates.toCoordinateArray()));

        // TODO this test is not complete

        try (Transaction tx = graphDb.beginTx()) {
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

    private void printResults(Layer layer, List<SpatialDatabaseRecord> results) {
        System.out.println("\tTesting layer '" + layer.getName() + "' (class "
                + layer.getClass() + "), found results: "
                + results.size());
        for (SpatialDatabaseRecord r : results) {
            System.out.println("\t\tGeometry: " + r);
        }
    }

    @Test
    public void testShapefileExport() throws Exception {
        ShapefileExporter exporter = new ShapefileExporter(graphDb);
        exporter.setExportDir("target/export");
        ArrayList<Layer> layers = new ArrayList<Layer>();
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb);

        layers.add(testSpecificEditableLayer(spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test dynamic layer with property encoder",
                        SimplePropertyEncoder.class, DynamicLayer.class)));
        layers.add(testSpecificEditableLayer(spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test dynamic layer with graph encoder",
                        SimpleGraphEncoder.class, DynamicLayer.class)));
        layers.add(testSpecificEditableLayer(spatialService,
                (EditableLayer) spatialService.createLayer(
                        "test dynamic layer with OSM encoder",
                        OSMGeometryEncoder.class, OSMLayer.class)));
        Exception osmExportException = null;
        try (Transaction tx = graphDb.beginTx()) {
            for (Layer layer : layers) {
                exporter.exportLayer(layer.getName());
            }
            tx.success();
        } catch (Exception e) {
            if (e.getMessage().contains(
                    "org.locationtech.jts.geom.Geometry")) {
                osmExportException = e;
            } else {
                throw e;
            }
        }
        assertNotNull(
                "Missing expected shapefile export exception from multi-geometry OSM layer",
                osmExportException);
    }

    @Test
    public void testIndexAccessAfterBulkInsertion() throws Exception {
        // Use these two lines if you want to examine the output.
//        File dbPath = new File("target/var/BulkTest");
//        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath.getCanonicalPath());
        GraphDatabaseService db = graphDb;
        SpatialDatabaseService sdbs = new SpatialDatabaseService(db);
        EditableLayer layer = sdbs.getOrCreateSimplePointLayer("Coordinates", "rtree", "lat", "lon");

        Random rand = new Random();

        try (Transaction tx = db.beginTx()) {
            List<Node> coordinateNodes = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                Node node = db.createNode();
                node.addLabel(Label.label("Coordinates"));
                node.setProperty("lat", rand.nextDouble());
                node.setProperty("lon", rand.nextDouble());
                coordinateNodes.add(node);
            }
            layer.addAll(coordinateNodes);
            tx.success();
        }

        try (Transaction tx = db.beginTx()) { // 'points',{longitude:15.0,latitude:60.0},100
            Result result = db.execute("CALL spatial.withinDistance('Coordinates',{longitude:0.5, latitude:0.5},1000.0) YIELD node AS malmo");
            int i = 0;
            ResourceIterator thing = result.columnAs("malmo");
            while (thing.hasNext()) {
                assertNotNull(thing.next());
                i++;
            }
            assertEquals(i, 1000);
            tx.success();
        }

        try (Transaction tx = db.beginTx()) {
            String cypher = "MATCH ()-[:RTREE_ROOT]->(n)\n" +
                    "MATCH (n)-[:RTREE_CHILD]->(m)-[:RTREE_REFERENCE]->(p)\n" +
                    "RETURN count(p)";
            Result result = db.execute(cypher);
//           System.out.println(result.columns().toString());
            Object obj = result.columnAs("count(p)").next();
            assertTrue(obj instanceof Long);
            assertEquals(1000L, (long) ((Long) obj));
            tx.success();
        }

        db.shutdown();
    }

}
