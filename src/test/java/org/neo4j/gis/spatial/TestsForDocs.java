/*
 * Copyright (c) 2010-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.geotools.data.DataStore;
import org.geotools.data.neo4j.Neo4jSpatialDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.gis.spatial.index.LayerIndexReader;
import org.neo4j.gis.spatial.osm.OSMDataset;
import org.neo4j.gis.spatial.osm.OSMDataset.Way;
import org.neo4j.gis.spatial.osm.OSMDataset.WayPoint;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

/**
 * Some test code written specifically for the user manual. This normally means
 * we repeat some of the setup/teardown code in each test so that it is a more
 * complete explanation of how to write the code. Most other test classes rely
 * on infrastructure in the Neo4jTestCase that is unlikely to be relevant to the
 * users own coding experience.
 */
public class TestsForDocs {
    private DatabaseManagementService databases;
    private GraphDatabaseService graphDb;

    @Before
    public void setUp() throws Exception {
        this.databases = new DatabaseManagementServiceBuilder(new File("target/docs-db")).build();
        this.graphDb = databases.database(DEFAULT_DATABASE_NAME);
    }

    @After
    public void tearDown() {
        this.databases.shutdown();
        this.databases = null;
        this.graphDb = null;
    }

    private void checkIndexAndFeatureCount(String layerName) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {
            SpatialDatabaseService spatial = new SpatialDatabaseService(graphDb);
            Layer layer = spatial.getLayer(tx, layerName);
            if (layer.getIndex().count(tx) < 1) {
                System.out.println("Warning: index count zero: " + layer.getName());
            }
            System.out.println("Layer '" + layer.getName() + "' has " + layer.getIndex().count(tx) + " entries in the index");
            DataStore store = new Neo4jSpatialDataStore(graphDb);
            SimpleFeatureCollection features = store.getFeatureSource(layer.getName()).getFeatures();
            System.out.println("Layer '" + layer.getName() + "' has " + features.size() + " features");
            assertEquals("FeatureCollection.size for layer '" + layer.getName() + "' not the same as index count", layer.getIndex().count(tx), features.size());
            if (layer instanceof OSMLayer)
                checkOSMAPI(tx, layer);
            tx.commit();
        }
    }

    private void checkOSMAPI(Transaction tx, Layer layer) {
        HashMap<Long, Integer> waysFound = new HashMap<>();
        long mostCommon = 0;
        int mostCount = 0;
        OSMDataset osm = (OSMDataset) layer.getDataset();
        Node wayNode = osm.getAllWayNodes(tx).iterator().next();
        Way way = osm.getWayFrom(wayNode);
        System.out.println("Got first way " + way);
        for (WayPoint n : way.getWayPoints()) {
            Way w = n.getWay();
            Long wayId = w.getNode().getId();
            if (!waysFound.containsKey(wayId)) {
                waysFound.put(wayId, 0);
            }
            waysFound.put(wayId, waysFound.get(wayId) + 1);
            if (waysFound.get(wayId) > mostCount) {
                mostCommon = wayId;
                mostCount = waysFound.get(wayId);
            }
        }
        System.out.println("Found " + waysFound.size() + " ways overlapping '" + way.toString() + "'");
        for (long wayId : waysFound.keySet()) {
            System.out.println("\t" + wayId + ":\t" + waysFound.get(wayId) +
                    ((wayId == way.getNode().getId()) ? "\t(original way)" : ""));
        }
        assertTrue("Start way should be most found way", way.equals(osm.getWayFromId(tx, mostCommon)));
    }

    private void importMapOSM(GraphDatabaseService db) throws Exception {
        // START SNIPPET: importOsm tag::importOsm[]
        OSMImporter importer = new OSMImporter("map.osm");
        importer.setCharset(StandardCharsets.UTF_8);
        importer.importFile(db, "map.osm");
        importer.reIndex(db);
        // END SNIPPET: importOsm end::importOsm[]
    }

    /**
     * Sample code for importing Open Street Map example.
     */
    @Test
    public void testImportOSM() throws Exception {
        System.out.println("\n=== Simple test map.osm ===");
        importMapOSM(graphDb);
        GraphDatabaseService database = graphDb;
        // START SNIPPET: searchBBox tag::searchBBox[]
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb);
        try (Transaction tx = database.beginTx()) {
            Layer layer = spatialService.getLayer(tx, "map.osm");
            LayerIndexReader spatialIndex = layer.getIndex();
            System.out.println("Have " + spatialIndex.count(tx) + " geometries in " + spatialIndex.getBoundingBox(tx));

            Envelope bbox = new Envelope(12.94, 12.96, 56.04, 56.06);
            List<SpatialDatabaseRecord> results = GeoPipeline
                    .startIntersectWindowSearch(tx, layer, bbox)
                    .toSpatialDatabaseRecordList();

            doGeometryTestsOnResults(bbox, results);
            tx.commit();
        }
        // END SNIPPET: searchBBox end::searchBBox[]

        checkIndexAndFeatureCount("map.osm");
    }

    @Test
    public void testImportShapefile() throws Exception {
        System.out.println("\n=== Test Import Shapefile ===");
        GraphDatabaseService database = graphDb;

        // START SNIPPET: importShapefile tag::importShapefile[]
        ShapefileImporter importer = new ShapefileImporter(database);
        importer.importFile("shp/highway.shp", "highway", StandardCharsets.UTF_8);
        // END SNIPPET: importShapefile end::importShapefile[]

        checkIndexAndFeatureCount("highway");
    }

    @Test
    public void testExportShapefileFromOSM() throws Exception {
        System.out.println("\n=== Test import map.osm, create DynamicLayer and export shapefile ===");
        importMapOSM(graphDb);
        GraphDatabaseService database = graphDb;
        // START SNIPPET: exportShapefileFromOSM tag::exportShapefileFromOSM[]
        SpatialDatabaseService spatialService = new SpatialDatabaseService(database);
        try (Transaction tx = database.beginTx()) {
            OSMLayer layer = (OSMLayer) spatialService.getLayer(tx, "map.osm");
            DynamicLayerConfig wayLayer = layer.addSimpleDynamicLayer(tx, Constants.GTYPE_LINESTRING);
            ShapefileExporter shpExporter = new ShapefileExporter(database);
            shpExporter.exportLayer(wayLayer.getName());
            tx.commit();
        }
        // END SNIPPET: exportShapefileFromOSM end::exportShapefileFromOSM[]
    }

    @Test
    public void testExportShapefileFromQuery() throws Exception {
        System.out.println("\n=== Test import map.osm, create DynamicLayer and export shapefile ===");
        importMapOSM(graphDb);
        GraphDatabaseService database = graphDb;
        // START SNIPPET: exportShapefileFromQuery tag::exportShapefileFromQuery[]
        SpatialDatabaseService spatialService = new SpatialDatabaseService(database);
        Envelope bbox = new Envelope(12.94, 12.96, 56.04, 56.06);
        List<SpatialDatabaseRecord> results;
        try (Transaction tx = database.beginTx()) {
            Layer layer = spatialService.getLayer(tx, "map.osm");
            LayerIndexReader spatialIndex = layer.getIndex();
            System.out.println("Have " + spatialIndex.count(tx) + " geometries in " + spatialIndex.getBoundingBox(tx));

            results = GeoPipeline
                    .startIntersectWindowSearch(tx, layer, bbox)
                    .toSpatialDatabaseRecordList();

            spatialService.createResultsLayer(tx, "results", results);
            ShapefileExporter shpExporter = new ShapefileExporter(database);
            shpExporter.exportLayer("results");
            tx.commit();

        }
        // END SNIPPET: exportShapefileFromQuery end::exportShapefileFromQuery[]
        doGeometryTestsOnResults(bbox, results);
    }

    private void doGeometryTestsOnResults(Envelope bbox, List<SpatialDatabaseRecord> results) {
        System.out.println("Found " + results.size() + " geometries in " + bbox);
        Geometry geometry = results.get(0).getGeometry();
        System.out.println("First geometry is " + geometry);
        geometry.buffer(2);
    }

}