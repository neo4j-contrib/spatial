package org.neo4j.gis.spatial;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.geotools.data.shapefile.shp.ShapefileException;
import org.neo4j.gis.spatial.query.SearchIntersect;

import com.vividsolutions.jts.geom.Envelope;

/**
 * Test cases for initial version of Neo4j-Spatial. This was converted directly from Davide
 * Savazzi's console applications: ShapefileImporter, Test, Test2 and Test3.
 * 
 * @author Davide Savazzi
 * @author Craig Taverner
 */
public class TestSpatial extends Neo4jTestCase {
    private final String SHP_DIR = "target/shp";
    private final String OSM_DIR = "target/osm";
    private enum DataFormat {
        SHP("ESRI Shapefile"),
        OSM("OpenStreetMap");
        private String description;
        DataFormat(String description) {
            this.description = description;
        }
        public String toString(){
            return description;
        }
    }

    /**
     * This class represents mock objects for representing geometries in simple form in memory for
     * testing against real geometries. We have a few hard-coded test geometries we expect to find
     * stored in predictable ways in the test database. Currently we only test for bounding box so
     * this class only contains that information.
     * 
     * @author craig
     * @since 1.0.0
     */
    private static class TestGeometry {
        private String id;
        private String name;
        Envelope bounds;

        public TestGeometry(String id, String name, String bounds) {
            this.id = id;
            this.name = name;
            float bf[] = new float[4];
            int bi = 0;
            for (String bound : bounds.replaceAll("[\\(\\)\\s]+", "").split(",")) {
                bf[bi++] = Float.parseFloat(bound);
            }
            this.bounds = new Envelope(bf[0], bf[2], bf[1], bf[3]);
        }

        public String toString() {
            return name.length() > 0 ? name : id;
        }

        public boolean inOrIntersects(Envelope env) {
            return env.intersects(bounds);
        }
    }

    private static final ArrayList<String> layers = new ArrayList<String>();
    private static final HashMap<String, ArrayList<TestGeometry>> layerTestGeometries = new HashMap<String, ArrayList<TestGeometry>>();
    private static final HashMap<String, DataFormat> layerTestFormats = new HashMap<String, DataFormat>();
    static {
        //TODO: Rather load this from a configuration file, properties file or JRuby test code
        addTestLayer("sweden.osm", DataFormat.OSM);
        addTestLayer("sweden.osm.administrative", DataFormat.OSM);
        addTestGeometry("sweden_administrative.103", "Dalby söderskog", "(13.32406,55.671652), (13.336948,55.679243)");
        addTestGeometry("sweden_administrative.83", "Söderåsen", "(13.167721,56.002416), (13.289724,56.047099)");

        //TODO: Rather load this from a configuration file, properties file or JRuby test code
        addTestLayer("sweden_administrative", DataFormat.SHP);
        addTestGeometry("sweden_administrative.103", "Dalby söderskog", "(13.32406,55.671652), (13.336948,55.679243)");
        addTestGeometry("sweden_administrative.83", "Söderåsen", "(13.167721,56.002416), (13.289724,56.047099)");

        addTestLayer("sweden_natural", DataFormat.SHP);
        addTestGeometry("sweden_natural.208", "Bokskogen", "(13.1935576,55.5324763), (13.2710125,55.5657891)");
        addTestGeometry("sweden_natural.6110", "Pålsjö skog", "(12.6744031,56.0636946), (12.6934147,56.0771857)");

        addTestLayer("sweden_water", DataFormat.SHP);
        addTestGeometry("sweden_water.9548", "Yddingesjön", "(13.23564,55.5360264), (13.2676649,55.5558856)");
        addTestGeometry("sweden_water.10494", "Finjasjön", "(13.6718979,56.1157516), (13.7398759,56.1566911)");

        addTestLayer("sweden_highway", DataFormat.SHP);
        addTestGeometry("sweden_highway.58904", "Holmeja byväg", "(13.2819022,55.5561414), (13.2820848,55.5575418)");
        addTestGeometry("sweden_highway.45305", "Yttre RIngvägen", "(12.9827334,55.5473645), (13.0118313,55.5480455)");
        addTestGeometry("sweden_highway.43536", "Yttre RIngvägen", "(12.9412071,55.5564264), (12.9422181,55.5571701)");
    }

    private static void addTestLayer(String layer, DataFormat format) {
        layers.add(layer);
        layerTestFormats.put(layer,format);
    }

    private static void addTestGeometry(String id, String name, String bounds) {
        String layer = layers.get(layers.size() - 1);
        if (layerTestGeometries.get(layer) == null) {
            layerTestGeometries.put(layer, new ArrayList<TestGeometry>());
        }
        ArrayList<TestGeometry> geoms = layerTestGeometries.get(layer);
        geoms.add(new TestGeometry(id, name, bounds));
    }

    protected void setUp() throws Exception {
        super.setUp(true,false,false); // pass true to delete previous database, speeding up the index test
        long start = System.currentTimeMillis();
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        // for (String layerName : new String[] {"sweden.osm"}) {
        for (String layerName : new String[] {"sweden.osm.administrative"}) {
        // for (String layerName : new String[] {"sweden_highway"}) {
        // for (String layerName : new String[] {"sweden_administrative", "sweden_natural"}) {
        // for (String layerName : new String[] {"sweden_administrative", "sweden_natural", "sweden_water"}) {
        // for (String layerName : layers) {
            Layer layer = spatialService.getLayer(layerName);
            if (layer == null || layer.getIndex() == null || layer.getIndex().count() < 1) {
                switch(TestSpatial.layerTestFormats.get(layerName)) {
                case SHP: loadTestShpData(layerName, 1000); break;
                case OSM: loadTestOsmData(layerName, 1000); break;
                default: System.err.println("Unknown format: "+layerTestFormats.get(layerName));
                }
            }
        }
        System.out.println("Total time for load: " + 1.0 * (System.currentTimeMillis() - start) / 1000.0 + "s");
    }

    private void loadTestShpData(String layerName, int commitInterval) throws ShapefileException, FileNotFoundException, IOException {
        String shpPath = SHP_DIR + File.separator + layerName;
        System.out.println("\n=== Loading layer " + layerName + " from " + shpPath + " ===");
        ShapefileImporter importer = new ShapefileImporter(graphDb(), new NullListener(commitInterval));
        importer.importFile(shpPath, layerName);
    }

    private void loadTestOsmData(String layerName, int commitInterval) throws Exception {
        String osmPath = OSM_DIR + File.separator + layerName;
        System.out.println("\n=== Loading layer " + layerName + " from " + osmPath + " ===");
        reActivateDatabase(false, true, false);
        OSMImporter importer = new OSMImporter(layerName);
        importer.importFile(getBatchInserter(), osmPath);
        reActivateDatabase(false, false, false);
        importer.reIndex(graphDb(), commitInterval);
    }

    public void testSpatialIndex() {
        long start = System.currentTimeMillis();
        for (String layer : layers) {
            testSpatialIndex(layer);
        }
        System.out.println("Total time for index test: " + 1.0 * (System.currentTimeMillis() - start) / 1000.0 + "s");
    }

    private void testSpatialIndex(String layerName) {
        System.out.println("\n=== Testing layer: " + layerName + " ===");
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        Layer layer = spatialService.getLayer(layerName);
        if (layer == null || layer.getIndex() == null || layer.getIndex().count() < 1) {
            System.out.println("Layer not loaded: " + layerName);
            return;
        }

        ((RTreeIndex)layer.getIndex()).warmUp();

        SpatialIndexReader fakeIndex = new SpatialIndexPerformanceProxy(new FakeIndex(graphDb(), layer));
        SpatialIndexReader rtreeIndex = new SpatialIndexPerformanceProxy(layer.getIndex());

        System.out.println("FakeIndex bounds:  " + fakeIndex.getLayerBoundingBox());
        System.out.println("RTreeIndex bounds: " + rtreeIndex.getLayerBoundingBox());

        System.out.println("FakeIndex count:  " + fakeIndex.count());
        System.out.println("RTreeIndex count: " + rtreeIndex.count());

        // Bounds for sweden_administrative: 11.1194502 : 24.1585511, 55.3550515 : 69.0600767
        // Envelope bbox = new Envelope(12.85, 13.25, 55.5, 55.65); // cover Malmö
        Envelope bbox = new Envelope(13.0, 14.00, 55.0, 56.0); // cover central Skåne
        // Envelope bbox = new Envelope(13, 14, 55, 58); // cover admin area 'Söderåsen'
        // Envelope bbox = new Envelope(7, 10, 37, 40);

        System.out.println("Displaying test geometries for layer '" + layerName + "'");
        for (TestGeometry testData : layerTestGeometries.get(layerName)) {
            System.out.println("\tGeometry: " + testData.toString() + " " + (testData.inOrIntersects(bbox) ? "is" : "is NOT")
                    + " inside search region");
        }

        Search searchQuery = new SearchIntersect(layer.getGeometryFactory().toGeometry(bbox));
        for (SpatialIndexReader index : new SpatialIndexReader[] {fakeIndex, rtreeIndex}) {
            ArrayList<TestGeometry> foundData = new ArrayList<TestGeometry>();
            index.executeSearch(searchQuery);
            List<SpatialDatabaseRecord> results = searchQuery.getResults();
            System.out.println("\tIndex[" + index.getClass() + "] found results: " + results.size());
            int ri = 0;
            for (SpatialDatabaseRecord r : results) {
                if (r.getGeomNode().hasProperty("NAME")) {
                    String name = (String)r.getProperty("NAME");
                    if (name != null && name.length() > 0) {
                        System.out.println("\tRTreeIndex result[" + ri + "]: " + r.getId() + ":" + r.getType() + " - "
                                + r.toString());
                        if (ri++ > 10)
                            break;
                    }
                    for (TestGeometry testData : layerTestGeometries.get(layerName)) {
                        if (testData.name.equals(name)) {
                            System.out.println("\tFound match in test data: test[" + testData + "] == result[" + r + "]");
                            foundData.add(testData);
                        }
                    }
                } else {
                    System.err.println("\tNo name in RTreeIndex result: " + r.getId() + ":" + r.getType() + " - " + r.toString());
                }
            }
            System.out.println("Found " + foundData.size() + " test datasets in region[" + bbox + "]");
            for (TestGeometry testData : foundData) {
                System.out.println("\t" + testData + ": " + testData.bounds);
            }
            System.out.println("Verifying results for " + layerTestGeometries.size() + " test datasets in region[" + bbox + "]");
            for (TestGeometry testData : layerTestGeometries.get(layerName)) {
                System.out.println("\t" + testData + ": " + testData.bounds);
                String error = "Incorrect test result: test[" + testData + "] not found by search inside region[" + bbox + "]";
                if (testData.inOrIntersects(bbox) && !foundData.contains(testData)) {
                    System.out.println(error);
                    assertTrue(error, false);
                }
            }
        }

    }

}