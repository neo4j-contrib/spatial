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
package org.neo4j.gis.spatial;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.geotools.data.shapefile.shp.ShapefileException;
import org.neo4j.collections.rtree.Envelope;
import org.neo4j.collections.rtree.NullListener;
import org.neo4j.gis.spatial.filter.SearchIntersect;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.graphdb.Node;


/**
 * <p>
 * Test cases for initial version of Neo4j-Spatial. This was originally
 * converted directly from Davide Savazzi's console applications:
 * ShapefileImporter, Test, Test2 and Test3. It was then extended substantially
 * by Craig to include tests for successful search of a number of specific
 * geometries in a number of specific shapefiles. After this tests for OSM
 * dataset import and search were added. The latest OSM work, specific to
 * DynamicLayer's is tested in the new test class TestDynamicLayers.
 * </p>
 * <p>
 * This class can also be configured on the command-line, java runtime
 * configuration or maven configuration. Set the java system property
 * spatial.test.mode to one of the following options:
 * <dl>
 * <dt>long</dt>
 * <dd>All known tests are run, regardless of the size of data or length of time
 * taken to test. This is good for infrequent tests with laeger data. It is
 * possible the maven dependencies will not include the necessary data to run
 * this test, so if you get failures, check that it is not simply missing data.
 * <dt>short</dt>
 * <dd>A very short set of tests are run, just as a quite check. This might be
 * one small SHP and one small OSM file, or even less</dd>
 * <dt>dev</dt>
 * <dd>Tests here change all the time as the developers are coding new features.
 * Craig in particular uses this place for a kind of TDD approach, writing tests
 * as he codes to drive the development. No guarranteee these tests will pass,
 * especially if something is only half coded</dd>
 * <dt>any other value, or leave unset</dt>
 * <dd>The default set of tests to run. This should be something that will work
 * well in the hudson build, as well as for developers downloading and trying
 * out neo4j-spatial</dd>
 * </dl>
 * </p>
 *
 * @author Davide Savazzi
 * @author Craig Taverner
 */
public class TestSpatial extends Neo4jTestCase {
	
    private final String SHP_DIR = "target/shp";
    private final String OSM_DIR = "target/osm";
    
    private final ArrayList<String> layers = new ArrayList<String>();
    private final HashMap<String, Envelope> layerTestEnvelope = new HashMap<String, Envelope>();
    private final HashMap<String, ArrayList<TestGeometry>> layerTestGeometries = new HashMap<String, ArrayList<TestGeometry>>();
    private final HashMap<String, DataFormat> layerTestFormats = new HashMap<String, DataFormat>();
    private final HashMap<Integer,Integer> geomStats = new HashMap<Integer,Integer>();    
    private String spatialTestMode = System.getProperty("spatial.test.mode");

    public void setUp() throws Exception {
    	super.setUp();
    	
        // TODO: Rather load this from a configuration file, properties file or JRuby test code

        Envelope bbox = new Envelope(12.9, 12.99, 56.05, 56.07); // covers half of Billesholm

        addTestLayer("billesholm.osm", DataFormat.OSM, bbox);
        addTestGeometry(70423036, "Ljungsgårdsvägen", "outside top left", "(12.9599540,56.0570692), (12.9624780,56.0716282)");
        addTestGeometry(67835020, "Villagatan", "in the middle", "(12.9776065,56.0561477), (12.9814421,56.0572131)");
        addTestGeometry(60966388, "Storgatan", "crossing left edge", "(12.9682980,56.0524546), (12.9710302,56.0538436)");
        
        bbox = new Envelope(12.5, 14.1, 55.0, 56.3); // cover central Skåne
        // Bounds for sweden_administrative: 11.1194502 : 24.1585511, 55.3550515 : 69.0600767
        // Envelope bbox = new Envelope(12.85, 13.25, 55.5, 55.65); // cover Malmö
        // Envelope bbox = new Envelope(13, 14, 55, 58); // cover admin area 'Söderåsen'
        // Envelope bbox = new Envelope(7, 10, 37, 40);

        addTestLayer("sweden.osm", DataFormat.OSM, bbox);
        addTestLayer("sweden.osm.administrative", DataFormat.OSM, bbox);
        
        addTestLayer("sweden_administrative", DataFormat.SHP, bbox);
        addTestGeometry(1055, "Söderåsens nationalpark", "near top edge", "(13.167721,56.002416), (13.289724,56.047099)");
        addTestGeometry(1067, "", "inside", "(13.2122907,55.6969478), (13.5614499,55.7835819)");
        addTestGeometry(943, "", "crosses left edge", "(12.9120438,55.8253138), (13.0501381,55.8484289)");
        addTestGeometry(884, "", "outside left", "(12.7492433,55.9269403), (12.9503304,55.964951)");
        addTestGeometry(1362, "", "crosses top right", "(13.7453871,55.9483067), (14.0084487,56.1538786)");
        addTestGeometry(1521, "", "outside right", "(14.0762394,55.4889569), (14.1869043,55.7592587)");
        addTestGeometry(1184, "", "outside above", "(13.4215555,56.109138), (13.4683671,56.2681347)");

        addTestLayer("sweden_natural", DataFormat.SHP, bbox);
        addTestGeometry(208, "Bokskogen", "", "(13.1935576,55.5324763), (13.2710125,55.5657891)");
        addTestGeometry(3462, "Pålsjö skog", "", "(12.6746748,56.0634246), (12.6934147,56.0776016)");
        addTestGeometry(647, "Dalby söderskog", "", "(13.32406,55.671652), (13.336948,55.679243)");

        addTestLayer("sweden_water", DataFormat.SHP, bbox);
        addTestGeometry(13149, "Yddingesjön", "", "(13.23564,55.5360264), (13.2676649,55.5558856)");
        addTestGeometry(14431, "Finjasjön", "", "(13.6718979,56.1157516), (13.7398759,56.1566911)");

        // TODO missing file
        addTestLayer("sweden_highway", DataFormat.SHP, bbox);
        addTestGeometry(58904, "Holmeja byväg", "", "(13.2819022,55.5561414), (13.2820848,55.5575418)");
        addTestGeometry(45305, "Yttre RIngvägen", "", "(12.9827334,55.5473645), (13.0118313,55.5480455)");
        addTestGeometry(43536, "Yttre RIngvägen", "", "(12.9412071,55.5564264), (12.9422181,55.5571701)");
    }

    private void addTestLayer(String layer, DataFormat format, Envelope bbox) {
        layers.add(layer);
        layerTestEnvelope.put(layer, bbox);
        layerTestFormats.put(layer, format);
        layerTestGeometries.put(layer, new ArrayList<TestGeometry>());
    }

    private void addTestGeometry(Integer id, String name, String comments, String bounds) {
        String layer = layers.get(layers.size() - 1);
        ArrayList<TestGeometry> geoms = layerTestGeometries.get(layer);
        geoms.add(new TestGeometry(id, name, comments, bounds));
    }

    public void testShpSwedenAdministrative() throws Exception {
    	testLayer("sweden_administrative");
    }
    
    public void testShpSwedenNatural() throws Exception {
       	testLayer("sweden_natural");
    }
    
    public void testShpSwedenWater() throws Exception {
       	testLayer("sweden_water");
    }

    public void testOsmBillesholm() throws Exception {
    	testLayer("billesholm.osm");
    }    

    public void testOsmSwedenAdministrative() throws Exception {
    	if ("long".equals(spatialTestMode))
    		testLayer("sweden.osm.administrative");
    }    

    public void testOsmSweden() throws Exception {
    	if ("long".equals(spatialTestMode))
    		testLayer("sweden.osm");
    }     
    
	// TODO missing file
    /* 
    public void testShpSwedenHighway() throws Exception {
    	if ("long".equals(spatialTestMode))
    		testLayer("sweden_highway");
    } */   

    private void testLayer(String layerName) throws Exception {
        testImport(layerName);
        testSpatialIndex(layerName);    	
    }
    
    private void testImport(String layerName) throws Exception {
        long start = System.currentTimeMillis();
        System.out.println("\n===========\n=========== Import Test: " + layerName + "\n===========");
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        Layer layer = spatialService.getLayer(layerName);
        if (layer == null || layer.getIndex() == null || layer.getIndex().count() < 1) {
            switch (layerTestFormats.get(layerName)) {
            case SHP:
                loadTestShpData(layerName, 1000);
                break;
            case OSM:
                loadTestOsmData(layerName, 1000);
                break;
            default:
                fail("Unknown format: " + layerTestFormats.get(layerName));
            }
        } else {
        	fail("Layer already present: " + layerName);
        }
        
        System.out.println("Total time for load: " + 1.0 * (System.currentTimeMillis() - start) / 1000.0 + "s");
    }

    private void loadTestShpData(String layerName, int commitInterval) throws ShapefileException, FileNotFoundException, IOException {
        String shpPath = SHP_DIR + File.separator + layerName;
        System.out.println("\n=== Loading layer " + layerName + " from " + shpPath + " ===");
        ShapefileImporter importer = new ShapefileImporter(graphDb(), new NullListener(), commitInterval);
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

    private void testSpatialIndex(String layerName) {
        System.out.println("\n=== Spatial Index Test: " + layerName + " ===");
        long start = System.currentTimeMillis();

        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        Layer layer = spatialService.getLayer(layerName);
        if (layer == null || layer.getIndex() == null || layer.getIndex().count() < 1) {
            fail("Layer not loaded: " + layerName);
        }

        LayerIndexReader fakeIndex = new SpatialIndexPerformanceProxy(new FakeIndex(layer));
        LayerIndexReader rtreeIndex = new SpatialIndexPerformanceProxy(layer.getIndex());

        System.out.println("RTreeIndex bounds: " + rtreeIndex.getBoundingBox());
        System.out.println("FakeIndex bounds: " + fakeIndex.getBoundingBox());        
        assertEnvelopeEquals(fakeIndex.getBoundingBox(), rtreeIndex.getBoundingBox());
        
        System.out.println("RTreeIndex count: " + rtreeIndex.count());
        assertEquals(fakeIndex.count(), rtreeIndex.count());
        
        Envelope bbox = layerTestEnvelope.get(layerName);

        System.out.println("Displaying test geometries for layer '" + layerName + "' including expected search results");
        for (TestGeometry testData : layerTestGeometries.get(layerName)) {
            System.out.println("\tGeometry: " + testData + " " + (testData.inOrIntersects(bbox) ? "is" : "is NOT")
                    + " inside search region");
        }

        for (LayerIndexReader index : new LayerIndexReader[] { rtreeIndex, fakeIndex }) {
            ArrayList<TestGeometry> foundData = new ArrayList<TestGeometry>();

            SearchIntersect searchQuery = new SearchIntersect(layer, layer.getGeometryFactory().toGeometry(Utilities.fromNeo4jToJts(bbox)));
            SearchRecords results = index.search(searchQuery);
         
            int count = 0;
            int ri = 0;
            for (SpatialDatabaseRecord r : results) {
            	count++;
                if (ri++ < 10) {
                    StringBuffer props = new StringBuffer();
                    for (String prop : r.getPropertyNames()) {
                        if (props.length() > 0) props.append(", ");
                        props.append(prop + ": " + r.getProperty(prop));
                    }
                    
                    System.out.println("\tRTreeIndex result[" + ri + "]: " + r.getId() + ":" + r.getType() + " - " + r.toString() + ": PROPS["+props+"]");
                } else if (ri == 10) {
                    // System.out.println("\t.. and " + (count - ri) + " more ..");
                }
                
                addGeomStats(r.getGeomNode());
                
                String name = (String) r.getProperty("NAME");
                if (name == null) name = (String) r.getProperty("name");
                
                Integer id = (Integer) r.getProperty("ID");
                if ((name != null && name.length() > 0) || id != null) {
                    for (TestGeometry testData : layerTestGeometries.get(layerName)) {
                        if ((name != null && name.length() > 0 && testData.name.equals(name)) || (id != null && testData.id.equals(id))) {
                            System.out.println("\tFound match in test data: test[" + testData + "] == result[" + r + "]");
                            foundData.add(testData);
                        } /* else if(name != null && name.length()>0 && name.startsWith(testData.name.substring(0,1))) {
                            System.out.println("\tOnly first character matched: test[" + testData + "] == result[" + r + "]");
                        } */
                    }
                } else {
                    System.err.println("\tNo name or id in RTreeIndex result: " + r.getId() + ":" + r.getType() + " - "
                            + r.toString());
                }
            }
            
            dumpGeomStats();
            
            System.out.println("Found " + foundData.size() + " test datasets in region[" + bbox + "]");
            for (TestGeometry testData : foundData) {
                System.out.println("\t" + testData + ": " + testData.bounds);
            }
            
            System.out.println("Verifying results for " + layerTestGeometries.size() + " test datasets in region[" + bbox + "]");
            for (TestGeometry testData : layerTestGeometries.get(layerName)) {
                System.out.println("\t" + testData + ": " + testData.bounds);
                if (testData.inOrIntersects(bbox) && !foundData.contains(testData)) {
                    String error = "Incorrect test result: test[" + testData + "] not found by search inside region[" + bbox + "]";                	
                    System.out.println(error);
                    fail(error);
                }
            }
        }

        System.out.println("Total time for index test: " + 1.0 * (System.currentTimeMillis() - start) / 1000.0 + "s");
    }

	private void assertEnvelopeEquals(Envelope a, Envelope b) {
		assertTrue(a.isValid());
		assertTrue(b.isValid());
		assertEquals(a.getDimension(), b.getDimension());
		
		for (int i = 0; i < a.getDimension(); i++) {
			assertEquals(a.getMin(i), b.getMin(i), 0);
			assertEquals(a.getMax(i), b.getMax(i), 0);
		}
	}    
    
    private void addGeomStats(Node geomNode) {
        addGeomStats((Integer)geomNode.getProperty(Constants.PROP_TYPE, null));
    }

    private void addGeomStats(Integer geom) {
        Integer count = geomStats.get(geom);
        geomStats.put(geom, count == null ? 1 : count + 1);
    }

    private void dumpGeomStats() {
        System.out.println("Geometry statistics for " + geomStats.size() + " geometry types:");
        for (Object key : geomStats.keySet()) {
            Integer count = geomStats.get(key);
            System.out.println("\t" + SpatialDatabaseService.convertGeometryTypeToName((Integer)key) + ": " + count);
        }
        geomStats.clear();
    }

    private enum DataFormat {
        SHP("ESRI Shapefile"), OSM("OpenStreetMap");
        
        private String description;

        DataFormat(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
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
        
    	private final Integer id;
        private final String name;
        private final String comments;
        protected Envelope bounds;

        public TestGeometry(Integer id, String name, String comments, String bounds) {
            this.id = id;
            this.name = name == null ? "" : name;
            this.comments = comments;
            
            float bf[] = new float[4];
            int bi = 0;
            for (String bound : bounds.replaceAll("[\\(\\)\\s]+", "").split(",")) {
                bf[bi++] = Float.parseFloat(bound);
            }
            this.bounds = new Envelope(bf[0], bf[2], bf[1], bf[3]);
        }

        @Override
        public String toString() {
            return (name.length() > 0 ? name : "ID[" + id + "]") + (comments == null || comments.length() < 1 ? "" : " (" + comments + ")");
        }

        public boolean inOrIntersects(Envelope env) {
            return env.intersects(bounds);
        }
    }
        
}