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

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.geotools.data.DataStore;
import org.geotools.data.neo4j.Neo4jSpatialDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.neo4j.gis.spatial.osm.*;
import org.neo4j.gis.spatial.osm.OSMDataset.Way;
import org.neo4j.gis.spatial.pipes.osm.OSMGeoPipeline;
import org.neo4j.graphdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class TestOSMImport extends Neo4jTestCase {
    public static final String spatialTestMode = System.getProperty("spatial.test.mode");
    public static final String spatialTestModeX = System.getProperty("spatial.test.mode.extra");

    public TestOSMImport(String layerName, boolean includePoints, boolean useBatchInserter) {
        setName("OSM-Import[points:" + includePoints + ", batch:" + useBatchInserter + "]: " + layerName);
    }

    public static Test suite() {
        deleteBaseDir();
        TestSuite suite = new TestSuite();
        String[] smallModels = new String[]{"one-street.osm", "two-street.osm"};
//		String[] mediumModels = new String[] { "map.osm", "map2.osm" };
        String[] mediumModels = new String[]{"map.osm"};
        String[] largeModels = new String[]{"cyprus.osm", "croatia.osm", "denmark.osm"};

        // Setup default test cases (short or medium only, no long cases)
        ArrayList<String> layersToTest = new ArrayList<String>();
//		layersToTest.addAll(Arrays.asList(smallModels));
        layersToTest.addAll(Arrays.asList(mediumModels));

        // Now modify the test cases based on the spatial.test.mode setting
        if (spatialTestMode != null && spatialTestMode.equals("long")) {
            // Very long running tests
            layersToTest.addAll(Arrays.asList(largeModels));
        } else if (spatialTestMode != null && spatialTestMode.equals("short")) {
            // Tests used for a quick check
            layersToTest.clear();
            layersToTest.addAll(Arrays.asList(smallModels));
        } else if (spatialTestMode != null && spatialTestMode.equals("dev")) {
            // Tests relevant to current development
            layersToTest.clear();
//			layersToTest.add("/home/craig/Desktop/AWE/Data/MapData/baden-wurttemberg.osm/baden-wurttemberg.osm");
            layersToTest.addAll(Arrays.asList(largeModels));
        }
        boolean[] pointsTestModes = new boolean[]{true, false};
        boolean[] batchTestModes = new boolean[]{true, false};
        if (spatialTestModeX != null) {
            if (spatialTestModeX.equals("suppressBatch")) {
                batchTestModes = new boolean[]{false};
            } else if (spatialTestModeX.equals("suppressGraph")) {
                batchTestModes = new boolean[]{true};
            }
        }

        // Finally build the set of complete test cases based on the collection
        // above
        for (final String layerName : layersToTest) {
            for (final boolean includePoints : pointsTestModes) {
                for (final boolean useBatchInserter : batchTestModes) {
                    suite.addTest(new TestOSMImport(layerName, includePoints, useBatchInserter) {
                        public void runTest() {
                            try {
                                runImport(layerName, includePoints, useBatchInserter);
                            } catch (Exception e) {
                                e.printStackTrace();
                                throw new SpatialDatabaseException(e.getMessage(), e);
                            }
                        }
                    });
                }
            }
        }
        System.out.println("This suite has " + suite.testCount() + " tests");
        for (int i = 0; i < suite.testCount(); i++) {
            System.out.println("\t" + suite.testAt(i).toString());
        }
        return suite;
    }

    protected static String checkOSMFile(String osm) {
        File osmFile = new File(osm);
        if (!osmFile.exists()) {
            osmFile = new File(new File("osm"), osm);
            if (!osmFile.exists()) {
                return null;
            }
        }
        return osmFile.getPath();
    }

    protected static void checkOSMLayer(GraphDatabaseService graphDatabaseService, String layerName) throws IOException {
        try (Transaction tx = graphDatabaseService.beginTx()) {
            SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDatabaseService);
            OSMLayer layer = (OSMLayer) spatialService.getOrCreateLayer(layerName, OSMGeometryEncoder.class, OSMLayer.class);
            assertNotNull("OSM Layer index should not be null", layer.getIndex());
            assertNotNull("OSM Layer index envelope should not be null", layer.getIndex().getBoundingBox());
            Envelope bbox = Utilities.fromNeo4jToJts(layer.getIndex().getBoundingBox());
            debugEnvelope(bbox, layerName, Constants.PROP_BBOX);
            // ((RTreeIndex)layer.getIndex()).debugIndexTree();
            checkIndexAndFeatureCount(layer);
            checkChangesetsAndUsers(layer);
            checkOSMSearch(layer);
            tx.success();
        }
    }

    public static void checkOSMSearch(OSMLayer layer) throws IOException {
        OSMDataset osm = (OSMDataset) layer.getDataset();
        Way way = null;
        int count = 0;
        for (Way wayNode : osm.getWays()) {
            way = wayNode;
            if (count++ > 100)
                break;
        }
        assertNotNull("Should be at least one way", way);
        Envelope bbox = way.getEnvelope();
        runSearches(layer, bbox, true);
        org.neo4j.gis.spatial.rtree.Envelope layerBBox = layer.getIndex().getBoundingBox();
        double[] centre = layerBBox.centre();
        double width = layerBBox.getWidth() / 100.0;
        double height = layerBBox.getHeight() / 100.0;
        bbox = new Envelope(centre[0] - width, centre[0] + width, centre[1] - height, centre[1] + height);
        runSearches(layer, bbox, false);
    }

    private static void runSearches(OSMLayer layer, Envelope bbox, boolean willHaveResult) {
        for (int i = 0; i < 4; i++) {
            Geometry searchArea = layer.getGeometryFactory().toGeometry(bbox);
            runWithinSearch(layer, searchArea, willHaveResult);
            bbox.expandBy(bbox.getWidth(), bbox.getHeight());
        }
    }

    private static void runWithinSearch(OSMLayer layer, Geometry searchArea, boolean willHaveResult) {
        long start = System.currentTimeMillis();
        List<SpatialDatabaseRecord> results = OSMGeoPipeline
                .startWithinSearch(layer, searchArea).toSpatialDatabaseRecordList();
        long time = System.currentTimeMillis() - start;
        System.out.println("Took " + time + "ms to find " + results.size() + " search results in layer " + layer.getName()
                + " using search within " + searchArea);
        if (willHaveResult)
            assertTrue("Should be at least one result, but got zero", results.size() > 0);
    }

    public static void debugEnvelope(Envelope bbox, String layer, String name) {
        System.out.println("Layer '" + layer + "' has envelope '" + name + "': " + bbox);
        System.out.println("\tX: [" + bbox.getMinX() + ":" + bbox.getMaxX() + "]");
        System.out.println("\tY: [" + bbox.getMinY() + ":" + bbox.getMaxY() + "]");
    }

    public static void checkIndexAndFeatureCount(Layer layer) throws IOException {
        if (layer.getIndex().count() < 1) {
            System.out.println("Warning: index count zero: " + layer.getName());
        }
		System.out.println("Layer '" + layer.getName() + "' has " + layer.getIndex().count() + " entries in the index");
		GraphDatabaseService graphDb = layer.getSpatialDatabase().getDatabase();
		DataStore store = new Neo4jSpatialDataStore(graphDb);
		try (Transaction tx = graphDb.beginTx()) {
			SimpleFeatureCollection features = store.getFeatureSource(layer.getName()).getFeatures();
			int featuresSize = features.size();
			int indexCount = layer.getIndex().count();
			System.out.println("Layer '" + layer.getName() + "' has " + featuresSize + " features");
			assertEquals("FeatureCollection.size for layer '" + layer.getName() + "' not the same as index count",
					indexCount, featuresSize);
			tx.success();
		}
    }

    public static void checkChangesetsAndUsers(OSMLayer layer) {
        double totalMatch = 0.0;
        int waysMatched = 0;
        int waysCounted = 0;
        int nodesCounted = 0;
        int waysMissing = 0;
        int nodesMissing = 0;
        int usersMissing = 0;
        float maxMatch = 0.0f;
        float minMatch = 1.0f;
        HashMap<Long, Integer> userNodeCount = new HashMap<Long, Integer>();
        HashMap<Long, String> userNames = new HashMap<Long, String>();
        HashMap<Long, Long> userIds = new HashMap<Long, Long>();
        OSMDataset dataset = (OSMDataset) layer.getDataset();
        for (Node way : dataset.getAllWayNodes()) {
            int node_count = 0;
            int match_count = 0;
            assertNull("Way has changeset property", way.getProperty("changeset", null));
            Node wayChangeset = dataset.getChangeset(way);
            if (wayChangeset != null) {
                long wayCS = (Long) wayChangeset.getProperty("changeset");
                for (Node node : dataset.getWayNodes(way)) {
                    assertNull("Node has changeset property", node.getProperty("changeset", null));
                    Node nodeChangeset = dataset.getChangeset(node);
                    if (nodeChangeset == null) {
                        nodesMissing++;
                    } else {
                        long nodeCS = (Long) nodeChangeset.getProperty("changeset");
                        if (nodeChangeset.equals(wayChangeset)) {
                            match_count++;
                        } else {
                            assertFalse("Two changeset nodes should not have the same changeset number: way(" + wayCS + ")==node("
                                    + nodeCS + ")", wayCS == nodeCS);
                        }
                        Node user = dataset.getUser(nodeChangeset);
                        if (user != null) {
                            long userid = user.getId();
                            if (userNodeCount.containsKey(userid)) {
                                userNodeCount.put(userid, userNodeCount.get(userid) + 1);
                            } else {
                                userNodeCount.put(userid, 1);
                                userNames.put(userid, (String) user.getProperty("name", null));
                                userIds.put(userid, (Long) user.getProperty("uid", null));
                            }
                        } else {
                            if (usersMissing++ < 10) {
                                System.out.println("Changeset " + nodeCS + " should have user: " + nodeChangeset);
                            }
                        }
                    }
                    node_count++;
                }
            } else {
                waysMissing++;
            }
            if (node_count > 0) {
                waysMatched++;
                float match = ((float) match_count) / ((float) node_count);
                maxMatch = Math.max(maxMatch, match);
                minMatch = Math.min(minMatch, match);
                totalMatch += match;
                nodesCounted += node_count;
            }
            waysCounted++;
        }
        System.out.println("After checking " + waysCounted + " ways:");
        System.out.println("\twe found " + waysMatched + " ways with an average of " + (nodesCounted / waysMatched) + " nodes");
        System.out.println("\tand an average of " + (100.0 * totalMatch / waysMatched) + "% matching changesets");
        System.out.println("\twith min-match " + (100.0 * minMatch) + "% and max-match " + (100.0 * maxMatch) + "%");
        System.out.println("\tWays missing changsets: " + waysMissing);
        System.out.println("\tNodes missing changsets: " + nodesMissing + " (~" + (nodesMissing / waysMatched) + " / way)");
        System.out.println("\tUnique users: " + userNodeCount.size() + " (with " + usersMissing + " changeset missing users)");
        ArrayList<ArrayList<Long>> userCounts = new ArrayList<ArrayList<Long>>();
        for (long user : userNodeCount.keySet()) {
            int count = userNodeCount.get(user);
            userCounts.ensureCapacity(count);
            while (userCounts.size() < count + 1) {
                userCounts.add(null);
            }
            ArrayList<Long> userSet = userCounts.get(count);
            if (userSet == null) {
                userSet = new ArrayList<Long>();
            }
            userSet.add(user);
            userCounts.set(count, userSet);
        }
        if (userCounts.size() > 1) {
            System.out.println("\tTop 20 users (nodes: users):");
            for (int ui = userCounts.size() - 1, i = 0; i < 20 && ui >= 0; ui--) {
                ArrayList<Long> userSet = userCounts.get(ui);
                if (userSet != null && userSet.size() > 0) {
                    i++;
                    StringBuffer us = new StringBuffer();
                    for (long user : userSet) {
                        Node userNode = layer.getSpatialDatabase().getDatabase().getNodeById(user);
                        int csCount = 0;
                        for (@SuppressWarnings("unused")
                        Relationship rel : userNode.getRelationships(OSMRelation.USER, Direction.INCOMING)) {
                            csCount++;
                        }
                        String name = userNames.get(user);
                        Long uid = userIds.get(user);
                        if (us.length() > 0) {
                            us.append(", ");
                        }
                        us.append("" + name + " (uid=" + uid + ", id=" + user + ", changesets=" + csCount + ")");
                    }
                    System.out.println("\t\t" + ui + ": " + us.toString());
                }
            }
        }
    }

    protected void runImport(String osm, boolean includePoints, boolean useBatchInserter) throws Exception {
        // TODO: Consider merits of using dependency data in target/osm,
        // downloaded by maven, as done in TestSpatial, versus the test data
        // commited to source code as done here
        String osmPath = checkOSMFile(osm);
        if (osmPath == null) {
            return;
        }
        printDatabaseStats();
        loadTestOsmData(osm, osmPath, includePoints, useBatchInserter, 1000);
        checkOSMLayer(graphDb(), osm);
        printDatabaseStats();
    }

    protected void loadTestOsmData(String layerName, String osmPath, boolean includePoints, boolean useBatchInserter,
                                   int commitInterval) throws Exception {
        System.out.println("\n=== Loading layer " + layerName + " from " + osmPath + ", includePoints=" + includePoints
                + ", useBatchInserter=" + useBatchInserter + " ===");
        if (useBatchInserter) {
            reActivateDatabase(false, true, false);
        }
        long start = System.currentTimeMillis();
        // START SNIPPET: importOsm
        OSMImporter importer = new OSMImporter(layerName, new ConsoleListener());
        importer.setCharset(Charset.forName("UTF-8"));
        if (useBatchInserter) {
            importer.importFile(getBatchInserter(), osmPath, includePoints);
            reActivateDatabase(false, false, false);
        } else {
            importer.importFile(graphDb(), osmPath, includePoints, 5000, true);
        }
        // END SNIPPET: importOsm
        // Weird hack to force GC on large loads
        if (System.currentTimeMillis() - start > 300000) {
            for (int i = 0; i < 3; i++) {
                System.gc();
                Thread.sleep(1000);
            }
        }
        importer.reIndex(graphDb(), commitInterval, includePoints, false);
    }
}
