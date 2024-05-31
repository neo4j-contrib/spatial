/*
 * Copyright (c) "Neo4j"
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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.geotools.api.data.DataStore;
import org.geotools.data.neo4j.Neo4jSpatialDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.junit.jupiter.api.Assertions;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.osm.OSMDataset;
import org.neo4j.gis.spatial.osm.OSMDataset.Way;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.osm.OSMRelation;
import org.neo4j.gis.spatial.pipes.osm.OSMGeoPipeline;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class TestOSMImportBase extends Neo4jTestCase {

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

	protected static void checkOSMLayer(GraphDatabaseService db, String layerName) throws IOException {
		int indexCount;
		try (Transaction tx = db.beginTx()) {
			SpatialDatabaseService spatial = new SpatialDatabaseService(
					new IndexManager((GraphDatabaseAPI) db, SecurityContext.AUTH_DISABLED));
			OSMLayer layer = (OSMLayer) spatial.getOrCreateLayer(tx, layerName, OSMGeometryEncoder.class,
					OSMLayer.class, null);
			Assertions.assertNotNull(layer.getIndex(), "OSM Layer index should not be null");
			Assertions.assertNotNull(layer.getIndex().getBoundingBox(tx),
					"OSM Layer index envelope should not be null");
			Envelope bbox = Utilities.fromNeo4jToJts(layer.getIndex().getBoundingBox(tx));
			debugEnvelope(bbox, layerName, Constants.PROP_BBOX);
			// ((RTreeIndex)layer.getIndex()).debugIndexTree();
			indexCount = checkIndexCount(tx, layer);
			checkChangesetsAndUsers(tx, layer);
			checkOSMSearch(tx, layer);
			tx.commit();
		}
		checkFeatureCount(db, indexCount, layerName);
	}

	public static void checkOSMSearch(Transaction tx, OSMLayer layer) {
		OSMDataset osm = OSMDataset.fromLayer(tx, layer);
		Way way = null;
		int count = 0;
		for (Way wayNode : osm.getWays(tx)) {
			// Do not `break` from the loop or experience the RelationshipTraversalCursor leak bug in Neo4j 4.3
			if (count++ <= 100) {
				way = wayNode;
			}
		}
		Assertions.assertNotNull(way, "Should be at least one way");
		Envelope bbox = way.getEnvelope();
		runSearches(tx, layer, bbox, true);
		org.neo4j.gis.spatial.rtree.Envelope layerBBox = layer.getIndex().getBoundingBox(tx);
		double[] centre = layerBBox.centre();
		double width = layerBBox.getWidth(0) / 100.0;
		double height = layerBBox.getWidth(1) / 100.0;
		bbox = new Envelope(centre[0] - width, centre[0] + width, centre[1] - height, centre[1] + height);
		runSearches(tx, layer, bbox, false);
	}

	private static void runSearches(Transaction tx, OSMLayer layer, Envelope bbox, boolean willHaveResult) {
		for (int i = 0; i < 4; i++) {
			Geometry searchArea = layer.getGeometryFactory().toGeometry(bbox);
			runWithinSearch(tx, layer, searchArea, willHaveResult);
			bbox.expandBy(bbox.getWidth(), bbox.getHeight());
		}
	}

	private static void runWithinSearch(Transaction tx, OSMLayer layer, Geometry searchArea, boolean willHaveResult) {
		long start = System.currentTimeMillis();
		List<SpatialDatabaseRecord> results = OSMGeoPipeline.startWithinSearch(tx, layer, searchArea)
				.toSpatialDatabaseRecordList();
		long time = System.currentTimeMillis() - start;
		System.out.println(
				"Took " + time + "ms to find " + results.size() + " search results in layer " + layer.getName()
						+ " using search within " + searchArea);
		if (willHaveResult) {
			Assertions.assertTrue(results.size() > 0, "Should be at least one result, but got zero");
		}
	}

	public static void debugEnvelope(Envelope bbox, String layer, String name) {
		System.out.println("Layer '" + layer + "' has envelope '" + name + "': " + bbox);
		System.out.println("\tX: [" + bbox.getMinX() + ":" + bbox.getMaxX() + "]");
		System.out.println("\tY: [" + bbox.getMinY() + ":" + bbox.getMaxY() + "]");
	}

	public static int checkIndexCount(Transaction tx, Layer layer) {
		if (layer.getIndex().count(tx) < 1) {
			System.out.println("Warning: index count zero: " + layer.getName());
		}
		System.out.println(
				"Layer '" + layer.getName() + "' has " + layer.getIndex().count(tx) + " entries in the index");
		return layer.getIndex().count(tx);
	}

	public static void checkFeatureCount(GraphDatabaseService db, int indexCount, String layerName) throws IOException {
		DataStore store = new Neo4jSpatialDataStore(db);
		SimpleFeatureCollection features = store.getFeatureSource(layerName).getFeatures();
		int featuresSize = features.size();
		System.out.println("Layer '" + layerName + "' has " + featuresSize + " features");
		Assertions.assertEquals(indexCount, featuresSize,
				"FeatureCollection.size for layer '" + layerName + "' not the same as index count");
	}

	private static void checkChangesetsAndUsers(Transaction tx, OSMLayer layer) {
		double totalMatch = 0.0;
		int waysMatched = 0;
		int waysCounted = 0;
		int nodesCounted = 0;
		int waysMissing = 0;
		int nodesMissing = 0;
		int usersMissing = 0;
		float maxMatch = 0.0f;
		float minMatch = 1.0f;
		HashMap<String, Integer> userNodeCount = new HashMap<>();
		HashMap<String, String> userNames = new HashMap<>();
		HashMap<String, Long> userIds = new HashMap<>();
		OSMDataset dataset = OSMDataset.fromLayer(tx, layer);
		for (Node way : dataset.getAllWayNodes(tx)) {
			int node_count = 0;
			int match_count = 0;
			Assertions.assertNull(way.getProperty("changeset", null), "Way has changeset property");
			Node wayChangeset = OSMDataset.getChangeset(way);
			if (wayChangeset != null) {
				long wayCS = (Long) wayChangeset.getProperty("changeset");
				for (Node node : OSMDataset.getWayNodes(way)) {
					Assertions.assertNull(node.getProperty("changeset", null), "Node has changeset property");
					Node nodeChangeset = OSMDataset.getChangeset(node);
					if (nodeChangeset == null) {
						nodesMissing++;
					} else {
						long nodeCS = (Long) nodeChangeset.getProperty("changeset");
						if (nodeChangeset.equals(wayChangeset)) {
							match_count++;
						} else {
							Assertions.assertNotEquals(wayCS, nodeCS,
									"Two changeset nodes should not have the same changeset number: way(" + wayCS
											+ ")==node(" + nodeCS + ")");
						}
						Node user = OSMDataset.getUser(nodeChangeset);
						if (user != null) {
							String userid = user.getElementId();
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
		System.out.println(
				"\twe found " + waysMatched + " ways with an average of " + (nodesCounted / waysMatched) + " nodes");
		System.out.println("\tand an average of " + (100.0 * totalMatch / waysMatched) + "% matching changesets");
		System.out.println("\twith min-match " + (100.0 * minMatch) + "% and max-match " + (100.0 * maxMatch) + "%");
		System.out.println("\tWays missing changsets: " + waysMissing);
		System.out.println(
				"\tNodes missing changsets: " + nodesMissing + " (~" + (nodesMissing / waysMatched) + " / way)");
		System.out.println(
				"\tUnique users: " + userNodeCount.size() + " (with " + usersMissing + " changeset missing users)");
		ArrayList<ArrayList<String>> userCounts = new ArrayList<>();
		for (String user : userNodeCount.keySet()) {
			int count = userNodeCount.get(user);
			userCounts.ensureCapacity(count);
			while (userCounts.size() < count + 1) {
				userCounts.add(null);
			}
			ArrayList<String> userSet = userCounts.get(count);
			if (userSet == null) {
				userSet = new ArrayList<>();
			}
			userSet.add(user);
			userCounts.set(count, userSet);
		}
		if (userCounts.size() > 1) {
			System.out.println("\tTop 20 users (nodes: users):");
			for (int ui = userCounts.size() - 1, i = 0; i < 20 && ui >= 0; ui--) {
				ArrayList<String> userSet = userCounts.get(ui);
				if (userSet != null && userSet.size() > 0) {
					i++;
					StringBuilder us = new StringBuilder();
					for (String user : userSet) {
						Node userNode = tx.getNodeByElementId(user);
						int csCount = 0;
						for (@SuppressWarnings("unused")
						Relationship rel : userNode.getRelationships(Direction.INCOMING, OSMRelation.USER)) {
							csCount++;
						}
						String name = userNames.get(user);
						Long uid = userIds.get(user);
						if (us.length() > 0) {
							us.append(", ");
						}
						us.append(String.format("%s (uid=%d, id=%s, changesets=%d)", name, uid, user, csCount));
					}
					System.out.println("\t\t" + ui + ": " + us);
				}
			}
		}
	}

	protected void runImport(String osm, boolean includePoints) throws Exception {
		// TODO: Consider merits of using dependency data in target/osm,
		// downloaded by maven, as done in TestSpatial, versus the test data
		// committed to source code as done here
		String osmPath = checkOSMFile(osm);
		if (osmPath == null) {
			return;
		}
		printDatabaseStats();
		loadTestOsmData(osm, osmPath, includePoints);
		checkOSMLayer(graphDb(), osm);
		printDatabaseStats();
	}

	protected void loadTestOsmData(String layerName, String osmPath, boolean includePoints) throws Exception {
		System.out.printf("\n=== Loading layer '%s' from %s, includePoints=%b ===\n", layerName, osmPath,
				includePoints);
		long start = System.currentTimeMillis();
		// tag::importOsm[] START SNIPPET: importOsm
		OSMImporter importer = new OSMImporter(layerName, new ConsoleListener());
		importer.setCharset(StandardCharsets.UTF_8);
		importer.importFile(graphDb(), osmPath, includePoints, 5000);
		// end::importOsm[] END SNIPPET: importOsm
		// Weird hack to force GC on large loads
		if (System.currentTimeMillis() - start > 300000) {
			for (int i = 0; i < 3; i++) {
				System.gc();
				Thread.sleep(1000);
			}
		}
		importer.reIndex(graphDb(), 1000, includePoints);
	}
}
