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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.data.neo4j.StyledImageExporter;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Coordinate;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.osm.OSMDataset;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.osm.OSMRelation;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.filter.SearchAll;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class OsmAnalysisTest extends TestOSMImportBase {

	public static final String spatialTestMode = System.getProperty("spatial.test.mode");
	public static final boolean usePoints = true;

	private static Stream<Arguments> parameters() {
		deleteBaseDir();
		String[] smallModels = new String[]{"one-street.osm", "two-street.osm"};
		//String[] mediumModels = new String[]{"map.osm", "map2.osm"};
		String[] largeModels = new String[]{"cyprus.osm", "croatia.osm", "denmark.osm"};

		// Setup default test cases (short or medium only, no long cases)
		ArrayList<String> layersToTest = new ArrayList<>(Arrays.asList(smallModels));
//		layersToTest.addAll(Arrays.asList(mediumModels));

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
			// layersToTest.add("/home/craig/Desktop/AWE/Data/MapData/baden-wurttemberg.osm/baden-wurttemberg.osm");
			// layersToTest.add("cyprus.osm");
			// layersToTest.add("croatia.osm");
			layersToTest.add("cyprus.osm");
		}

		int[] years = new int[]{3};
		int[] days = new int[]{1};

		// Finally build the set of complete test cases based on the collection above
		ArrayList<Arguments> params = new ArrayList<>();
		for (final String layerName : layersToTest) {
			for (final int y : years) {
				for (final int d : days) {
					params.add(Arguments.of(layerName, y, d));
				}
			}
		}
		System.out.println("This suite has " + params.size() + " tests");
		for (Arguments arguments : params) {
			System.out.println("\t" + Arrays.toString(arguments.get()));
		}
		return params.stream();
	}

	@ParameterizedTest
	@MethodSource("parameters")
	public void runTest(String layerName, int years, int days) throws Exception {
		runAnalysis(layerName, years, days);
	}

	protected void runAnalysis(String osm, int years, int days) throws Exception {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		boolean alreadyImported;
		try (Transaction tx = graphDb().beginTx()) {
			alreadyImported = spatial.getLayer(tx, osm) != null;
			tx.commit();
		}
		if (!alreadyImported) {
			runImport(osm, usePoints);
		}
		testAnalysis2(osm, years, days);
	}

	public void testAnalysis2(String osm, int years, int days) throws IOException {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		LinkedHashMap<DynamicLayerConfig, Long> slides = new LinkedHashMap<>();
		Map<String, User> userIndex = new HashMap<>();
		int user_rank = 1;
		long latestTimestamp = 0L;
		long firstTimestamp = Long.MAX_VALUE;
		try (Transaction tx = graphDb().beginTx()) {
			OSMLayer layer = (OSMLayer) spatial.getLayer(tx, osm);
			OSMDataset dataset = OSMDataset.fromLayer(tx, layer);

			for (Node cNode : dataset.getAllChangesetNodes(tx)) {
				long timestamp = (Long) cNode.getProperty("timestamp", 0L);
				Node userNode = OSMDataset.getUser(cNode);
				String name = (String) userNode.getProperty("name");

				User user = userIndex.get(name);
				if (user == null) {
					user = new User(userNode.getElementId(), name);
					userIndex.put(name, user);
				}
				user.addChangeset(cNode, timestamp);
				if (latestTimestamp < timestamp) {
					latestTimestamp = timestamp;
				}
				if (firstTimestamp > timestamp) {
					firstTimestamp = timestamp;
				}
			}
			tx.commit();
		}
		SortedSet<User> topTen = getTopTen(userIndex);
		try (Transaction tx = graphDb().beginTx()) {
			OSMLayer layer = (OSMLayer) spatial.getLayer(tx, osm);
			Date latest = new Date(latestTimestamp);
			Calendar time = Calendar.getInstance();
			time.setTime(latest);
			int slidesPerYear = 360 / days;
			int slideCount = slidesPerYear * years;
			long msPerSlide = (long) days * 24 * 3600000;
			int timeWindow = 15;
			StringBuilder userQuery = new StringBuilder();
			for (User user : topTen) {
				if (userQuery.length() > 0) {
					userQuery.append(" or ");
				}
				userQuery.append("user = '").append(user.name).append("'");
				user_rank++;
			}
			for (int i = -timeWindow; i < slideCount; i++) {
				long timestamp = latestTimestamp - i * msPerSlide;
				long maxTime = timestamp + 15 * msPerSlide;
				time.setTimeInMillis(timestamp);
				Date date = new Date(timestamp);
				System.out.println("Preparing slides for " + date);
				String name = osm + "-" + date;
				DynamicLayerConfig config = layer.addLayerConfig(tx, name, Constants.GTYPE_GEOMETRY,
						"timestamp > " + timestamp + " and timestamp < "
								+ maxTime + " and (" + userQuery + ")");
				System.out.println("Added dynamic layer '" + config.getName() + "' with CQL: " + config.getQuery());
				slides.put(config, timestamp);
			}
			DynamicLayerConfig config = layer.addLayerConfig(tx, osm + "-top-ten", Constants.GTYPE_GEOMETRY,
					userQuery.toString());
			System.out.println("Added dynamic layer '" + config.getName() + "' with CQL: " + config.getQuery());
			slides.clear();
			slides.put(config, 0L);
			tx.commit();
		}

		StyledImageExporter imageExporter = new StyledImageExporter(graphDb());
		String exportDir = "target/export/" + osm + "/analysis";
		imageExporter.setExportDir(exportDir);
		imageExporter.setZoom(2.0);
		imageExporter.setOffset(-0.2, 0.25);
		imageExporter.setSize(1280, 800);
		imageExporter.setStyleFiles(new String[]{"sld/background.sld", "sld/rank.sld"});

		String[] layerPropertyNames = new String[]{"name", "timestamp", "user", "days", "user_rank"};
		StringBuilder userParams = new StringBuilder();
		user_rank = 1;
		for (User user : topTen) {
			if (userParams.length() > 0) {
				userParams.append(",");
			}
			userParams.append(user.name).append(":").append(user_rank);
			user_rank++;
		}

		boolean checkedOne = false;

		for (DynamicLayerConfig layerToExport : slides.keySet()) {
			try (Transaction tx = graphDb().beginTx()) {
				layerToExport.setExtraPropertyNames(tx, layerPropertyNames);
				layerToExport.getPropertyMappingManager()
						.addPropertyMapper(tx, "timestamp", "days", "Days", Long.toString(slides.get(layerToExport)));
				layerToExport.getPropertyMappingManager()
						.addPropertyMapper(tx, "user", "user_rank", "Map", userParams.toString());
				if (!checkedOne) {
					int i = 0;
					System.out.println("Checking layer '" + layerToExport + "' in detail");
					SearchRecords records = layerToExport.getIndex().search(tx, new SearchAll());
					for (SpatialRecord record : records) {
						System.out.println("Got record " + i + ": " + record);
						for (String name : record.getPropertyNames(tx)) {
							System.out.println("\t" + name + ":\t" + record.getProperty(tx, name));
							checkedOne = true;
						}
						if (i++ > 10) {
							break;
						}
					}
				}
				tx.commit();
			}

			imageExporter.saveLayerImage(new String[]{osm, layerToExport.getName()},
					new File(layerToExport.getName() + ".png"));
			//break;
		}
	}

	public void testAnalysis(String osm) throws Exception {
		SortedMap<String, Layer> layers;
		ReferencedEnvelope bbox;
		try (Transaction tx = graphDb().beginTx()) {
			Node osmImport = tx.findNode(OSMImporter.LABEL_DATASET, "name", osm);
			Node usersNode = osmImport.getSingleRelationship(OSMRelation.USERS, Direction.OUTGOING).getEndNode();

			Map<String, User> userIndex = collectUserChangesetData(usersNode);
			SortedSet<User> topTen = getTopTen(userIndex);

			SpatialDatabaseService spatial = new SpatialDatabaseService(
					new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
			layers = exportPoints(tx, osm, spatial, topTen);

			layers = removeEmptyLayers(tx, layers);
			bbox = getEnvelope(tx, layers.values());
			tx.commit();
		}

		StyledImageExporter imageExporter = new StyledImageExporter(graphDb());
		String exportDir = "target/export/" + osm + "/analysis";
		imageExporter.setExportDir(exportDir);
		imageExporter.setZoom(2.0);
		imageExporter.setOffset(-0.05, -0.05);
		imageExporter.setSize(1280, 800);

		for (String layerName : layers.keySet()) {
			SortedMap<String, Layer> layersSubset = new TreeMap<>(layers.headMap(layerName));

			String[] to_render = new String[Math.min(10, layersSubset.size() + 1)];
			to_render[0] = layerName;
			if (layersSubset.size() > 0) {
				for (int i = 1; i < to_render.length; i++) {
					String name = layersSubset.lastKey();
					layersSubset.remove(name);
					to_render[i] = name;
				}
			}

			System.out.println("exporting " + layerName);
			imageExporter.saveLayerImage(
					to_render, // (String[])
					// layersSubset.keySet().toArray(new
					// String[] {}),
					"/Users/davidesavazzi/Desktop/amanzi/awe trial/osm_germany/germany_poi_small.sld",
					new File(layerName + ".png"), bbox);
		}
	}

	private static ReferencedEnvelope getEnvelope(Transaction tx, Collection<Layer> layers) {
		CoordinateReferenceSystem crs = null;

		Envelope envelope = null;
		for (Layer layer : layers) {
			Envelope bbox = layer.getIndex().getBoundingBox(tx);
			if (envelope == null) {
				envelope = new Envelope(bbox);
			} else {
				envelope.expandToInclude(bbox);
			}
			if (crs == null) {
				crs = layer.getCoordinateReferenceSystem(tx);
			}
		}

		return new ReferencedEnvelope(Utilities.fromNeo4jToJts(envelope), crs);
	}

	private static SortedMap<String, Layer> removeEmptyLayers(Transaction tx, Map<String, Layer> layers) {
		SortedMap<String, Layer> result = new TreeMap<>();

		for (Entry<String, Layer> entry : layers.entrySet()) {
			if (entry.getValue().getIndex().count(tx) > 0) {
				result.put(entry.getKey(), entry.getValue());
			}
		}

		return result;
	}

	private static SortedMap<String, Layer> exportPoints(Transaction tx, String layerPrefix,
			SpatialDatabaseService spatialService, Set<User> users) {
		SortedMap<String, Layer> layers = new TreeMap<>();
		int startYear = 2009;
		int endYear = 2011;

		for (int y = startYear; y <= endYear; y++) {
			for (int w = 1; w <= 52; w++) {
				if (y == 2011 && w == 36) {
					break;
				}

				String name = layerPrefix + "-" + y + "_";
				if (w >= 10) {
					name += w;
				} else {
					name += "0" + w;
				}

				EditableLayerImpl layer = (EditableLayerImpl) spatialService.createLayer(tx, name,
						WKBGeometryEncoder.class, EditableLayerImpl.class, "");
				layer.setExtraPropertyNames(
						new String[]{"user_id", "user_name", "year", "month", "dayOfMonth", "weekOfYear"}, tx);

				layers.put(name, layer);
			}
		}

		for (User user : users) {
			Node userNode = tx.getNodeByElementId(user.id);
			System.out.println("analyzing user: " + userNode.getProperty("name"));
			for (Relationship r : userNode.getRelationships(Direction.INCOMING, OSMRelation.USER)) {
				Node changeset = r.getStartNode();
				if (changeset.hasProperty("changeset")) {
					System.out.println("analyzing changeset: " + changeset.getProperty("changeset"));
					for (Relationship nr : changeset.getRelationships(Direction.INCOMING, OSMRelation.CHANGESET)) {
						Node changedNode = nr.getStartNode();
						if (changedNode.hasProperty("node_osm_id") && changedNode.hasProperty("timestamp")) {
							long timestamp = (Long) changedNode.getProperty("timestamp");

							Calendar c = Calendar.getInstance();
							c.setTimeInMillis(timestamp);
							int nodeYear = c.get(Calendar.YEAR);
							int nodeWeek = c.get(Calendar.WEEK_OF_YEAR);

							if (layers.containsKey(layerPrefix + "-" + nodeYear + "_" + nodeWeek)) {
								EditableLayer l = (EditableLayer) layers.get(
										layerPrefix + "-" + nodeYear + "_" + nodeWeek);
								l.add(tx, l.getGeometryFactory().createPoint(
												new Coordinate((Double) changedNode.getProperty("lon"), (Double) changedNode
														.getProperty("lat"))),
										new String[]{"user_id", "user_name", "year", "month",
												"dayOfMonth", "weekOfYear"},
										new Object[]{user.internalId, user.name, c.get(Calendar.YEAR),
												c.get(Calendar.MONTH),
												c.get(Calendar.DAY_OF_MONTH), c.get(Calendar.WEEK_OF_YEAR)});
							}
						}
					}
				}
			}
		}

		return layers;
	}

	private static SortedSet<User> getTopTen(Map<String, User> userIndex) {
		SortedSet<User> userList = new TreeSet<>(userIndex.values());
		SortedSet<User> topTen = new TreeSet<>();

		int count = 0;
		for (User user : userList) {
			if (count < 10) {
				topTen.add(user);
				user.internalId = count++;
			} else {
				break;
			}
		}

		for (User user : topTen) {
			System.out.println(user.id + "# " + user.name + " = " + user.changesets.size());
		}

		return topTen;
	}

	private static Map<String, User> collectUserChangesetData(Node usersNode) {
		Map<String, User> userIndex = new HashMap<>();
		for (Relationship r : usersNode.getRelationships(Direction.OUTGOING, OSMRelation.OSM_USER)) {
			Node userNode = r.getEndNode();
			String name = (String) userNode.getProperty("name");

			User user = new User(userNode.getElementId(), name);
			userIndex.put(name, user);

			for (Relationship ur : userNode.getRelationships(Direction.INCOMING, OSMRelation.USER)) {
				Node node = ur.getStartNode();
				if (node.hasProperty("changeset")) {
					user.changesets.add(node.getElementId());
				}
			}
		}

		return userIndex;
	}

	static class User implements Comparable<User> {

		String id;
		int internalId;
		String name;
		List<String> changesets = new ArrayList<>();
		long latestTimestamp = 0L;

		public User(String id, String name) {
			this.id = id;
			this.name = name;
		}

		public void addChangeset(Node cNode, long timestamp) {
			changesets.add(cNode.getElementId());
			if (latestTimestamp < timestamp) {
				latestTimestamp = timestamp;
			}
		}

		@Override
		public int compareTo(User other) {
			return -1 * Integer.compare(changesets.size(), other.changesets.size());
		}
	}
}
