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

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gis.spatial.index.IndexManagerImpl;
import org.neo4j.gis.spatial.osm.OSMDataset;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.rtree.filter.SearchAll;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spatial.api.SpatialRecord;
import org.neo4j.spatial.api.SpatialRecords;
import org.neo4j.spatial.cli.tools.StyledImageExporter;

public class OsmAnalysisTest extends TestOSMImportBase {

	private static final boolean usePoints = true;

	private static Stream<Arguments> parameters() {
		List<String> layersToTest = List.of("one-street.osm", "two-street.osm");

		int[] years = new int[]{3};
		int[] days = new int[]{1};

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
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		boolean alreadyImported;
		try (Transaction tx = graphDb().beginTx()) {
			alreadyImported = spatial.getLayer(tx, osm, true) != null;
			tx.commit();
		}
		if (!alreadyImported) {
			runImport(osm, usePoints);
		}
		testAnalysis2(osm, years, days);
	}

	public void testAnalysis2(String osm, int years, int days) throws IOException {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		LinkedHashMap<DynamicLayerConfig, Long> slides = new LinkedHashMap<>();
		Map<String, User> userIndex = new HashMap<>();
		int user_rank = 1;
		long latestTimestamp = 0L;
		long firstTimestamp = Long.MAX_VALUE;
		try (Transaction tx = graphDb().beginTx()) {
			OSMLayer layer = (OSMLayer) spatial.getLayer(tx, osm, true);
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
			OSMLayer layer = (OSMLayer) spatial.getLayer(tx, osm, false);
			Date latest = new Date(latestTimestamp);
			Calendar time = Calendar.getInstance();
			time.setTime(latest);
			int slidesPerYear = 360 / days;
			int slideCount = slidesPerYear * years;
			long msPerSlide = (long) days * 24 * 3600000;
			int timeWindow = 15;
			StringBuilder userQuery = new StringBuilder();
			for (User user : topTen) {
				if (!userQuery.isEmpty()) {
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

		StyledImageExporter imageExporter = new StyledImageExporter(driver, DEFAULT_DATABASE_NAME);
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
			if (!userParams.isEmpty()) {
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
					SpatialRecords records = layerToExport.getIndex().search(tx, new SearchAll());
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
