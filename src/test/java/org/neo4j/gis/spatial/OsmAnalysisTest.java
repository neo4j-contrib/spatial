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
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
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

import junit.framework.Test;
import junit.framework.TestSuite;

import org.geotools.data.neo4j.StyledImageExporter;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.neo4j.gis.spatial.utilities.ReferenceNodes;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.filter.SearchAll;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.osm.OSMDataset;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.osm.OSMRelation;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Coordinate;

public class OsmAnalysisTest extends TestOSMImport {
	public static final String spatialTestMode = System.getProperty("spatial.test.mode");
	public static final boolean usePoints = true;
	public static final boolean useBatchInserter = false;

	public OsmAnalysisTest(String layerName) {
		super(layerName, usePoints, useBatchInserter);
		setName("DavideOsmAnalysisTest: " + layerName);
	}

	public static Test suite() {
		deleteBaseDir();
		TestSuite suite = new TestSuite();
		String[] smallModels = new String[] { "one-street.osm", "two-street.osm" };
		String[] mediumModels = new String[] { "map.osm", "map2.osm" };
		String[] largeModels = new String[] { "cyprus.osm", "croatia.osm", "denmark.osm" };

		// Setup default test cases (short or medium only, no long cases)
		ArrayList<String> layersToTest = new ArrayList<String>();
		layersToTest.addAll(Arrays.asList(smallModels));
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

		int years[] = new int[]{3};
		int days[] = new int[]{1};

		// Finally build the set of complete test cases based on the collection
		// above
		for (final String layerName : layersToTest) {
			for(final int y:years) {
				for(final int d:days){
					suite.addTest(new OsmAnalysisTest(layerName) {
						public void runTest() {
							try {
								runAnalysis(layerName, y, d);
							} catch (Exception e) {
								// assertTrue("Failed to run import test due to exception: "
								// + e, false);
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

	private String dataset;
	private GraphDatabaseService db;
	
	protected GraphDatabaseService graphDb() {
		return db==null ? super.graphDb() : db;
	}
	
	protected void shutdownDatabase() {
		if(db!=null) {
			db.shutdown();
			db = null;
		}
	}
	
	protected SpatialDatabaseService setDataset(String dataset) {
		this.dataset = dataset;
		if (db != null) {
			shutdownDatabase();
		}
		db = new GraphDatabaseFactory().newEmbeddedDatabase("var/" + dataset);
		return new SpatialDatabaseService(db);
	}
	
	protected void runAnalysis(String osm, int years, int days) throws Exception {
		if(setDataset(osm).getLayer(osm)==null){
			runImport(osm, usePoints, useBatchInserter);
		}
		try (Transaction tx = graphDb().beginTx()) {
			testAnalysis2(osm, years, days);
			tx.success();
		}
		shutdownDatabase();
	}

	public void testAnalysis2(String osm, int years, int days) throws IOException {
		SpatialDatabaseService spatial = new SpatialDatabaseService(graphDb());
		OSMLayer layer = (OSMLayer) spatial.getLayer(osm);
		OSMDataset dataset = (OSMDataset) layer.getDataset();
		Map<String, User> userIndex = new HashMap<String, User>();
		long latestTimestamp = 0L;
		long firstTimestamp = Long.MAX_VALUE;

		for (Node cNode : dataset.getAllChangesetNodes()) {
			long timestamp = (Long) cNode.getProperty("timestamp", 0L);
			Node userNode = dataset.getUser(cNode);
			String name = (String) userNode.getProperty("name");

			User user = userIndex.get(name);
			if (user == null) {
				user = new User(userNode.getId(), name);
				userIndex.put(name, user);
			}
			user.addChangeset(cNode, timestamp);
			if (latestTimestamp < timestamp)
				latestTimestamp = timestamp;
			if (firstTimestamp > timestamp)
				firstTimestamp = timestamp;
		}
		SortedSet<User> topTen = getTopTen(userIndex);
		
		Date latest = new Date(latestTimestamp);
		Calendar time = Calendar.getInstance();
		time.setTime(latest);
		int slidesPerYear = 360/days;
		int slideCount = slidesPerYear * years;
		long msPerSlide = days * 24 * 3600000;
		int timeWindow = 15;
		StringBuffer userQuery = new StringBuffer();
		int user_rank = 1;
		for (User user : topTen) {
			if (userQuery.length() > 0)
				userQuery.append(" or ");
			userQuery.append("user = '" + user.name + "'");
			user_rank++;
		}
		LinkedHashMap<DynamicLayerConfig,Long> slides = new LinkedHashMap<DynamicLayerConfig,Long>();
		for (int i = -timeWindow; i < slideCount; i++) {
			long timestamp = latestTimestamp - i * msPerSlide;
			long minTime = timestamp;
			long maxTime = timestamp + 15 * msPerSlide;
			time.setTimeInMillis(timestamp);
			Date date = new Date(timestamp);
			System.out.println("Preparing slides for " + date);
			String name = osm + "-" + date;
			DynamicLayerConfig config = layer.addLayerConfig(name, Constants.GTYPE_GEOMETRY, "timestamp > " + minTime + " and timestamp < "
					+ maxTime + " and (" + userQuery + ")");
			System.out.println("Added dynamic layer '"+config.getName()+"' with CQL: "+config.getQuery());
			slides.put(config, timestamp);
		}
		DynamicLayerConfig config = layer.addLayerConfig(osm + "-top-ten", Constants.GTYPE_GEOMETRY, userQuery.toString());
		System.out.println("Added dynamic layer '"+config.getName()+"' with CQL: "+config.getQuery());
		slides.clear();
		slides.put(config, 0L);

		StyledImageExporter imageExporter = new StyledImageExporter(graphDb());
		String exportDir = "target/export/" + osm + "/analysis";
		imageExporter.setExportDir(exportDir);
		imageExporter.setZoom(2.0);
		imageExporter.setOffset(-0.2, 0.25);
		imageExporter.setSize(1280, 800);
		imageExporter.setStyleFiles(new String[] { "sld/background.sld", "sld/rank.sld" });

		String[] layerPropertyNames = new String[]{"name", "timestamp", "user", "days", "user_rank"};
		StringBuffer userParams = new StringBuffer();
		user_rank = 1;
		for (User user : topTen) {
			if(userParams.length()>0) userParams.append(",");
			userParams.append(user.name).append(":").append(user_rank);
			user_rank++;
		}
		
		boolean checkedOne = false;

		for(DynamicLayerConfig layerToExport:slides.keySet()){
			layerToExport.setExtraPropertyNames(layerPropertyNames);
			layerToExport.getPropertyMappingManager().addPropertyMapper("timestamp", "days", "Days", Long.toString(slides.get(layerToExport)));
			layerToExport.getPropertyMappingManager().addPropertyMapper("user", "user_rank", "Map", userParams.toString());
			if (!checkedOne) {
				int i = 0;
				System.out.println("Checking layer '" + layerToExport + "' in detail");
				SearchRecords records = layerToExport.getIndex().search(new SearchAll());
				for (SpatialRecord record : records) {
					System.out.println("Got record " + i + ": " + record);
					for (String name : record.getPropertyNames()) {
						System.out.println("\t" + name + ":\t" + record.getProperty(name));
						checkedOne = true;
					}
					if (i++ > 10)
						break;
				}
			}
			
			imageExporter.saveLayerImage(new String[] { osm, layerToExport.getName() }, new File(layerToExport.getName() + ".png"));
			//break;
		}
	}

	public void testAnalysis(String osm, int years, int days) throws Exception {
		Node osmRoot = ReferenceNodes.getReferenceNode(graphDb(),"osm_root");
		Node osmImport = osmRoot.getSingleRelationship(OSMRelation.OSM, Direction.OUTGOING).getEndNode();
		Node usersNode = osmImport.getSingleRelationship(OSMRelation.USERS, Direction.OUTGOING).getEndNode();

		Map<String, User> userIndex = collectUserChangesetData(usersNode);
		SortedSet<User> topTen = getTopTen(userIndex);

		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		SortedMap<String, Layer> layers = exportPoints(osm, spatialService, topTen);

		layers = removeEmptyLayers(layers);
		ReferencedEnvelope bbox = getEnvelope(layers.values());

		StyledImageExporter imageExporter = new StyledImageExporter(graphDb());
		String exportDir = "target/export/" + osm + "/analysis";
		imageExporter.setExportDir(exportDir);
		imageExporter.setZoom(2.0);
		imageExporter.setOffset(-0.05, -0.05);
		imageExporter.setSize(1280, 800);

		for (String layerName : layers.keySet()) {
			SortedMap<String, Layer> layersSubset = new TreeMap<String, Layer>(layers.headMap(layerName));

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

	private ReferencedEnvelope getEnvelope(Collection<Layer> layers) {
		CoordinateReferenceSystem crs = null;

		Envelope envelope = new Envelope();
		for (Layer layer : layers) {
			envelope.expandToInclude(layer.getIndex().getBoundingBox());
			if (crs == null) {
				crs = layer.getCoordinateReferenceSystem();
			}
		}

		return new ReferencedEnvelope(Utilities.fromNeo4jToJts(envelope), crs);
	}

	private SortedMap<String, Layer> removeEmptyLayers(Map<String, Layer> layers) {
		SortedMap<String, Layer> result = new TreeMap<String, Layer>();

		for (Entry<String, Layer> entry : layers.entrySet()) {
			if (entry.getValue().getIndex().count() > 0) {
				result.put(entry.getKey(), entry.getValue());
			}
		}

		return result;
	}

	private SortedMap<String, Layer> exportPoints(String layerPrefix, SpatialDatabaseService spatialService,
			Set<User> users) {
		SortedMap<String, Layer> layers = new TreeMap<String, Layer>();
		int startYear = 2009;
		int endYear = 2011;

		for (int y = startYear; y <= endYear; y++) {
			for (int w = 1; w <= 52; w++) {
				if (y == 2011 && w == 36) {
					break;
				}

				String name = layerPrefix + "-" + y + "_";
				if (w >= 10)
					name += w;
				else
					name += "0" + w;

				EditableLayerImpl layer = (EditableLayerImpl) spatialService.createLayer(name, WKBGeometryEncoder.class,
						EditableLayerImpl.class);
				layer.setExtraPropertyNames(new String[] { "user_id", "user_name", "year", "month", "dayOfMonth", "weekOfYear" });

				// EditableLayerImpl layer = (EditableLayerImpl)
				// spatialService.getLayer(name);

				layers.put(name, layer);
			}
		}

		for (User user : users) {
			Node userNode = graphDb().getNodeById(user.id);
			System.out.println("analyzing user: " + userNode.getProperty("name"));
			for (Relationship r : userNode.getRelationships(Direction.INCOMING, OSMRelation.USER)) {
				Node changeset = r.getStartNode();
				if (changeset.hasProperty("changeset")) {
					System.out.println("analyzing changeset: " + changeset.getProperty("changeset"));
					Transaction tx = graphDb().beginTx();
					try {
						for (Relationship nr : changeset.getRelationships(Direction.INCOMING, OSMRelation.CHANGESET)) {
							Node changedNode = nr.getStartNode();
							if (changedNode.hasProperty("node_osm_id") && changedNode.hasProperty("timestamp")) {
								long timestamp = (Long) changedNode.getProperty("timestamp");

								Calendar c = Calendar.getInstance();
								c.setTimeInMillis(timestamp);
								int nodeYear = c.get(Calendar.YEAR);
								int nodeWeek = c.get(Calendar.WEEK_OF_YEAR);

								if (layers.containsKey(layerPrefix + "-" + nodeYear + "_" + nodeWeek)) {
									EditableLayer l = (EditableLayer) layers.get(layerPrefix + "-" + nodeYear + "_" + nodeWeek);
									l.add(l.getGeometryFactory().createPoint(
											new Coordinate((Double) changedNode.getProperty("lon"), (Double) changedNode
													.getProperty("lat"))), new String[] { "user_id", "user_name", "year", "month",
											"dayOfMonth", "weekOfYear" },
											new Object[] { user.internalId, user.name, c.get(Calendar.YEAR), c.get(Calendar.MONTH),
													c.get(Calendar.DAY_OF_MONTH), c.get(Calendar.WEEK_OF_YEAR) });
								}
							}
						}

						tx.success();
					} finally {
						tx.close();
					}
				}
			}
		}

		return layers;
	}

	private SortedSet<User> getTopTen(Map<String, User> userIndex) {
		SortedSet<User> userList = new TreeSet<User>();
		userList.addAll(userIndex.values());
//		for (String name : userIndex.keySet()) {
//			userList.add(userIndex.get(name));
//		}

		SortedSet<User> topTen = new TreeSet<User>();

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

	private Map<String, User> collectUserChangesetData(Node usersNode) {
		Map<String, User> userIndex = new HashMap<String, User>();
		for (Relationship r : usersNode.getRelationships(Direction.OUTGOING, OSMRelation.OSM_USER)) {
			Node userNode = r.getEndNode();
			String name = (String) userNode.getProperty("name");

			User user = new User(userNode.getId(), name);
			userIndex.put(name, user);

			for (Relationship ur : userNode.getRelationships(Direction.INCOMING, OSMRelation.USER)) {
				Node node = ur.getStartNode();
				if (node.hasProperty("changeset")) {
					user.changesets.add(node.getId());
				}
			}
		}

		return userIndex;
	}

	class User implements Comparable<User> {

		long id;
		int internalId;
		String name;
		List<Long> changesets = new ArrayList<Long>();
		long latestTimestamp = 0L;

		public User(long id, String name) {
			this.id = id;
			this.name = name;
		}

		public void addChangeset(Node cNode, long timestamp) {
			changesets.add(cNode.getId());
			if (latestTimestamp < timestamp)
				latestTimestamp = timestamp;
		}

		@Override
		public int compareTo(User other) {
			return -1 * ((Integer) changesets.size()).compareTo((Integer) other.changesets.size());
		}
	}
}