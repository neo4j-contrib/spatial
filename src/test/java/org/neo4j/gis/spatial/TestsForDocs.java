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
import java.util.HashMap;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.geotools.data.DataStore;
import org.geotools.data.shapefile.shp.ShapefileException;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.neo4j.gis.spatial.DynamicLayer.LayerConfig;
import org.neo4j.gis.spatial.geotools.data.Neo4jSpatialDataStore;
import org.neo4j.gis.spatial.osm.OSMDataset;
import org.neo4j.gis.spatial.osm.OSMDataset.OSMNode;
import org.neo4j.gis.spatial.osm.OSMDataset.Way;
import org.neo4j.gis.spatial.osm.OSMDataset.WayPoint;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.query.SearchIntersectWindow;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

import com.vividsolutions.jts.geom.Envelope;

/**
 * Some test code written specifically for the user manual. This normally means
 * we repeat some of the setup/teardown code in each test so that it is a more
 * complete explanation of how to write the code. Most other test classes rely
 * on infrastructure in the Neo4jTestCase that is unlikely to be relevant to the
 * users own coding experience.
 * 
 * @author craig
 */
public class TestsForDocs extends Neo4jTestCase {

	private void checkIndexAndFeatureCount(String layerName) throws IOException {
		GraphDatabaseService database = new EmbeddedGraphDatabase("target/var/neo4j-db");
		try {
			SpatialDatabaseService spatial = new SpatialDatabaseService(database);
			Layer layer = spatial.getLayer(layerName);
			if (layer.getIndex().count() < 1) {
				System.out.println("Warning: index count zero: " + layer.getName());
			}
			System.out.println("Layer '" + layer.getName() + "' has " + layer.getIndex().count() + " entries in the index");
			DataStore store = new Neo4jSpatialDataStore(database);
			SimpleFeatureCollection features = store.getFeatureSource(layer.getName()).getFeatures();
			System.out.println("Layer '" + layer.getName() + "' has " + features.size() + " features");
			assertEquals("FeatureCollection.size for layer '" + layer.getName() + "' not the same as index count", layer.getIndex()
					.count(), features.size());
			if (layer instanceof OSMLayer)
				checkOSMAPI(layer);
		} finally {
			database.shutdown();
		}
	}

	private void checkOSMAPI(Layer layer) {
		HashMap<Long, Integer> waysFound = new HashMap<Long, Integer>();
		long mostCommon = 0;
		int mostCount = 0;
		OSMDataset osm = (OSMDataset) layer.getDataset();
		Node wayNode = osm.getAllWayNodes().iterator().next();
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
			}
		}
		assertTrue("Start way should be most found way", way.equals(osm.getWayFromId(mostCommon)));
	}

	private void importMapOSM() throws Exception {
		// START SNIPPET: importOsm
		OSMImporter importer = new OSMImporter("map.osm");

		BatchInserter batchInserter = new BatchInserterImpl("target/var/neo4j-db");
		importer.importFile(batchInserter, "map.osm", false);
		batchInserter.shutdown();

		GraphDatabaseService db = new EmbeddedGraphDatabase("target/var/neo4j-db");
		importer.reIndex(db);
		db.shutdown();
		// END SNIPPET: importOsm
	}

	/**
	 * Sample code for importing Open Street Map example.
	 * 
	 * @throws Exception
	 */
	public void testImportOSM() throws Exception {
		super.shutdownDatabase(true);
		deleteFileOrDirectory(new File("target/var/neo4j-db"));

		System.out.println("\n=== Simple test map.osm ===");
		importMapOSM();

		// START SNIPPET: searchBBox
		GraphDatabaseService database = new EmbeddedGraphDatabase("target/var/neo4j-db");
		try {
			SpatialDatabaseService spatialService = new SpatialDatabaseService(database);
			Layer layer = spatialService.getLayer("map.osm");
			SpatialIndexReader spatialIndex = layer.getIndex();
			System.out.println("Have " + spatialIndex.count() + " geometries in " + spatialIndex.getLayerBoundingBox());

			Envelope bbox = new Envelope(12.94, 12.96, 56.04, 56.06);
			Search searchQuery = new SearchIntersectWindow(bbox);
			spatialIndex.executeSearch(searchQuery);
			List<SpatialDatabaseRecord> results = searchQuery.getResults();
			System.out.println("Found " + results.size() + " geometries in " + bbox);
			System.out.println("First geometry is " + results.get(0).getGeometry());
		} finally {
			database.shutdown();
		}
		// END SNIPPET: searchBBox

		checkIndexAndFeatureCount("map.osm");
	}

	public void testImportShapefile() throws Exception {
		super.shutdownDatabase(true);
		deleteFileOrDirectory(new File("target/var/neo4j-db"));

		System.out.println("\n=== Test Import Shapefile ===");

		// START SNIPPET: importShapefile
		GraphDatabaseService database = new EmbeddedGraphDatabase("target/var/neo4j-db");
		try {
			ShapefileImporter importer = new ShapefileImporter(database);
			importer.importFile("shp/highway.shp", "highway");
		} finally {
			database.shutdown();
		}
		// END SNIPPET: importShapefile

		checkIndexAndFeatureCount("highway");
	}

	public void testExportShapefileFromOSM() throws Exception {
		super.shutdownDatabase(true);
		deleteFileOrDirectory(new File("target/var/neo4j-db"));

		System.out.println("\n=== Test import map.osm, create DynamicLayer and export shapefile ===");
		importMapOSM();

		GraphDatabaseService database = new EmbeddedGraphDatabase("target/var/neo4j-db");
		try {
			// START SNIPPET: exportShapefileFromOSM
			SpatialDatabaseService spatialService = new SpatialDatabaseService(database);
			OSMLayer layer = (OSMLayer) spatialService.getLayer("map.osm");
			LayerConfig wayLayer = layer.addSimpleDynamicLayer(Constants.GTYPE_LINESTRING);
			ShapefileExporter shpExporter = new ShapefileExporter(database);
			shpExporter.exportLayer(wayLayer.getName());
			// END SNIPPET: exportShapefileFromOSM
		} finally {
			database.shutdown();
		}
	}

	public void testExportShapefileFromQuery() throws Exception {
		super.shutdownDatabase(true);
		deleteFileOrDirectory(new File("target/var/neo4j-db"));

		System.out.println("\n=== Test import map.osm, create DynamicLayer and export shapefile ===");
		importMapOSM();

		GraphDatabaseService database = new EmbeddedGraphDatabase("target/var/neo4j-db");
		try {
			// START SNIPPET: exportShapefileFromQuery
			SpatialDatabaseService spatialService = new SpatialDatabaseService(database);
			Layer layer = spatialService.getLayer("map.osm");
			SpatialIndexReader spatialIndex = layer.getIndex();
			System.out.println("Have " + spatialIndex.count() + " geometries in " + spatialIndex.getLayerBoundingBox());

			Envelope bbox = new Envelope(12.94, 12.96, 56.04, 56.06);
			Search searchQuery = new SearchIntersectWindow(bbox);
			spatialIndex.executeSearch(searchQuery);
			List<SpatialDatabaseRecord> results = searchQuery.getResults();

			Layer resultsLayer = spatialService.createResultsLayer("results", results);
			ShapefileExporter shpExporter = new ShapefileExporter(database);
			shpExporter.exportLayer("results");
			// END SNIPPET: exportShapefileFromQuery

			System.out.println("Found " + results.size() + " geometries in " + bbox);
			System.out.println("First geometry is " + results.get(0).getGeometry());
		} finally {
			database.shutdown();
		}
	}

}
