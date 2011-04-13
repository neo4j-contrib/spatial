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
import java.util.List;

import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.query.SearchIntersectWindow;
import org.neo4j.graphdb.GraphDatabaseService;
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

	/**
	 * Sample code for importing Open Street Map example.
	 * 
	 * @throws Exception
	 */
	public void testImportOSM() throws Exception {
		super.shutdownDatabase(true);
		deleteFileOrDirectory(new File("target/var/neo4j-db"));

		System.out.println("\n=== Simple test map.osm ===");
		// START SNIPPET: importOsm
		OSMImporter importer = new OSMImporter("map.osm");

		BatchInserter batchInserter = new BatchInserterImpl("target/var/neo4j-db");
		importer.importFile(batchInserter, "map.osm", false);
		batchInserter.shutdown();

		GraphDatabaseService db = new EmbeddedGraphDatabase("target/var/neo4j-db");
		importer.reIndex(db);
		db.shutdown();
		// END SNIPPET: importOsm

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

	}

}
