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
import java.util.HashMap;
import java.util.Map;

import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

class SpatialOsmImport {
	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		/**
		 * args[0] -> OSM name args[1] -> Graph data path args[2] -> OSM File
		 * path
         * example:
         * mvn clean compile exec:java -Dexec.mainClass="SpatialOsmImport" -Dexec.args="test.osm target/osm-db one-street.osm"
		 **/
		OSMImporter importer = new OSMImporter(args[0].toString());
		 Map<String, String> config = new HashMap<String, String>();
		 config.put("neostore.nodestore.db.mapped_memory", "500M" );
		 config.put("neostore.relationshipstore.db.mapped_memory", "500M" );
		 config.put("dump_configuration", "true");
		 config.put("use_memory_mapped_buffers", "true");
		BatchInserter batchinserter = new BatchInserterImpl(args[1].toString(), config );
		try {
			importer.importFile(batchinserter, args[2].toString(), false);
			 batchinserter.shutdown();
			 System.out.println("//////////////\nFinished importing in " + (System.currentTimeMillis()-start)/1000 + "s");
			 GraphDatabaseService db = new EmbeddedGraphDatabase(args[1]);
			importer.reIndex(db, 10000);
			db.shutdown();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		System.out.println("Finished everything in " + (System.currentTimeMillis()-start)/1000 + "s");
		// batchinserter.shutdown();
	}
}