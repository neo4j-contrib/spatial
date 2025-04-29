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
package org.neo4j.gis.spatial.osm;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.ShapefileExporter;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class OSMLayerToShapefileExporter {

	/**
	 * This method allows for a console, command-line application for loading
	 * accessing an existing database containing an existing OSM model, and
	 * exporting one or more dynamic layers to shapefiles. The layer
	 * specifications are key-value pairs (dot separated). If the value is left
	 * out, all values are accepted (test for existance of key only).
	 *
	 * @param args , the database directory, OSM dataset and layer
	 *             specifications.
	 */
	public static void main(String[] args) {
		if (args.length < 5) {
			System.out.println("Usage: osmtoshp neo4jHome database exportdir osmdataset layerspec <..layerspecs..>");
			System.out.println(
					"\tNote: 'database' can only be something other than 'neo4j' in Neo4j Enterprise Edition.");
		} else {
			String homeDir = args[0];
			String database = args[1];
			String exportDir = args[2];
			String osmdataset = args[3];
			List<String> layerspecs = new ArrayList<>(Arrays.asList(args).subList(4, args.length));
			DatabaseManagementService databases = new DatabaseManagementServiceBuilder(Path.of(homeDir)).build();
			GraphDatabaseService db = databases.database(database);
			SpatialDatabaseService spatial = new SpatialDatabaseService(
					new IndexManager((GraphDatabaseAPI) db, SecurityContext.AUTH_DISABLED));
			OSMLayer layer;
			try (Transaction tx = db.beginTx()) {
				layer = (OSMLayer) spatial.getLayer(tx, osmdataset);
			}
			if (layer != null) {
				ShapefileExporter exporter = new ShapefileExporter(db);
				exporter.setExportDir(args[1] + File.separator + layer.getName());
				for (String layerspec : layerspecs) {
					String[] fields = layerspec.split("[.\\-]");
					HashMap<String, String> tags = new HashMap<>();
					String key = fields[0];
					String name = key;
					if (fields.length > 1) {
						String value = fields[1];
						name = key + "-" + value;
						tags.put(key, value);
					}

					try (Transaction tx = db.beginTx()) {
						if (layer.getLayerNames(tx).contains(name)) {
							System.out.println("Exporting previously existing layer: " + name);
						} else {
							System.out.println("Creating and exporting new layer: " + name);
							layer.addDynamicLayerOnWayTags(tx, name, Constants.GTYPE_LINESTRING, tags);
						}
						exporter.exportLayer(name);
						tx.commit();
					} catch (Exception e) {
						System.err.println("Failed to export dynamic layer " + name + ": " + e);
						e.printStackTrace(System.err);
					}
				}
			} else {
				System.err.println("No such layer: " + args[2]);
			}
			databases.shutdown();
		}
	}

}
