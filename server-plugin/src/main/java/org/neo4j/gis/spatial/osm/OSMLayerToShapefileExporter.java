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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.geotools.data.neo4j.Neo4jSpatialDataStore;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.gis.spatial.ShapefileExporter;

public class OSMLayerToShapefileExporter {

	private static final Logger LOGGER = Logger.getLogger(OSMLayerToShapefileExporter.class.getName());

	/**
	 * The entry point of the application. This method is responsible for exporting specific OpenStreetMap (OSM) layers
	 * from a Neo4j spatial database into Shapefiles. It establishes a connection to the Neo4j database, validates
	 * the existence of the required dataset, and handles the export of specified layers.
	 *
	 * @param args the command-line arguments for the application:
	 *             args[0] - The Bolt URI for connecting to the Neo4j database.
	 *             args[1] - The name of the database within Neo4j to connect to.
	 *             args[2] - The username for authenticating with the Neo4j database.
	 *             args[3] - The password for authenticating with the Neo4j database.
	 *             args[4] - The name of the OSM dataset layer to be exported.
	 *             args[5..n] - Specifications for the layers to export, in the format <key>.<value> or <key>.
	 * @throws Exception if an error occurs during the execution of the method.
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 6) {
			LOGGER.warning("Usage: osmtoshp <bolturi> <database> <user> <password> <osmdataset> <layerspecs..>");
		} else {
			String bolturi = args[0];
			String database = args[1];
			String user = args[2];
			String password = args[3];
			String osmdataset = args[4];
			List<String> layerspecs = new ArrayList<>(Arrays.asList(args).subList(5, args.length));

			var driver = GraphDatabase.driver(bolturi, AuthTokens.basic(user, password));
			var dataStore = new Neo4jSpatialDataStore(driver, database);

			var layers = Arrays.asList(dataStore.getTypeNames());
			if (!layers.contains(osmdataset)) {
				LOGGER.info("No layer " + osmdataset + " found for database " + database);
				return;
			}
			ShapefileExporter exporter = new ShapefileExporter(dataStore);
			exporter.setExportDir(args[1] + File.separator + osmdataset);
			for (String layerspec : layerspecs) {
				String[] fields = layerspec.split("[.\\-]");
				String key = fields[0];
				String name = key;
				if (fields.length > 1) {
					String value = fields[1];
					name = key + "-" + value;
				}

				if (layers.contains(name)) {
					LOGGER.info("Exporting existing layer: " + name);
					exporter.exportLayer(name);
				}
			}
		}
	}

}
