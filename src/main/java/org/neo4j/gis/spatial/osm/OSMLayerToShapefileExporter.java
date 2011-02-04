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
package org.neo4j.gis.spatial.osm;

import java.io.File;
import java.util.HashMap;

import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.ShapefileExporter;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class OSMLayerToShapefileExporter {
	/**
	 * This method allows for a console, command-line application for loading
	 * accessing an existing database containing an existing OSM model, and
	 * exporting one or more dynamic layers to shapefiles. The layer
	 * specifications are key.value pairs (dot separated). If the value is left
	 * out, all values are accepted (test for existance of key only).
	 * 
	 * @param args
	 *            , the database directory, OSM dataset and layer
	 *            specifications.
	 */
	public static void main(String[] args) {
		if (args.length < 4) {
			System.out.println("Usage: osmtoshp databasedir exportdir osmdataset layerspec <..layerspecs..>");
		} else {
			GraphDatabaseService db = new EmbeddedGraphDatabase((new File(args[0])).getAbsolutePath());
			SpatialDatabaseService spatial = new SpatialDatabaseService(db);
			OSMLayer layer = (OSMLayer) spatial.getLayer(args[2]);
			if (layer != null) {
				ShapefileExporter exporter = new ShapefileExporter(db);
				exporter.setExportDir(args[1]+File.separator+layer.getName());
				for (int i = 3; i < args.length; i++) {
					String[] fields = args[i].split("[\\.\\-]");
					HashMap<String, String> tags = new HashMap<String, String>();
					String key = fields[0];
					String name = key;
					if (fields.length > 1) {
						String value = fields.length > 1 ? fields[1] : null;
						name = key + "-" + value;
						tags.put(key, value);
					}

					try {
						if (layer.getLayerNames().contains(name)) {
							System.out.println("Exporting previously existing layer: "+name);
							exporter.exportLayer(name);
						} else {
							System.out.println("Creating and exporting new layer: "+name);
							layer.addDynamicLayerOnWayTags(name, Constants.GTYPE_LINESTRING, tags);
							exporter.exportLayer(name);
						}
					} catch (Exception e) {
						System.err.println("Failed to export dynamic layer " + name + ": " + e);
						e.printStackTrace(System.err);
					}
				}
			} else {
				System.err.println("No such layer: " + args[2]);
			}
			db.shutdown();
		}
	}

}
