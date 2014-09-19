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
import java.io.Serializable;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.FeatureStore;
import org.geotools.data.neo4j.Neo4jSpatialDataStore;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class ShapefileExporter {
	Neo4jSpatialDataStore neo4jDataStore;
	File exportDir;

	public ShapefileExporter(GraphDatabaseService db) {
		neo4jDataStore = new Neo4jSpatialDataStore(db);
		exportDir = null;
	}

	public void setExportDir(String dir) {
		exportDir = (dir == null || dir.length() == 0) ? null : (new File(dir)).getAbsoluteFile();
	}

	public File exportLayer(String layerName) throws Exception {
		String fileName = layerName.replace(" ", "-");
		return exportLayer(layerName, fileName + ".shp");
	}

	public File exportLayer(String layerName, String fileName) throws Exception {
		return exportLayer(layerName, new File(fileName));
	}

	private File checkFile(File file) {
		if (!file.isAbsolute() && exportDir != null) {
			file = new File(exportDir, file.getPath());
		}
		file = file.getAbsoluteFile();
		file.getParentFile().mkdirs();
		if (file.exists()) {
			System.out.println("Deleting previous file: " + file);
			file.delete();
		}
		return file;
	}

	public File exportLayer(String layerName, File file) throws Exception {
        file = checkFile(file);
        ShapefileDataStoreFactory factory = new ShapefileDataStoreFactory();
        Map<String, Serializable> create = new HashMap<String, Serializable>();
        URL url = file.toURI().toURL();
        create.put("url", url);
        create.put("create spatial index", Boolean.TRUE);
        create.put("charset", "UTF-8");
        ShapefileDataStore shpDataStore = (ShapefileDataStore) factory.createNewDataStore(create);
        CoordinateReferenceSystem crs = null;
		try (Transaction tx = neo4jDataStore.beginTx()) {
			SimpleFeatureType featureType = neo4jDataStore.getSchema(layerName);
            GeometryDescriptor geometryType = featureType.getGeometryDescriptor();
            crs = geometryType.getCoordinateReferenceSystem();
            // crs = neo4jDataStore.getFeatureSource(layerName).getInfo().getCRS();

            shpDataStore.createSchema(featureType);
            FeatureStore store = (FeatureStore) shpDataStore.getFeatureSource();
            store.addFeatures(neo4jDataStore.getFeatureSource(layerName).getFeatures());
            tx.success();
		}
        if (crs != null)
            shpDataStore.forceSchemaCRS(crs);
        if (!file.exists()) {
            throw new Exception("Shapefile was not created: " + file);
        } else if (file.length() < 10) {
            throw new Exception("Shapefile was unexpectedly small, only " + file.length() + " bytes: " + file);
        }
        return file;
	}
}
