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

import java.io.IOException;

import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.junit.Test;
import org.neo4j.gis.spatial.geotools.data.Neo4jSpatialDataStore;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;

import com.vividsolutions.jts.geom.Envelope;

public class TestOSMImport extends Neo4jTestCase {

	@Test
	public void testImport_Map1() throws Exception {
		runImport("map.osm");
	}

	@Test
	public void testImport_Map2() throws Exception {
		runImport("map2.osm");
	}

	private void runImport(String osmFile) throws Exception {
		// TODO: Consider merits of using dependency data in target/osm,
		// downloaded by maven, as done in TestSpatial, versus the test data
		// commited to source code as done here
		printDatabaseStats();
		loadTestOsmData(osmFile, 1000);
		checkOSMLayer(osmFile);
		printDatabaseStats();
	}

	private void loadTestOsmData(String layerName, int commitInterval) throws Exception {
		String osmPath = layerName;
		System.out.println("\n=== Loading layer " + layerName + " from " + osmPath + " ===");
		reActivateDatabase(false, true, false);
		OSMImporter importer = new OSMImporter(layerName);
		importer.importFile(getBatchInserter(), osmPath, false);
		reActivateDatabase(false, false, false);
		importer.reIndex(graphDb(), commitInterval);
	}

	private void checkOSMLayer(String layerName) throws IOException {
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		OSMLayer layer = (OSMLayer) spatialService.getOrCreateLayer(layerName, OSMGeometryEncoder.class, OSMLayer.class);
		assertNotNull("OSM Layer index should not be null", layer.getIndex());
		assertNotNull("OSM Layer index envelope should not be null", layer.getIndex().getLayerBoundingBox());
		Envelope bbox = layer.getIndex().getLayerBoundingBox();
		System.out.println("OSM Layer has bounding box: " + bbox);
		//((RTreeIndex)layer.getIndex()).debugIndexTree();
		checkIndexAndFeatureCount(layer);
	}

	private void checkIndexAndFeatureCount(Layer layer) throws IOException {
		if(layer.getIndex().count()<1) {
			System.out.println("Warning: index count zero: "+layer.getName());
		}
		System.out.println("Layer '" + layer.getName() + "' has " + layer.getIndex().count() + " entries in the index");
		DataStore store = new Neo4jSpatialDataStore(graphDb());
		SimpleFeatureCollection features = store.getFeatureSource(layer.getName()).getFeatures();
		System.out.println("Layer '" + layer.getName() + "' has " + features.size() + " features");
		assertEquals("FeatureCollection.size for layer '" + layer.getName() + "' not the same as index count", layer.getIndex()
				.count(), features.size());
	}

}
