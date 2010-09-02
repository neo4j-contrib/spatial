package org.neo4j.gis.spatial;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;
import org.neo4j.gis.spatial.osm.OSMDataset;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;

public class TestDynamicLayers extends Neo4jTestCase {

	@Test
	public void testShapefileExport() throws Exception {
		// TODO: Consider merits of using dependency data in target/osm,
		// downloaded by maven, as done in TestSpatial, versus the test data
		// commited to source code as done here
		loadTestOsmData("map.osm", 1000);

		// Define dynamic layers
		ArrayList<Layer> layers = new ArrayList<Layer>();
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		OSMLayer osmLayer = (OSMLayer) spatialService.getLayer("map.osm");
		addSimpleDynamicLayer(osmLayer, layers, "highway", "primary");
		addSimpleDynamicLayer(osmLayer, layers, "highway", "secondary");
		addSimpleDynamicLayer(osmLayer, layers, "highway", "tertiary");
		addSimpleDynamicLayer(osmLayer, layers, "highway", "residential");
		addSimpleDynamicLayer(osmLayer, layers, "highway", "footway");
		addSimpleDynamicLayer(osmLayer, layers, "highway", "cycleway");
		addSimpleDynamicLayer(osmLayer, layers, "highway", "track");
		addSimpleDynamicLayer(osmLayer, layers, "highway", "path");
		addSimpleDynamicLayer(osmLayer, layers, "highway", "unclassified");
		addSimpleDynamicLayer(osmLayer, layers, "amenity", "parking");
		addSimpleDynamicLayer(osmLayer, layers, "railway", null);
		addSimpleDynamicLayer(osmLayer, layers, "highway", null);

		// Now export the layers to files
		ShapefileExporter exporter = new ShapefileExporter(graphDb());
		exporter.setExportDir("target/export");
		for (Layer layer : layers) {
			exporter.exportLayer(layer.getName());
		}
	}

	/**
	 * Add a rule for a pure way based search, with a single property key/value
	 * match on the way tags. All ways with the specified tag property will be
	 * returned.
	 * @param osmLayer
	 * @param layers
	 * @param key
	 * @param value
	 */
	private void addSimpleDynamicLayer(OSMLayer osmLayer, ArrayList<Layer> layers, String key, String value) {
		HashMap<String, String> tags = new HashMap<String, String>();
		tags.put(key, value);
		layers.add(osmLayer.addDynamicLayerOnWayTags(value==null ? key : key + "-" + value, Constants.GTYPE_LINESTRING, tags));
	}

	private void loadTestOsmData(String layerName, int commitInterval) throws Exception {
		String osmPath = layerName;
		System.out.println("\n=== Loading layer " + layerName + " from " + osmPath + " ===");
		reActivateDatabase(false, true, false);
		OSMImporter importer = new OSMImporter(layerName);
		importer.importFile(getBatchInserter(), osmPath);
		reActivateDatabase(false, false, false);
		importer.reIndex(graphDb(), commitInterval);
	}

}
