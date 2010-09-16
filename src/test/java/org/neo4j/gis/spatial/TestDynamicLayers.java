package org.neo4j.gis.spatial;

import java.util.ArrayList;

import org.junit.Test;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;

public class TestDynamicLayers extends Neo4jTestCase {

	@Test
	public void testShapefileExport() throws Exception {
		// TODO: Consider merits of using dependency data in target/osm,
		// downloaded by maven, as done in TestSpatial, versus the test data
		// commited to source code as done here
		loadTestOsmData("map2.osm", 1000);

		// Define dynamic layers
		ArrayList<Layer> layers = new ArrayList<Layer>();
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		OSMLayer osmLayer = (OSMLayer) spatialService.getLayer("map2.osm");
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "primary"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "secondary"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "tertiary"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "residential"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "footway"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "cycleway"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "track"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "path"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "unclassified"));
		layers.add(osmLayer.addSimpleDynamicLayer("amenity", "parking", Constants.GTYPE_POLYGON));
		layers.add(osmLayer.addSimpleDynamicLayer("railway", null));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", null));
		layers.add(osmLayer.addSimpleDynamicLayer("waterway", null));
		layers.add(osmLayer.addSimpleDynamicLayer("building", null, Constants.GTYPE_POLYGON));
		assertEquals(layers.size() + 1, osmLayer.getLayerNames().size());

		// Now export the layers to files
		ShapefileExporter exporter = new ShapefileExporter(graphDb());
		exporter.setExportDir("target/export");
		for (Layer layer : layers) {
			exporter.exportLayer(layer.getName());
		}
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
