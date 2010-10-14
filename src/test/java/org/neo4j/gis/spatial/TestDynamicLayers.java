package org.neo4j.gis.spatial;

import java.util.ArrayList;

import org.geotools.data.DataSourceException;
import org.junit.Test;
import org.neo4j.gis.spatial.geotools.data.StyledImageExporter;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;

public class TestDynamicLayers extends Neo4jTestCase {

	@Test
	public void testShapefileExport() throws Exception {
		// TODO: Consider merits of using dependency data in target/osm,
		// downloaded by maven, as done in TestSpatial, versus the test data
		// commited to source code as done here
		printDatabaseStats();
		loadTestOsmData("map2.osm", 1000);
		printDatabaseStats();

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
		layers.add(osmLayer.addSimpleDynamicLayer("natural", null, Constants.GTYPE_GEOMETRY));
		layers.add(osmLayer.addSimpleDynamicLayer("natural", "water", Constants.GTYPE_POLYGON));
		layers.add(osmLayer.addSimpleDynamicLayer("natural", "wood", Constants.GTYPE_POLYGON));
		layers.add(osmLayer.addSimpleDynamicLayer("natural", "coastline"));
		layers.add(osmLayer.addSimpleDynamicLayer("natural", "beach"));
		layers.add(osmLayer.addSimpleDynamicLayer(Constants.GTYPE_POLYGON));
		assertEquals(layers.size() + 1, osmLayer.getLayerNames().size());

		// Now export the layers to files
		// First prepare the SHP and PNG exporters
		ShapefileExporter shpExporter = new ShapefileExporter(graphDb());
		shpExporter.setExportDir("target/export");
		StyledImageExporter imageExporter = new StyledImageExporter(graphDb());
		imageExporter.setExportDir("target/export");
		imageExporter.setZoom(3.0);
		imageExporter.setOffset(-0.05, -0.05);
		imageExporter.setSize(1024, 768);
		imageExporter.saveLayerImage(osmLayer.getName(), "neo.sld.xml");

		// Now loop through all dynamic layers and export them to shapefiles,
		// where possible. Layers will multiple geometries cannot be exported
		// and we take note of how many times that happens
		int countMultiGeometryLayers = 0;
		int countMultiGeometryExceptions = 0;
		for (Layer layer : layers) {
			if (layer.getGeometryType() == Constants.GTYPE_GEOMETRY) {
				countMultiGeometryLayers++;
			}
			try {
	        	imageExporter.saveLayerImage(layer.getName(), "neo.sld.xml");
				shpExporter.exportLayer(layer.getName());
			} catch (Exception e) {
				if (e instanceof DataSourceException && e.getMessage().contains("geom.Geometry")) {
					System.out.println("Got geometry exception on layer with geometry["
					        + SpatialDatabaseService.convertGeometryTypeToName(layer.getGeometryType()) + "]: " + e.getMessage());
					countMultiGeometryExceptions++;
				} else {
					throw e;
				}
			}
		}
		assertEquals("Mismatching number of data source exceptions and raw geometry layers", countMultiGeometryLayers,
		        countMultiGeometryExceptions);
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
