package org.neo4j.gis.spatial;

import java.io.IOException;
import java.util.ArrayList;

import org.geotools.data.DataSourceException;
import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.junit.Test;
import org.neo4j.gis.spatial.geotools.data.Neo4jSpatialDataStore;
import org.neo4j.gis.spatial.geotools.data.StyledImageExporter;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;

import com.vividsolutions.jts.geom.Envelope;

public class TestDynamicLayers extends Neo4jTestCase {

	@Test
	public void testShapefileExport_Map1() throws Exception {
		runShapefileExport("map.osm");
	}

	@Test
	public void testShapefileExport_Map2() throws Exception {
		runShapefileExport("map2.osm");
	}

	private void runShapefileExport(String osmFile) throws Exception {
		// TODO: Consider merits of using dependency data in target/osm,
		// downloaded by maven, as done in TestSpatial, versus the test data
		// commited to source code as done here
		printDatabaseStats();
		loadTestOsmData(osmFile, 1000);
		checkOSMLayer(osmFile);
		printDatabaseStats();

		// Define dynamic layers
		ArrayList<Layer> layers = new ArrayList<Layer>();
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		OSMLayer osmLayer = (OSMLayer) spatialService.getLayer(osmFile);
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
		layers.add(osmLayer.addSimpleDynamicLayer(Constants.GTYPE_POINT));
		assertEquals(layers.size() + 1, osmLayer.getLayerNames().size());

		// Now export the layers to files
		// First prepare the SHP and PNG exporters
		ShapefileExporter shpExporter = new ShapefileExporter(graphDb());
		shpExporter.setExportDir("target/export/"+osmFile);
		StyledImageExporter imageExporter = new StyledImageExporter(graphDb());
		imageExporter.setExportDir("target/export/"+osmFile);
		imageExporter.setZoom(3.0);
		imageExporter.setOffset(-0.05, -0.05);
		imageExporter.setSize(1024, 768);
		// imageExporter.saveLayerImage("highway", null);
		// imageExporter.saveLayerImage(osmLayer.getName(), "neo.sld.xml");

		// Now loop through all dynamic layers and export them to shapefiles,
		// where possible. Layers will multiple geometries cannot be exported
		// and we take note of how many times that happens
		int countMultiGeometryLayers = 0;
		int countMultiGeometryExceptions = 0;
		for (Layer layer : layers) {
		//for (Layer layer : new Layer[] {}) {
			if (layer.getGeometryType() == Constants.GTYPE_GEOMETRY) {
				countMultiGeometryLayers++;
			}
			checkIndexAndFeatureCount(layer);
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

	private void loadTestOsmData(String layerName, int commitInterval) throws Exception {
		String osmPath = layerName;
		System.out.println("\n=== Loading layer " + layerName + " from " + osmPath + " ===");
		reActivateDatabase(false, true, false);
		OSMImporter importer = new OSMImporter(layerName);
		importer.importFile(getBatchInserter(), osmPath, false);
		reActivateDatabase(false, false, false);
		importer.reIndex(graphDb(), commitInterval);
	}

	private void checkOSMLayer(String layerName) {
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		OSMLayer layer = (OSMLayer) spatialService.getOrCreateLayer(layerName, OSMGeometryEncoder.class, OSMLayer.class);
		assertNotNull("OSM Layer index should not be null", layer.getIndex());
		assertNotNull("OSM Layer index envelope should not be null", layer.getIndex().getLayerBoundingBox());
		Envelope bbox = layer.getIndex().getLayerBoundingBox();
		System.out.println("OSM Layer has bounding box: " + bbox);
		((RTreeIndex)layer.getIndex()).debugIndexTree();
	}
}
