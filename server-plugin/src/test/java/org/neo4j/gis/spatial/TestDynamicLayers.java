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
package org.neo4j.gis.spatial;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.geotools.api.data.DataSourceException;
import org.geotools.api.data.DataStore;
import org.geotools.data.neo4j.Neo4jSpatialDataStore;
import org.geotools.data.neo4j.StyledImageExporter;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.NullListener;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class TestDynamicLayers extends Neo4jTestCase implements Constants {

	@Test
	public void testShapefileExport_Map1() throws Exception {
		runShapefileExport("map.osm");
	}

	@Test
	public void testShapefileExport_Map2() throws Exception {
		runShapefileExport("map2.osm");
	}

	@Test
	public void testImageExport_HighwayShp() throws Exception {
		runDynamicShapefile("highway.shp");
	}

	@SuppressWarnings("SameParameterValue")
	private void runDynamicShapefile(String shpFile) throws Exception {
		printDatabaseStats();
		loadTestShpData(shpFile);
		checkLayer(shpFile);
		printDatabaseStats();

		// Define dynamic layers
		ArrayList<Layer> layers = new ArrayList<>();
		try (Transaction tx = graphDb().beginTx()) {
			SpatialDatabaseService spatial = new SpatialDatabaseService(
					new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
			DynamicLayer shpLayer = spatial.asDynamicLayer(tx, spatial.getLayer(tx, shpFile));
			layers.add(shpLayer.addLayerConfig(tx, "CQL0-highway", GTYPE_GEOMETRY, "highway is not null"));
			layers.add(shpLayer.addLayerConfig(tx, "CQL1-highway", GTYPE_POINT,
					"geometryType(the_geom) = 'MultiLineString'"));
			layers.add(shpLayer.addLayerConfig(tx, "CQL2-highway", GTYPE_LINESTRING,
					"highway is not null and geometryType(the_geom) = 'MultiLineString'"));
			layers.add(
					shpLayer.addLayerConfig(tx, "CQL3-residential", GTYPE_MULTILINESTRING, "highway = 'residential'"));
			layers.add(
					shpLayer.addLayerConfig(tx, "CQL4-nameV", GTYPE_LINESTRING, "name is not null and name like 'V%'"));
			layers.add(
					shpLayer.addLayerConfig(tx, "CQL5-nameS", GTYPE_LINESTRING, "name is not null and name like 'S%'"));
			layers.add(shpLayer.addLayerConfig(tx, "CQL6-nameABC", GTYPE_LINESTRING,
					"name like 'A%' or name like 'B%' or name like 'B%'"));
			layers.add(shpLayer.addCQLDynamicLayerOnAttribute(tx, "highway", "residential", GTYPE_MULTILINESTRING));
			layers.add(shpLayer.addCQLDynamicLayerOnAttribute(tx, "highway", "path", GTYPE_MULTILINESTRING));
			layers.add(shpLayer.addCQLDynamicLayerOnAttribute(tx, "highway", "track", GTYPE_MULTILINESTRING));
			assertEquals(layers.size() + 1, shpLayer.getLayerNames(tx).size());
			tx.commit();
		}
		try (Transaction tx = graphDb().beginTx()) {

			// Now export the layers to files
			// First prepare the SHP and PNG exporters
			StyledImageExporter imageExporter = new StyledImageExporter(graphDb());
			imageExporter.setExportDir("target/export/" + shpFile);
			imageExporter.setZoom(3.0);
			imageExporter.setOffset(-0.05, -0.05);
			imageExporter.setSize(1024, 768);

			// Now loop through all dynamic layers and export them to images,
			// where possible. Layers will multiple geometries cannot be exported
			// and we take note of how many times that happens
			for (Layer layer : layers) {
				// for (Layer layer : new Layer[] {}) {
				checkIndexAndFeatureCount(layer);
				imageExporter.saveLayerImage(layer.getName(), null);
			}
			tx.commit();
		}
	}

	private void runShapefileExport(String osmFile) throws Exception {
		// TODO: Consider merits of using dependency data in target/osm,
		// downloaded by maven, as done in TestSpatial, versus the test data
		// commited to source code as done here
		printDatabaseStats();
		loadTestOsmData(osmFile);
		Envelope bbox = checkLayer(osmFile);
		printDatabaseStats();
		//bbox.expandBy(-0.1);
		bbox = scale(bbox, 0.2);

		// Define dynamic layers
		ArrayList<Layer> layers = new ArrayList<>();
		try (Transaction tx = graphDb().beginTx()) {
			SpatialDatabaseService spatial = new SpatialDatabaseService(
					new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
			OSMLayer osmLayer = (OSMLayer) spatial.getLayer(tx, osmFile);
			LinearRing ring = osmLayer.getGeometryFactory().createLinearRing(
					new Coordinate[]{new Coordinate(bbox.getMinX(), bbox.getMinY()),
							new Coordinate(bbox.getMinX(), bbox.getMaxY()),
							new Coordinate(bbox.getMaxX(), bbox.getMaxY()),
							new Coordinate(bbox.getMaxX(), bbox.getMinY()),
							new Coordinate(bbox.getMinX(), bbox.getMinY())});
			Polygon polygon = osmLayer.getGeometryFactory().createPolygon(ring, null);
			layers.add(osmLayer.addLayerConfig(tx, "CQL1-highway", GTYPE_LINESTRING,
					"highway is not null and geometryType(the_geom) = 'LineString'"));
			layers.add(osmLayer.addLayerConfig(tx, "CQL2-residential", GTYPE_LINESTRING,
					"highway = 'residential' and geometryType(the_geom) = 'LineString'"));
			layers.add(osmLayer.addLayerConfig(tx, "CQL3-natural", GTYPE_POLYGON,
					"natural is not null and geometryType(the_geom) = 'Polygon'"));
			layers.add(osmLayer.addLayerConfig(tx, "CQL4-water", GTYPE_POLYGON,
					"natural = 'water' and geometryType(the_geom) = 'Polygon'"));
			layers.add(osmLayer.addLayerConfig(tx, "CQL5-bbox", GTYPE_GEOMETRY,
					"BBOX(the_geom, " + toCoordinateText(bbox) + ")"));
			layers.add(osmLayer.addLayerConfig(tx, "CQL6-bbox-polygon", GTYPE_GEOMETRY, "within(the_geom, POLYGON(("
					+ toCoordinateText(polygon.getCoordinates()) + ")))"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "highway", "primary"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "highway", "secondary"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "highway", "tertiary"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, GTYPE_LINESTRING, "highway=*"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, GTYPE_LINESTRING, "highway=footway, bicycle=yes"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "highway=*, bicycle=yes"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "highway", "residential"));
			layers.add(osmLayer.addCQLDynamicLayerOnAttribute(tx, "highway", "residential", GTYPE_LINESTRING));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "highway", "footway"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "highway", "cycleway"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "highway", "track"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "highway", "path"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "highway", "unclassified"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "amenity", "parking", GTYPE_POLYGON));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "railway", null));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "highway", null));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "waterway", null));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "building", null, GTYPE_POLYGON));
			layers.add(osmLayer.addCQLDynamicLayerOnAttribute(tx, "building", null, GTYPE_POLYGON));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "natural", null, GTYPE_GEOMETRY));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "natural", "water", GTYPE_POLYGON));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "natural", "wood", GTYPE_POLYGON));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "natural", "coastline"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, "natural", "beach"));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, GTYPE_POLYGON));
			layers.add(osmLayer.addSimpleDynamicLayer(tx, GTYPE_POINT));
			layers.add(osmLayer.addCQLDynamicLayerOnGeometryType(tx, GTYPE_POLYGON));
			layers.add(osmLayer.addCQLDynamicLayerOnGeometryType(tx, GTYPE_POINT));
			assertEquals(layers.size() + 1, osmLayer.getLayerNames(tx).size());
			tx.commit();
		}
		// Now export the layers to files
		// First prepare the SHP and PNG exporters
		ShapefileExporter shpExporter = new ShapefileExporter(graphDb());
		shpExporter.setExportDir("target/export/" + osmFile);
		StyledImageExporter imageExporter = new StyledImageExporter(graphDb());
		imageExporter.setExportDir("target/export/" + osmFile);
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
			Integer geometryType;
			try (Transaction tx = graphDb().beginTx()) {
				geometryType = layer.getGeometryType(tx);
				if (layer.getGeometryType(tx) == GTYPE_GEOMETRY) {
					countMultiGeometryLayers++;
				}
				tx.commit();
			}
			checkIndexAndFeatureCount(layer);
			try {
				imageExporter.saveLayerImage(layer.getName(), null);
				shpExporter.exportLayer(layer.getName());
			} catch (Exception e) {
				if (e instanceof DataSourceException && e.getMessage().contains("geom.Geometry")) {
					System.out.println("Got geometry exception on layer with geometry["
							+ SpatialDatabaseService.convertGeometryTypeToName(geometryType) + "]: "
							+ e.getMessage());
					countMultiGeometryExceptions++;
				} else {
					throw e;
				}
			}
		}
		assertEquals(countMultiGeometryLayers, countMultiGeometryExceptions,
				"Mismatching number of data source exceptions and raw geometry layers");
	}

	@SuppressWarnings("SameParameterValue")
	private static Envelope scale(Envelope bbox, double fraction) {
		double xoff = bbox.getWidth(0) * (1.0 - fraction) / 2.0;
		double yoff = bbox.getWidth(1) * (1.0 - fraction) / 2.0;
		return new Envelope(bbox.getMinX() + xoff, bbox.getMaxX() - xoff, bbox.getMinY() + yoff, bbox.getMaxY() - yoff);
	}

	private static String toCoordinateText(Coordinate[] coordinates) {
		StringBuilder sb = new StringBuilder();
		for (Coordinate c : coordinates) {
			if (sb.length() > 0) {
				sb.append(", ");
			}
			sb.append(c.x).append(" ").append(c.y);
		}
		return sb.toString();
	}

	private static String toCoordinateText(Envelope bbox) {
		return bbox.getMinX() + ", " + bbox.getMinY() + ", " + bbox.getMaxX() + ", " + bbox.getMaxY();
	}

	private void checkIndexAndFeatureCount(Layer layer) throws IOException {
		try (Transaction tx = graphDb().beginTx()) {
			if (layer.getIndex().count(tx) < 1) {
				System.out.println("Warning: index count zero: " + layer.getName());
			}
			System.out.println(
					"Layer '" + layer.getName() + "' has " + layer.getIndex().count(tx) + " entries in the index");
			tx.commit();
		}
		DataStore store = new Neo4jSpatialDataStore(graphDb());
		try (Transaction tx = graphDb().beginTx()) {
			SimpleFeatureCollection features = store.getFeatureSource(layer.getName()).getFeatures();
			System.out.println("Layer '" + layer.getName() + "' has " + features.size() + " features");
			assertEquals(layer.getIndex().count(tx), features.size(),
					"FeatureCollection.size for layer '" + layer.getName() + "' not the same as index count");
			tx.commit();
		}
	}

	private void loadTestOsmData(String layerName) throws Exception {
		System.out.println("\n=== Loading layer " + layerName + " from " + layerName + " ===");
		OSMImporter importer = new OSMImporter(layerName);
		importer.setCharset(StandardCharsets.UTF_8);
		importer.importFile(graphDb(), layerName, 1000);
		importer.reIndex(graphDb(), 1000);
	}

	private void loadTestShpData(String layerName) throws IOException {
		String shpPath = "shp" + File.separator + layerName;
		System.out.println("\n=== Loading layer " + layerName + " from " + shpPath + " ===");
		ShapefileImporter importer = new ShapefileImporter(graphDb(), new NullListener(), 1000);
		importer.importFile(shpPath, layerName, StandardCharsets.UTF_8);
	}

	private Envelope checkLayer(String layerName) {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		Layer layer;
		try (Transaction tx = graphDb().beginTx()) {
			layer = spatial.getLayer(tx, layerName);
		}
		assertNotNull(layer.getIndex(), "Layer index should not be null");
		Envelope bbox;
		try (Transaction tx = graphDb().beginTx()) {
			bbox = layer.getIndex().getBoundingBox(tx);
		}
		assertNotNull(bbox, "Layer index envelope should not be null");
		System.out.println("Layer has bounding box: " + bbox);
		Neo4jTestUtils.debugIndexTree(graphDb(), layerName);
		return bbox;
	}
}
