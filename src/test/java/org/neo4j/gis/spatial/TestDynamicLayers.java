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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

import org.geotools.data.DataSourceException;
import org.geotools.data.DataStore;
import org.geotools.data.neo4j.Neo4jSpatialDataStore;
import org.geotools.data.neo4j.StyledImageExporter;
import org.geotools.data.shapefile.shp.ShapefileException;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.junit.Test;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.NullListener;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Exceptions;

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

	private void runDynamicShapefile(String shpFile) throws Exception {
		printDatabaseStats();
		loadTestShpData(shpFile, 1000);
		checkLayer(shpFile);
		printDatabaseStats();

		// Define dynamic layers
		ArrayList<Layer> layers = new ArrayList<Layer>();
        try (Transaction tx = graphDb().beginTx()) {

            SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
            DynamicLayer shpLayer = spatialService.asDynamicLayer(spatialService.getLayer(shpFile));
            layers.add(shpLayer.addLayerConfig("CQL0-highway", GTYPE_GEOMETRY, "highway is not null"));
            layers.add(shpLayer.addLayerConfig("CQL1-highway", GTYPE_POINT, "geometryType(the_geom) = 'MultiLineString'"));
            layers.add(shpLayer.addLayerConfig("CQL2-highway", GTYPE_LINESTRING, "highway is not null and geometryType(the_geom) = 'MultiLineString'"));
            layers.add(shpLayer.addLayerConfig("CQL3-residential", GTYPE_MULTILINESTRING, "highway = 'residential'"));
            layers.add(shpLayer.addLayerConfig("CQL4-nameV", GTYPE_LINESTRING, "name is not null and name like 'V%'"));
            layers.add(shpLayer.addLayerConfig("CQL5-nameS", GTYPE_LINESTRING, "name is not null and name like 'S%'"));
            layers.add(shpLayer.addLayerConfig("CQL6-nameABC", GTYPE_LINESTRING, "name like 'A%' or name like 'B%' or name like 'B%'"));
            layers.add(shpLayer.addCQLDynamicLayerOnAttribute("highway", "residential", GTYPE_MULTILINESTRING));
            layers.add(shpLayer.addCQLDynamicLayerOnAttribute("highway", "path", GTYPE_MULTILINESTRING));
            layers.add(shpLayer.addCQLDynamicLayerOnAttribute("highway", "track", GTYPE_MULTILINESTRING));
            assertEquals(layers.size() + 1, shpLayer.getLayerNames().size());
            tx.success();
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
            tx.success();
        }
	}

	private void runShapefileExport(String osmFile) throws Exception {
		// TODO: Consider merits of using dependency data in target/osm,
		// downloaded by maven, as done in TestSpatial, versus the test data
		// commited to source code as done here
		printDatabaseStats();
		loadTestOsmData(osmFile, 1000);
		Envelope bbox = checkLayer(osmFile);
		printDatabaseStats();
		//bbox.expandBy(-0.1);
		bbox = scale(bbox, 0.2);

        // Define dynamic layers
        ArrayList<Layer> layers = new ArrayList<Layer>();
        try (Transaction tx = graphDb().beginTx()) {
            SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
            OSMLayer osmLayer = (OSMLayer) spatialService.getLayer(osmFile);
            LinearRing ring = osmLayer.getGeometryFactory().createLinearRing(
                    new Coordinate[] { new Coordinate(bbox.getMinX(), bbox.getMinY()), new Coordinate(bbox.getMinX(), bbox.getMaxY()),
                            new Coordinate(bbox.getMaxX(), bbox.getMaxY()), new Coordinate(bbox.getMaxX(), bbox.getMinY()),
                            new Coordinate(bbox.getMinX(), bbox.getMinY()) });
            Polygon polygon = osmLayer.getGeometryFactory().createPolygon(ring, null);
            layers.add(osmLayer.addLayerConfig("CQL1-highway", GTYPE_LINESTRING, "highway is not null and geometryType(the_geom) = 'LineString'"));
            layers.add(osmLayer.addLayerConfig("CQL2-residential", GTYPE_LINESTRING, "highway = 'residential' and geometryType(the_geom) = 'LineString'"));
            layers.add(osmLayer.addLayerConfig("CQL3-natural", GTYPE_POLYGON, "natural is not null and geometryType(the_geom) = 'Polygon'"));
            layers.add(osmLayer.addLayerConfig("CQL4-water", GTYPE_POLYGON, "natural = 'water' and geometryType(the_geom) = 'Polygon'"));
            layers.add(osmLayer.addLayerConfig("CQL5-bbox", GTYPE_GEOMETRY, "BBOX(the_geom, " + toCoordinateText(bbox) + ")"));
            layers.add(osmLayer.addLayerConfig("CQL6-bbox-polygon", GTYPE_GEOMETRY, "within(the_geom, POLYGON(("
                    + toCoordinateText(polygon.getCoordinates()) + ")))"));
            layers.add(osmLayer.addSimpleDynamicLayer("highway", "primary"));
            layers.add(osmLayer.addSimpleDynamicLayer("highway", "secondary"));
            layers.add(osmLayer.addSimpleDynamicLayer("highway", "tertiary"));
            layers.add(osmLayer.addSimpleDynamicLayer(GTYPE_LINESTRING, "highway=*"));
            layers.add(osmLayer.addSimpleDynamicLayer(GTYPE_LINESTRING, "highway=footway, bicycle=yes"));
            layers.add(osmLayer.addSimpleDynamicLayer("highway=*, bicycle=yes"));
            layers.add(osmLayer.addSimpleDynamicLayer("highway", "residential"));
            layers.add(osmLayer.addCQLDynamicLayerOnAttribute("highway", "residential", GTYPE_LINESTRING));
            layers.add(osmLayer.addSimpleDynamicLayer("highway", "footway"));
            layers.add(osmLayer.addSimpleDynamicLayer("highway", "cycleway"));
            layers.add(osmLayer.addSimpleDynamicLayer("highway", "track"));
            layers.add(osmLayer.addSimpleDynamicLayer("highway", "path"));
            layers.add(osmLayer.addSimpleDynamicLayer("highway", "unclassified"));
            layers.add(osmLayer.addSimpleDynamicLayer("amenity", "parking", GTYPE_POLYGON));
            layers.add(osmLayer.addSimpleDynamicLayer("railway", null));
            layers.add(osmLayer.addSimpleDynamicLayer("highway", null));
            layers.add(osmLayer.addSimpleDynamicLayer("waterway", null));
            layers.add(osmLayer.addSimpleDynamicLayer("building", null, GTYPE_POLYGON));
            layers.add(osmLayer.addCQLDynamicLayerOnAttribute("building", null, GTYPE_POLYGON));
            layers.add(osmLayer.addSimpleDynamicLayer("natural", null, GTYPE_GEOMETRY));
            layers.add(osmLayer.addSimpleDynamicLayer("natural", "water", GTYPE_POLYGON));
            layers.add(osmLayer.addSimpleDynamicLayer("natural", "wood", GTYPE_POLYGON));
            layers.add(osmLayer.addSimpleDynamicLayer("natural", "coastline"));
            layers.add(osmLayer.addSimpleDynamicLayer("natural", "beach"));
            layers.add(osmLayer.addSimpleDynamicLayer(GTYPE_POLYGON));
            layers.add(osmLayer.addSimpleDynamicLayer(GTYPE_POINT));
            layers.add(osmLayer.addCQLDynamicLayerOnGeometryType(GTYPE_POLYGON));
            layers.add(osmLayer.addCQLDynamicLayerOnGeometryType(GTYPE_POINT));
            assertEquals(layers.size() + 1, osmLayer.getLayerNames().size());
            tx.success();
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
			// for (Layer layer : new Layer[] {}) {
			if (layer.getGeometryType() == GTYPE_GEOMETRY) {
				countMultiGeometryLayers++;
			}
			checkIndexAndFeatureCount(layer);
			try {
				imageExporter.saveLayerImage(layer.getName(), null);
				shpExporter.exportLayer(layer.getName());
			} catch (Exception e) {
				if (e instanceof DataSourceException && e.getMessage().contains("geom.Geometry")) {
					System.out.println("Got geometry exception on layer with geometry["
							+ SpatialDatabaseService.convertGeometryTypeToName(layer.getGeometryType()) + "]: "
							+ e.getMessage());
					countMultiGeometryExceptions++;
				} else {
					throw e;
				}
			}
		}
		assertEquals("Mismatching number of data source exceptions and raw geometry layers", countMultiGeometryLayers,
				countMultiGeometryExceptions);
	}

	private Envelope scale(Envelope bbox, double fraction) {
		double xoff = bbox.getWidth() * (1.0 - fraction) / 2.0;
		double yoff = bbox.getHeight() * (1.0 - fraction)/ 2.0;
		return new Envelope(bbox.getMinX() + xoff, bbox.getMaxX() - xoff, bbox.getMinY() + yoff, bbox.getMaxY() - yoff);
	}

	private String toCoordinateText(Coordinate[] coordinates) {
		StringBuffer sb = new StringBuffer();
		for (Coordinate c : coordinates) {
			if (sb.length() > 0)
				sb.append(", ");
			sb.append(c.x).append(" ").append(c.y);
		}
		return sb.toString();
	}

	private String toCoordinateText(Envelope bbox) {
		return "" + bbox.getMinX() + ", " + bbox.getMinY() + ", " + bbox.getMaxX() + ", " + bbox.getMaxY();
	}

	private void checkIndexAndFeatureCount(Layer layer) throws IOException {
		if (layer.getIndex().count() < 1) {
			System.out.println("Warning: index count zero: " + layer.getName());
		}
		System.out.println("Layer '" + layer.getName() + "' has " + layer.getIndex().count() + " entries in the index");
		DataStore store = new Neo4jSpatialDataStore(graphDb());
		try (Transaction tx = graphDb().beginTx()) {
			SimpleFeatureCollection features = store.getFeatureSource(layer.getName()).getFeatures();
			System.out.println("Layer '" + layer.getName() + "' has " + features.size() + " features");
			assertEquals("FeatureCollection.size for layer '" + layer.getName() + "' not the same as index count",
					layer.getIndex().count(), features.size());
			tx.success();
		}
	}

	private void loadTestOsmData(String layerName, int commitInterval) throws Exception {
		String osmPath = layerName;
		System.out.println("\n=== Loading layer " + layerName + " from " + osmPath + " ===");
		reActivateDatabase(false, true, false);
		OSMImporter importer = new OSMImporter(layerName);
		importer.setCharset(Charset.forName("UTF-8"));
		importer.importFile(getBatchInserter(), osmPath, false);
		reActivateDatabase(false, false, false);
		importer.reIndex(graphDb(), commitInterval);
	}

	private void loadTestShpData(String layerName, int commitInterval) throws ShapefileException, FileNotFoundException,
			IOException {
		String shpPath = "shp" + File.separator + layerName;
		System.out.println("\n=== Loading layer " + layerName + " from " + shpPath + " ===");
		ShapefileImporter importer = new ShapefileImporter(graphDb(), new NullListener(), commitInterval);
		importer.importFile(shpPath, layerName, Charset.forName("UTF-8"));
	}

	private Envelope checkLayer(String layerName) {
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		Layer layer = spatialService.getLayer(layerName);
		assertNotNull("Layer index should not be null", layer.getIndex());
		assertNotNull("Layer index envelope should not be null", layer.getIndex().getBoundingBox());
		Envelope bbox = layer.getIndex().getBoundingBox();
		System.out.println("Layer has bounding box: " + bbox);
		debugIndexTree((RTreeIndex) layer.getIndex());
		return bbox;
	}

}
