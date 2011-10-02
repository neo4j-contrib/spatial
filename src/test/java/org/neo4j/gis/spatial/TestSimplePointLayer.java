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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import junit.framework.AssertionFailedError;

import org.junit.Test;
import org.neo4j.collections.rtree.Envelope;
import org.neo4j.gis.spatial.geotools.data.StyledImageExporter;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.Point;


public class TestSimplePointLayer extends Neo4jTestCase {

	@Test
	public void testSimplePointLayer() {
		SpatialDatabaseService db = new SpatialDatabaseService(graphDb());
		EditableLayer layer = (EditableLayer) db.createSimplePointLayer("test", "Longitude", "Latitude");
		assertNotNull(layer);
		SpatialRecord record = layer.add(layer.getGeometryFactory().createPoint(new Coordinate(15.3, 56.2)));
		assertNotNull(record);
		
		// finds geometries that contain the given geometry
		List<SpatialDatabaseRecord> results = GeoPipeline
			.startContainSearch(layer, layer.getGeometryFactory().toGeometry(new com.vividsolutions.jts.geom.Envelope(15.0, 16.0, 56.0, 57.0)))
			.toSpatialDatabaseRecordList();
		
		// should not be contained
		assertEquals(0, results.size());
		
		results = GeoPipeline
			.startWithinSearch(layer, layer.getGeometryFactory().toGeometry(new com.vividsolutions.jts.geom.Envelope(15.0, 16.0, 56.0, 57.0)))
			.toSpatialDatabaseRecordList();
		
		assertEquals(1, results.size());
	}

	@Test
	public void testNeoTextLayer() {
		SpatialDatabaseService db = new SpatialDatabaseService(graphDb());
		SimplePointLayer layer = db.createSimplePointLayer("neo-text");
		assertNotNull(layer);
		for (Coordinate coordinate : makeCoordinateDataFromTextFile("NEO4J-SPATIAL.txt")) {
			SpatialRecord record = layer.add(coordinate);
			assertNotNull(record);
		}
		saveLayerAsImage(layer, 700, 70);

		Envelope bbox = layer.getIndex().getBoundingBox();
		double[] centre = bbox.centre();
		
		List<GeoPipeFlow> results = GeoPipeline
			.startNearestNeighborLatLonSearch(layer, new Coordinate(centre[0] + 0.1, centre[1]), 10.0)
			.sort("OrthodromicDistance").toList();

		saveResultsAsImage(results, "temporary-results-layer-" + layer.getName(), 130, 70);
		assertEquals(71, results.size());
		checkPointOrder(results);

		results = GeoPipeline
			.startNearestNeighborLatLonSearch(layer, new Coordinate(centre[0] + 0.1, centre[1]), 5.0)
			.sort("OrthodromicDistance").toList();

		saveResultsAsImage(results, "temporary-results-layer2-" + layer.getName(), 130, 70);
		assertEquals(30, results.size());
		checkPointOrder(results);
	}

	private void checkPointOrder(List<GeoPipeFlow> results) {
		for (int i = 0; i < results.size() - 1; i++) {
			GeoPipeFlow first = results.get(i);
			GeoPipeFlow second = results.get(i + 1);
			double d1 = (Double) first.getProperties().get("OrthodromicDistance");
			double d2 = (Double) second.getProperties().get("OrthodromicDistance");
			assertTrue("Point at position " + i + " (d=" + d1 + ") must be closer than point at position " + (i + 1) + " (d=" + d2
					+ ")", d1 <= d2);
		}
	}

	@Test
	public void testDensePointLayer() {
		SpatialDatabaseService db = new SpatialDatabaseService(graphDb());
		SimplePointLayer layer = db.createSimplePointLayer("neo-dense", "lon", "lat");
		assertNotNull(layer);
		for (Coordinate coordinate : makeDensePointData()) {
			Point point = layer.getGeometryFactory().createPoint(coordinate);
			SpatialRecord record = layer.add(point);
			assertNotNull(record);
		}
		saveLayerAsImage(layer, 300, 300);

		Envelope bbox = layer.getIndex().getBoundingBox();
		double[] centre = bbox.centre();
		
		List<SpatialDatabaseRecord> results = GeoPipeline
			.startNearestNeighborLatLonSearch(layer, new Coordinate(centre[0], centre[1]), 10.0)
			.toSpatialDatabaseRecordList();
		
		saveResultsAsImage(results, "temporary-results-layer-" + layer.getName(), 150, 150);
		assertEquals(456, results.size());
	}

	private void saveLayerAsImage(Layer layer, int width, int height) {
		ShapefileExporter shpExporter = new ShapefileExporter(graphDb());
		shpExporter.setExportDir("target/export/SimplePointTests");
		StyledImageExporter imageExporter = new StyledImageExporter(graphDb());
		imageExporter.setExportDir("target/export/SimplePointTests");
		imageExporter.setZoom(0.9);
		imageExporter.setSize(width, height);
		try {
			imageExporter.saveLayerImage(layer.getName());
			shpExporter.exportLayer(layer.getName());
		} catch (Exception e) {
			throw new AssertionFailedError("Failed to save layer '" + layer.getName() + "' as image: " + e.getMessage());
		}
	}

	private void saveResultsAsImage(List<? extends SpatialRecord> results, String layerName, int width, int height) {	
		ShapefileExporter shpExporter = new ShapefileExporter(graphDb());
		shpExporter.setExportDir("target/export/SimplePointTests");
		StyledImageExporter imageExporter = new StyledImageExporter(graphDb());
		imageExporter.setExportDir("target/export/SimplePointTests");
		imageExporter.setZoom(0.9);
		imageExporter.setSize(width, height);
		SpatialDatabaseService db = new SpatialDatabaseService(graphDb());
		EditableLayer tmpLayer = (EditableLayer) db.createSimplePointLayer(layerName, "lon", "lat");
		for (SpatialRecord record : results) {
			tmpLayer.add(record.getGeometry());
		}
		try {
			imageExporter.saveLayerImage(layerName);
			shpExporter.exportLayer(layerName);
		} catch (Exception e) {
			throw new AssertionFailedError("Failed to save results image: " + e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	private static Coordinate[] makeCoordinateDataFromTextFile(String textFile) {
		CoordinateList data = new CoordinateList();
		try {
			BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/"+textFile));
			Coordinate origin = new Coordinate(13.0, 55.6);
			String line;
			int row = 0;
			while ((line = reader.readLine()) != null) {
				int col = 0;
				for (String character : line.split("")) {
					if (col > 0 && !character.matches("\\s")) {
						Coordinate coordinate = new Coordinate(origin.x + (double) col / 100.0, origin.y - (double) row / 100.0);
						data.add(coordinate);
					}
					col++;
				}
				row++;
			}
		} catch (IOException e) {
			throw new AssertionFailedError("Input data for string test invalid: " + e.getMessage());
		}
		return data.toCoordinateArray();
	}

	@SuppressWarnings("unchecked")
	private static Coordinate[] makeDensePointData() {
		CoordinateList data = new CoordinateList();
		Coordinate origin = new Coordinate(13.0, 55.6);
		for (int row = 0; row < 40; row++) {
			for (int col = 0; col < 40; col++) {
				Coordinate coordinate = new Coordinate(origin.x + (double) col / 100.0, origin.y - (double) row / 100.0);
				data.add(coordinate);
			}
		}
		return data.toCoordinateArray();
	}

}
