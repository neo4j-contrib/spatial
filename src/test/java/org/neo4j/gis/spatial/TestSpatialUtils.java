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

import java.nio.charset.Charset;
import java.util.ArrayList;

import org.junit.Test;
import org.neo4j.gis.spatial.SpatialTopologyUtils.PointResult;
import org.neo4j.gis.spatial.osm.OSMDataset;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.linearref.LengthIndexedLine;
import com.vividsolutions.jts.linearref.LocationIndexedLine;

public class TestSpatialUtils extends Neo4jTestCase {

	@Test
	public void testJTSLinearRef() {
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		EditableLayer layer = spatialService.getOrCreateEditableLayer("jts");
		Coordinate[] coordinates = new Coordinate[] { new Coordinate(0, 0), new Coordinate(0, 1), new Coordinate(1, 1) };
		Geometry geometry = layer.getGeometryFactory().createLineString(coordinates);
		layer.add(geometry);
		debugLRS(geometry);

		// Now test the new API in the topology utils
		Point point = SpatialTopologyUtils.locatePoint(layer, geometry, 1.5, 0.5);
		assertEquals("X location incorrect", 0.5, point.getX());
		assertEquals("Y location incorrect", 1.5, point.getY());
		point = SpatialTopologyUtils.locatePoint(layer, geometry, 1.5, -0.5);
		assertEquals("X location incorrect", 0.5, point.getX());
		assertEquals("Y location incorrect", 0.5, point.getY());
		point = SpatialTopologyUtils.locatePoint(layer, geometry, 0.5, 0.5);
		assertEquals("X location incorrect", -0.5, point.getX());
		assertEquals("Y location incorrect", 0.5, point.getY());
		point = SpatialTopologyUtils.locatePoint(layer, geometry, 0.5, -0.5);
		assertEquals("X location incorrect", 0.5, point.getX());
		assertEquals("Y location incorrect", 0.5, point.getY());
	}

	/**
	 * This method just prints a bunch of information to the console to help
	 * understand the behaviour of the JTS LRS methods better. Currently no
	 * assertions are made.
	 * 
	 * @param geometry
	 */
	private void debugLRS(Geometry geometry) {
		LengthIndexedLine line = new com.vividsolutions.jts.linearref.LengthIndexedLine(geometry);
		double length = line.getEndIndex() - line.getStartIndex();
		System.out.println("Have Geometry: " + geometry);
		System.out.println("Have LengthIndexedLine: " + line);
		System.out.println("Have start index: " + line.getStartIndex());
		System.out.println("Have end index: " + line.getEndIndex());
		System.out.println("Have length: " + length);
		System.out.println("Extracting point at position 0.0: " + line.extractPoint(0.0));
		System.out.println("Extracting point at position 0.1: " + line.extractPoint(0.1));
		System.out.println("Extracting point at position 0.5: " + line.extractPoint(0.5));
		System.out.println("Extracting point at position 0.9: " + line.extractPoint(0.9));
		System.out.println("Extracting point at position 1.0: " + line.extractPoint(1.0));
		System.out.println("Extracting point at position 1.5: " + line.extractPoint(1.5));
		System.out.println("Extracting point at position 1.5 offset 0.5: " + line.extractPoint(1.5, 0.5));
		System.out.println("Extracting point at position 1.5 offset -0.5: " + line.extractPoint(1.5, -0.5));
		System.out.println("Extracting point at position " + length + ": " + line.extractPoint(length));
		System.out.println("Extracting point at position " + (length / 2) + ": " + line.extractPoint(length / 2));
		System.out.println("Extracting line from position 0.1 to 0.2: " + line.extractLine(0.1, 0.2));
		System.out.println("Extracting line from position 0.0 to " + (length / 2) + ": " + line.extractLine(0, length / 2));
		LocationIndexedLine pline = new LocationIndexedLine(geometry);
		System.out.println("Have LocationIndexedLine: " + pline);
		System.out.println("Have start index: " + pline.getStartIndex());
		System.out.println("Have end index: " + pline.getEndIndex());
		System.out.println("Extracting point at start: " + pline.extractPoint(pline.getStartIndex()));
		System.out.println("Extracting point at end: " + pline.extractPoint(pline.getEndIndex()));
		System.out.println("Extracting point at start offset 0.5: " + pline.extractPoint(pline.getStartIndex(), 0.5));
		System.out.println("Extracting point at end offset 0.5: " + pline.extractPoint(pline.getEndIndex(), 0.5));
	}

	@Test
	public void testSnapping() throws Exception {
		if (true)
			return;
		
		printDatabaseStats();
		String osm = "map.osm";
		loadTestOsmData(osm, 1000);
		printDatabaseStats();

		// Define dynamic layers
		ArrayList<Layer> layers = new ArrayList<Layer>();
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		OSMLayer osmLayer = (OSMLayer) spatialService.getLayer(osm);
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "primary"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "secondary"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "tertiary"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "residential"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "footway"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "cycleway"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "track"));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", "path"));
		layers.add(osmLayer.addSimpleDynamicLayer("railway", null));
		layers.add(osmLayer.addSimpleDynamicLayer("highway", null));

		// Now test snapping to a layer
		GeometryFactory factory = osmLayer.getGeometryFactory();
		EditableLayerImpl results = (EditableLayerImpl) spatialService.getOrCreateEditableLayer("testSnapping_results");
		String[] fieldsNames = new String[] { "snap-id", "description", "distance" };
		results.setExtraPropertyNames(fieldsNames);
		Point point = factory.createPoint(new Coordinate(12.9777, 56.0555));
		results.add(point, fieldsNames, new Object[] { 0L, "Point to snap", 0L });
		for (String layerName : new String[] { "railway", "highway-residential" }) {
			Layer layer = osmLayer.getLayer(layerName);
			assertNotNull("Missing layer: " + layerName, layer);
			System.out.println("Closest features in " + layerName + " to point " + point + ":");
			ArrayList<PointResult> edgeResults = SpatialTopologyUtils.findClosestEdges(point, layer);
			for (PointResult result : edgeResults) {
				System.out.println("\t" + result);
				results.add(result.getKey(), fieldsNames, new Object[] { result.getValue().getGeomNode().getId(),
						"Snapped point to layer " + layerName + ": " + result.getValue().getGeometry().toString(),
						(long) (1000000 * result.getDistance()) });
			}
			if (edgeResults.size() > 0) {
				PointResult closest = edgeResults.get(0);
				Point closestPoint = closest.getKey();

				SpatialDatabaseRecord wayRecord = closest.getValue();
				OSMDataset.Way way = ((OSMDataset) osmLayer.getDataset()).getWayFrom(wayRecord.getGeomNode());
				OSMDataset.WayPoint wayPoint = way.getPointAt(closestPoint.getCoordinate());
			}
		}

	}

	private void loadTestOsmData(String layerName, int commitInterval) throws Exception {
		String osmPath = layerName;
		System.out.println("\n=== Loading layer " + layerName + " from " + osmPath + " ===");
		reActivateDatabase(false, true, false);
		OSMImporter importer = new OSMImporter(layerName);
		importer.setCharset(Charset.forName("UTF-8"));
		importer.importFile(getBatchInserter(), osmPath);
		reActivateDatabase(false, false, false);
		importer.reIndex(graphDb(), commitInterval);
	}

}
