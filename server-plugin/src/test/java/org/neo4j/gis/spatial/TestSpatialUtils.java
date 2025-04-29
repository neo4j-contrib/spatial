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

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.linearref.LengthIndexedLine;
import org.locationtech.jts.linearref.LocationIndexedLine;
import org.neo4j.gis.spatial.SpatialTopologyUtils.PointResult;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.osm.OSMDataset;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class TestSpatialUtils extends Neo4jTestCase {

	@Test
	public void testJTSLinearRef() {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		Geometry geometry;
		try (Transaction tx = graphDb().beginTx()) {
			EditableLayer layer = spatial.getOrCreateEditableLayer(tx, "jts", null, null);
			Coordinate[] coordinates = new Coordinate[]{new Coordinate(0, 0), new Coordinate(0, 1),
					new Coordinate(1, 1)};
			geometry = layer.getGeometryFactory().createLineString(coordinates);
			layer.add(tx, geometry);
			debugLRS(geometry);
			tx.commit();
		}

		try (Transaction tx = graphDb().beginTx()) {
			double delta = 0.0001;
			Layer layer = spatial.getLayer(tx, "jts");
			// Now test the new API in the topology utils
			Point point = SpatialTopologyUtils.locatePoint(layer, geometry, 1.5, 0.5);
			assertEquals(0.5, point.getX(), delta, "X location incorrect");
			assertEquals(1.5, point.getY(), delta, "Y location incorrect");
			point = SpatialTopologyUtils.locatePoint(layer, geometry, 1.5, -0.5);
			assertEquals(0.5, point.getX(), delta, "X location incorrect");
			assertEquals(0.5, point.getY(), delta, "Y location incorrect");
			point = SpatialTopologyUtils.locatePoint(layer, geometry, 0.5, 0.5);
			assertEquals(-0.5, point.getX(), delta, "X location incorrect");
			assertEquals(0.5, point.getY(), delta, "Y location incorrect");
			point = SpatialTopologyUtils.locatePoint(layer, geometry, 0.5, -0.5);
			assertEquals(0.5, point.getX(), delta, "X location incorrect");
			assertEquals(0.5, point.getY(), delta, "Y location incorrect");
			tx.commit();
		}
	}

	/**
	 * This method just prints a bunch of information to the console to help
	 * understand the behaviour of the JTS LRS methods better. Currently no
	 * assertions are made.
	 */
	private static void debugLRS(Geometry geometry) {
		LengthIndexedLine line = new org.locationtech.jts.linearref.LengthIndexedLine(geometry);
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
		System.out.println(
				"Extracting line from position 0.0 to " + (length / 2) + ": " + line.extractLine(0, length / 2));
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
		// This was an ignored test, so perhaps not worth saving?
		printDatabaseStats();
		String osm = "map.osm";
		loadTestOsmData(osm, 1000);
		printDatabaseStats();

		// Define dynamic layers
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		try (Transaction tx = graphDb().beginTx()) {
			OSMLayer osmLayer = (OSMLayer) spatial.getLayer(tx, osm);
			osmLayer.addSimpleDynamicLayer(tx, "highway", "primary");
			osmLayer.addSimpleDynamicLayer(tx, "highway", "secondary");
			osmLayer.addSimpleDynamicLayer(tx, "highway", "tertiary");
			osmLayer.addSimpleDynamicLayer(tx, "highway", "residential");
			osmLayer.addSimpleDynamicLayer(tx, "highway", "footway");
			osmLayer.addSimpleDynamicLayer(tx, "highway", "cycleway");
			osmLayer.addSimpleDynamicLayer(tx, "highway", "track");
			osmLayer.addSimpleDynamicLayer(tx, "highway", "path");
			osmLayer.addSimpleDynamicLayer(tx, "railway", null);
			osmLayer.addSimpleDynamicLayer(tx, "highway", null);
			tx.commit();
		}

		// Now test snapping to a layer
		try (Transaction tx = graphDb().beginTx()) {
			OSMLayer osmLayer = (OSMLayer) spatial.getLayer(tx, osm);
			OSMDataset.fromLayer(tx, osmLayer); // cache for future usage below
			GeometryFactory factory = osmLayer.getGeometryFactory();
			EditableLayerImpl resultsLayer = (EditableLayerImpl) spatial.getOrCreateEditableLayer(tx,
					"testSnapping_results", null, null);
			String[] fieldsNames = new String[]{"snap-id", "description", "distance"};
			resultsLayer.setExtraPropertyNames(fieldsNames, tx);
			Point point = factory.createPoint(new Coordinate(12.9777, 56.0555));
			resultsLayer.add(tx, point, fieldsNames, new Object[]{0L, "Point to snap", 0L});
			for (String layerName : new String[]{"railway", "highway-residential"}) {
				Layer layer = osmLayer.getLayer(tx, layerName);
				assertNotNull(layer, "Missing layer: " + layerName);
				System.out.println("Closest features in " + layerName + " to point " + point + ":");
				List<PointResult> edgeResults = SpatialTopologyUtils.findClosestEdges(tx, point, layer);
				for (PointResult result : edgeResults) {
					System.out.println("\t" + result);
					resultsLayer.add(tx, result.getKey(), fieldsNames,
							new Object[]{result.getValue().getGeomNode().getElementId(),
									"Snapped point to layer " + layerName + ": " + result.getValue().getGeometry()
											.toString(),
									(long) (1000000 * result.getDistance())});
				}
				if (edgeResults.size() > 0) {
					PointResult closest = edgeResults.get(0);
					Point closestPoint = closest.getKey();

					SpatialDatabaseRecord wayRecord = closest.getValue();
					OSMDataset.Way way = ((OSMDataset) osmLayer.getDataset()).getWayFrom(wayRecord.getGeomNode());
					OSMDataset.WayPoint wayPoint = way.getPointAt(closestPoint.getCoordinate());
					// TODO: presumably we meant to assert something here?
				}
			}
			tx.commit();
		}

	}

	@SuppressWarnings("SameParameterValue")
	private void loadTestOsmData(String layerName, int commitInterval) throws Exception {
		System.out.println("\n=== Loading layer " + layerName + " from " + layerName + " ===");
		OSMImporter importer = new OSMImporter(layerName);
		importer.setCharset(StandardCharsets.UTF_8);
		importer.importFile(graphDb(), layerName, commitInterval);
		importer.reIndex(graphDb(), commitInterval);
	}
}
