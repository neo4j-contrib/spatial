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
package org.neo4j.spatial.osm.server.plugin;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.neo4j.gis.spatial.EditableLayerImpl;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.SpatialTopologyUtils;
import org.neo4j.gis.spatial.SpatialTopologyUtils.PointResult;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.index.IndexManagerImpl;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.osm.server.plugin.procedures.OsmSpatialProcedures;
import org.neo4j.spatial.testutils.Neo4jTestCase;

public class OsmSnappingTest extends Neo4jTestCase {

	private static final Logger LOGGER = Logger.getLogger(OsmSnappingTest.class.getName());

	@Override
	protected List<Class<?>> loadProceduresAndFunctions() {
		return List.of(SpatialFunctions.class, SpatialProcedures.class, OsmSpatialProcedures.class);
	}

	@Test
	public void testSnapping() throws Exception {
		printDatabaseStats();
		String osm = "map.osm";
		loadTestOsmData(osm, 1000);
		printDatabaseStats();

		// Define dynamic layers
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		try (Transaction tx = graphDb().beginTx()) {
			OSMLayer osmLayer = (OSMLayer) spatial.getLayer(tx, osm, false);
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
			OSMLayer osmLayer = (OSMLayer) spatial.getLayer(tx, osm, true);
			OSMDataset.fromLayer(tx, osmLayer); // cache for future usage below
			GeometryFactory factory = osmLayer.getGeometryFactory();
			EditableLayerImpl resultsLayer = (EditableLayerImpl) spatial.getOrCreateEditableLayer(tx,
					"testSnapping_results", null, null, false);
			Point point = factory.createPoint(new Coordinate(12.9777, 56.0555));
			resultsLayer.add(tx, point,
					Map.of("snap-id", 0L, "description", "Point to snap", "distance", 0L)
			);
			for (String layerName : new String[]{"railway", "highway-residential"}) {
				Layer layer = osmLayer.getLayer(tx, layerName);
				assertNotNull(layer, "Missing layer: " + layerName);
				LOGGER.fine("Closest features in " + layerName + " to point " + point + ":");
				List<PointResult> edgeResults = SpatialTopologyUtils.findClosestEdges(tx, point, layer);
				for (PointResult result : edgeResults) {
					LOGGER.fine("\t" + result);
					resultsLayer.add(tx, result.getKey(),
							Map.of("snap-id", result.getValue().getGeomNode().getElementId(),
									"description",
									"Snapped point to layer " + layerName + ": " + result.getValue().getGeometry()
											.toString(),
									"distance", (long) (1000000 * result.getDistance())));
				}
				assertFalse(edgeResults.isEmpty(), "Expected to snap to an edge in layer " + layerName);
				PointResult closest = edgeResults.getFirst();
				for (PointResult result : edgeResults) {
					assertTrue(closest.getDistance() <= result.getDistance(), "Closest edge should come first");
				}
				assertTrue(closest.getValue().getGeometry().distance(closest.getKey()) < 1e-9,
						"Snapped point should lie on the closest edge");
			}
			tx.commit();
		}

	}

	@SuppressWarnings("SameParameterValue")
	private void loadTestOsmData(String layerName, int commitInterval) throws Exception {
		LOGGER.fine("\n=== Loading layer " + layerName + " from " + layerName + " ===");
		OSMImporter importer = new OSMImporter(layerName);
		importer.setCharset(StandardCharsets.UTF_8);
		importer.importFile(graphDb(), layerName, commitInterval);
		importer.reIndex(graphDb(), commitInterval);
	}
}
