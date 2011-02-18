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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.neo4j.gis.spatial.SpatialTopologyUtils.PointResult;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.query.SearchContain;
import org.neo4j.gis.spatial.query.SearchWithin;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class TestSpatialUtils extends Neo4jTestCase {

	@Test
	public void testSnapping() throws Exception {
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
			for (PointResult result : SpatialTopologyUtils.findClosestEdges(point, layer)) {
				System.out.println("\t" + result);
				results.add(result.getKey(), fieldsNames, new Object[] { result.getValue().getGeomNode().getId(),
				        "Snapped point to layer " + layerName + ": " + result.getValue().getGeometry().toString(), (long)(1000000*result.getDistance()) });
			}
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
