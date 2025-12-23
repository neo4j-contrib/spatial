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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateList;
import org.neo4j.gis.spatial.DynamicLayer;
import org.neo4j.gis.spatial.EditableLayerImpl;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.encoders.SimpleGraphEncoder;
import org.neo4j.gis.spatial.encoders.SimplePropertyEncoder;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.index.IndexManagerImpl;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spatial.api.encoder.GeometryEncoder;
import org.neo4j.spatial.api.layer.EditableLayer;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.cli.tools.ShapefileExporter;
import org.neo4j.spatial.osm.server.plugin.procedures.OsmSpatialProcedures;
import org.neo4j.spatial.testutils.Neo4jTestCase;

public class OsmLayersTest extends Neo4jTestCase {

	@Override
	protected List<Class<?>> loadProceduresAndFunctions() {
		return List.of(SpatialFunctions.class, SpatialProcedures.class, OsmSpatialProcedures.class);
	}

	@Test
	public void testShapefileExport() throws Exception {
		ShapefileExporter exporter = new ShapefileExporter(driver, DEFAULT_DATABASE_NAME);
		exporter.setExportDir("target/export");
		ArrayList<String> layers = new ArrayList<>();

		layers.add(testSpecificEditableLayer("test dynamic layer with property encoder", SimplePropertyEncoder.class,
				DynamicLayer.class));
		layers.add(testSpecificEditableLayer("test dynamic layer with graph encoder", SimpleGraphEncoder.class,
				DynamicLayer.class));
		layers.add(testSpecificEditableLayer("test dynamic layer with OSM encoder", OSMGeometryEncoder.class,
				OSMLayer.class));

		for (String layerName : layers) {
			exporter.exportLayer(layerName);
		}
	}

	@Test
	public void testEditableLayers() {
		testSpecificEditableLayer("test OSM layer with OSM encoder", OSMGeometryEncoder.class, OSMLayer.class);
		testSpecificEditableLayer("test editable layer with OSM encoder", OSMGeometryEncoder.class,
				EditableLayerImpl.class);
	}

	private String testSpecificEditableLayer(String layerName, Class<? extends GeometryEncoder> geometryEncoderClass,
			Class<? extends Layer> layerClass) {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		inTx(tx -> {
			Layer layer = spatial.createLayer(tx, layerName, geometryEncoderClass, layerClass, null);
			assertNotNull(layer);
			assertInstanceOf(EditableLayer.class, layer, "Should be an editable layer");
		});
		inTx(tx -> {
			Layer layer = spatial.getLayer(tx, layerName, false);
			assertNotNull(layer);
			assertInstanceOf(EditableLayer.class, layer, "Should be an editable layer");
			EditableLayer editableLayer = (EditableLayer) layer;

			CoordinateList coordinates = new CoordinateList();
			coordinates.add(new Coordinate(13.1, 56.2), false);
			coordinates.add(new Coordinate(13.2, 56.0), false);
			coordinates.add(new Coordinate(13.3, 56.2), false);
			coordinates.add(new Coordinate(13.2, 56.0), false);
			coordinates.add(new Coordinate(13.1, 56.2), false);
			coordinates.add(new Coordinate(13.0, 56.0), false);
			editableLayer.add(tx, layer.getGeometryFactory().createLineString(coordinates.toCoordinateArray()));

			coordinates = new CoordinateList();
			coordinates.add(new Coordinate(14.1, 56.0), false);
			coordinates.add(new Coordinate(14.3, 56.1), false);
			coordinates.add(new Coordinate(14.2, 56.1), false);
			coordinates.add(new Coordinate(14.0, 56.0), false);
			editableLayer.add(tx, layer.getGeometryFactory().createLineString(coordinates.toCoordinateArray()));
			editableLayer.finalizeTransaction(tx);
		});

		return layerName;
	}
}
