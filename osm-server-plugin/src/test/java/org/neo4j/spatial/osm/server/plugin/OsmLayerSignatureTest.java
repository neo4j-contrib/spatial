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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gis.spatial.Constants.GTYPE_POINT;

import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gis.spatial.DynamicLayer;
import org.neo4j.gis.spatial.DynamicLayerConfig;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.index.IndexManagerImpl;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.osm.server.plugin.procedures.OsmSpatialProcedures;
import org.neo4j.spatial.testutils.Neo4jTestCase;

public class OsmLayerSignatureTest extends Neo4jTestCase {

	private SpatialDatabaseService spatial;

	@Override
	protected List<Class<?>> loadProceduresAndFunctions() {
		return List.of(SpatialFunctions.class, SpatialProcedures.class, OsmSpatialProcedures.class);
	}

	@BeforeEach
	public void setup() {
		spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
	}

	private Layer testLayerSignature(String signature, Function<Transaction, Layer> layerMaker) {
		Layer layer;
		try (Transaction tx = graphDb().beginTx()) {
			layer = layerMaker.apply(tx);
			tx.commit();
		}
		assertEquals(signature, layer.getSignature());
		return layer;
	}

	@Test
	public void testDynamicLayer() {
		Layer layer = testLayerSignature(
				"EditableLayer(name='test', encoder=WKTGeometryEncoder(geom='wkt', bbox='bbox'))",
				tx -> spatial.getOrCreateEditableLayer(tx, "test", "wkt", "wkt", null, false));
		inTx(tx -> {
			DynamicLayer dynamic = spatial.asDynamicLayer(tx, layer);
			assertEquals("EditableLayer(name='test', encoder=WKTGeometryEncoder(geom='wkt', bbox='bbox'))",
					dynamic.getSignature());
			DynamicLayerConfig points = dynamic.addCQLDynamicLayerOnAttribute(tx, "is_a", "point", GTYPE_POINT);
			assertEquals(
					"DynamicLayer(name='CQL:is_a-point', config={layer='CQL:is_a-point', query=\"geometryType(the_geom) = 'Point' AND is_a = 'point'\"})",
					points.getSignature());
		});
	}

}
