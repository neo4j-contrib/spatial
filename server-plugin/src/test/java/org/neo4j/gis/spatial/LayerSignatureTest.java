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

import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.index.IndexManagerImpl;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.testutils.Neo4jTestCase;

public class LayerSignatureTest extends Neo4jTestCase {

	private SpatialDatabaseService spatial;

	@Override
	protected List<Class<?>> loadProceduresAndFunctions() {
		return List.of(SpatialFunctions.class, SpatialProcedures.class);
	}

	@BeforeEach
	public void setup() throws Exception {
		spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
	}

	@Test
	public void testSimplePointLayer() {
		testLayerSignature("EditableLayer(name='test', encoder=SimplePointEncoder(x='lng', y='lat', bbox='bbox'))",
				tx -> spatial.createSimplePointLayer(tx, "test", "lng", "lat"));
	}

	@Test
	public void testNativePointLayer() {
		testLayerSignature(
				"EditableLayer(name='test', encoder=NativePointEncoder(geometry='position', bbox='mbr', crs=4326))",
				tx -> spatial.createNativePointLayer(tx, "test", "position", "mbr"));
	}

	@Test
	public void testDefaultSimplePointLayer() {
		testLayerSignature(
				"EditableLayer(name='test', encoder=SimplePointEncoder(x='longitude', y='latitude', bbox='bbox'))",
				tx -> spatial.createSimplePointLayer(tx, "test"));
	}

	@Test
	public void testSimpleWKBLayer() {
		testLayerSignature("EditableLayer(name='test', encoder=WKBGeometryEncoder(geom='geometry', bbox='bbox'))",
				tx -> spatial.createWKBLayer(tx, "test", null));
	}

	@Test
	public void testWKBLayer() {
		testLayerSignature("EditableLayer(name='test', encoder=WKBGeometryEncoder(geom='wkb', bbox='bbox'))",
				tx -> spatial.getOrCreateEditableLayer(tx, "test", "wkb", "wkb", null, true));
	}

	@Test
	public void testWKTLayer() {
		testLayerSignature("EditableLayer(name='test', encoder=WKTGeometryEncoder(geom='wkt', bbox='bbox'))",
				tx -> spatial.getOrCreateEditableLayer(tx, "test", "wkt", "wkt", null, true));
	}

	// TODO this is duplicated code
	private Layer testLayerSignature(String signature, Function<Transaction, Layer> layerMaker) {
		Layer layer;
		try (Transaction tx = graphDb().beginTx()) {
			layer = layerMaker.apply(tx);
			tx.commit();
		}
		assertEquals(signature, layer.getSignature());
		return layer;
	}
}
