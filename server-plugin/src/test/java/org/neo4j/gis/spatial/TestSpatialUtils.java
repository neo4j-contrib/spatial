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
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.index.IndexManagerImpl;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spatial.api.layer.EditableLayer;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.testutils.Neo4jTestCase;

public class TestSpatialUtils extends Neo4jTestCase {

	@Override
	protected List<Class<?>> loadProceduresAndFunctions() {
		return List.of(SpatialFunctions.class, SpatialProcedures.class);
	}

	@Test
	public void testJTSLinearRef() {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		Geometry geometry;
		try (Transaction tx = graphDb().beginTx()) {
			EditableLayer layer = spatial.getOrCreateEditableLayer(tx, "jts", null, null, false);
			Coordinate[] coordinates = new Coordinate[]{new Coordinate(0, 0), new Coordinate(0, 1),
					new Coordinate(1, 1)};
			geometry = layer.getGeometryFactory().createLineString(coordinates);
			layer.add(tx, geometry);
			layer.finalizeTransaction(tx);
			tx.commit();
		}

		try (Transaction tx = graphDb().beginTx()) {
			double delta = 0.0001;
			Layer layer = spatial.getLayer(tx, "jts", true);
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
}
