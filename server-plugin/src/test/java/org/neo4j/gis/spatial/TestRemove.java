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

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class TestRemove extends Neo4jTestCase {

	private static final String layerName = "TestRemove";

	@Test
	public void testAddMoreThanMaxNodeRefThenDeleteAll() {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));

		try (Transaction tx = graphDb().beginTx()) {
			spatial.createLayer(tx, layerName, WKTGeometryEncoder.class, EditableLayerImpl.class, "");
			tx.commit();
		}

		int rtreeMaxNodeReferences = 100;

		String[] ids = new String[rtreeMaxNodeReferences + 1];

		try (Transaction tx = graphDb().beginTx()) {
			EditableLayer layer = (EditableLayer) spatial.getLayer(tx, layerName);
			GeometryFactory geomFactory = layer.getGeometryFactory();
			for (int i = 0; i < ids.length; i++) {
				ids[i] = layer.add(tx, geomFactory.createPoint(new Coordinate(i, i))).getNodeId();
			}
			tx.commit();
		}

		Neo4jTestUtils.debugIndexTree(graphDb(), layerName);

		try (Transaction tx = graphDb().beginTx()) {
			EditableLayer layer = (EditableLayer) spatial.getLayer(tx, layerName);
			for (String id : ids) {
				layer.delete(tx, id);
			}
			tx.commit();
		}

		Neo4jTestUtils.debugIndexTree(graphDb(), layerName);
	}
}
