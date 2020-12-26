/*
 * Copyright (c) 2010-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;

public class TestRemove extends Neo4jTestCase {
    private static final String layerName = "TestRemove";

    @Test
    public void testAddMoreThanMaxNodeRefThenDeleteAll() {
        SpatialDatabaseService spatialService = new SpatialDatabaseService();

        try (Transaction tx = graphDb().beginTx()) {
            spatialService.createLayer(tx, layerName, WKTGeometryEncoder.class, EditableLayerImpl.class);
            tx.commit();
        }

        int rtreeMaxNodeReferences = 100;

        long[] ids = new long[rtreeMaxNodeReferences + 1];

        try (Transaction tx = graphDb().beginTx()) {
            EditableLayer layer = (EditableLayer) spatialService.getLayer(tx, layerName);
            GeometryFactory geomFactory = layer.getGeometryFactory();
            for (int i = 0; i < ids.length; i++) {
                ids[i] = layer.add(tx, geomFactory.createPoint(new Coordinate(i, i))).getNodeId();
            }
            tx.commit();
        }

        Neo4jTestUtils.debugIndexTree(graphDb(), layerName);

        try (Transaction tx = graphDb().beginTx()) {
            EditableLayer layer = (EditableLayer) spatialService.getLayer(tx, layerName);
            for (long id : ids) {
                layer.delete(tx, id);
            }
            tx.commit();
        }

        Neo4jTestUtils.debugIndexTree(graphDb(), layerName);
    }
}