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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;


public class TestRemove extends Neo4jTestCase {

	public void testAddMoreThanMaxNodeRefThenDeleteAll() throws Exception {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());

        EditableLayer layer = (EditableLayer) spatialService
        	.createLayer("TestRemove", WKTGeometryEncoder.class, EditableLayerImpl.class);
        GeometryFactory geomFactory = layer.getGeometryFactory();
        
        int rtreeMaxNodeReferences = 100;
        
        long[] ids = new long[rtreeMaxNodeReferences + 1];
        for (int i = 0; i < ids.length; i++) {
        	ids[i] = layer.add(geomFactory.createPoint(new Coordinate(i, i))).getNodeId();
        }

        debugIndexTree((LayerRTreeIndex) layer.getIndex());        
        
        for (long id : ids) {
        	layer.delete(id);
        }
        
        debugIndexTree((LayerRTreeIndex) layer.getIndex());
    }		
}