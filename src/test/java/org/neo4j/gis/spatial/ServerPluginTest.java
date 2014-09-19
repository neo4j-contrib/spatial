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

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.server.plugin.SpatialPlugin;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import com.vividsolutions.jts.geom.Envelope;


public class ServerPluginTest extends Neo4jTestCase {

	private static final String LAYER = "layer";
	private static final String LON = "lon";
	private static final String LAT = "lat";
	private SpatialPlugin plugin;

	@Before
	public void setUp() throws Exception {
		super.setUp();
		plugin = new SpatialPlugin();

	}

	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}

	@Test
	public void testCreateLayer() {
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		assertNull(spatialService.getLayer(LAYER));
		plugin.addSimplePointLayer(graphDb(), LAYER, LAT, LON);

		assertNotNull(spatialService.getLayer(LAYER));
	}

	@Test
	public void testSearchPoints() {
	    Transaction tx2 = graphDb().beginTx();
        Node point = graphDb().createNode();
        point.setProperty(LAT, 60.1);
        point.setProperty(LON, 15.2);
        tx2.success();
        tx2.finish();
        plugin.addSimplePointLayer( graphDb(), LAYER, LAT, LON );
        
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		Layer layer = spatialService.getLayer(LAYER);
        debugIndexTree((RTreeIndex) layer.getIndex());
        
        plugin.addNodeToLayer(graphDb(), point, LAYER);
        Iterable<Node> geometries = plugin.findGeometriesInBBox( graphDb(), 15.0, 15.3, 60.0, 60.2, LAYER );
        assertTrue( geometries.iterator().hasNext() );
        
        geometries = plugin.findGeometriesWithinDistance(graphDb(), 15.2, 60.1, 100, LAYER);
        assertTrue(geometries.iterator().hasNext());
//        plugin.addEditableLayer(graphDb(), LAYER);
//        plugin.addGeometryWKTToLayer(graphDb(), "POINT(15.2 60.1)", LAYER);
//        plugin.addCQLDynamicLayer(graphDb(), LAYER, "CQL1", "Geometry", "within(the_geom, POLYGON((15.1 60.0, 15.1 60.2, 15.2 60.2, 15.2 60.0, 15.1 60.0)))");
//        geometries = plugin.findGeometriesInLayer( graphDb(), 15.0, 15.3, 60.0, 60.2, "CQL1" );
//        assertTrue( geometries.iterator().hasNext() );

	}
	
	@Test
	public void testAddPointToLayerWithDefaults() {
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        plugin.addSimplePointLayer( graphDb(), LAYER, LAT, LON );
		assertNotNull(spatialService.getLayer(LAYER));
		Layer layer2 = spatialService.getLayer(LAYER);

        try (Transaction tx = graphDb().beginTx()) {
            List<SpatialDatabaseRecord> results = GeoPipeline
                .startWithinSearch(layer2, layer2.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 60.0, 61.0)))
                .toSpatialDatabaseRecordList();

            assertEquals(0, results.size());
            tx.success();
        }
        Node point;
        try (Transaction tx = graphDb().beginTx()) {
            point = graphDb().createNode();
            point.setProperty(LAT, 60.1);
            point.setProperty(LON, 15.2);
            point.setProperty(Constants.PROP_BBOX, new double[] { 15.2, 60.1, 15.2, 60.1 });
            tx.success();
        }
		plugin.addNodeToLayer(graphDb(), point, LAYER);
		plugin.addGeometryWKTToLayer(graphDb(), "POINT(15.2 60.1)", LAYER);

        try (Transaction tx = graphDb().beginTx()) {
            List<SpatialDatabaseRecord> results = GeoPipeline
                .startWithinSearch(layer2, layer2.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 60.0, 61.0)))
                .toSpatialDatabaseRecordList();

            assertEquals(2, results.size());
            tx.success();
        }
	}
	
	@Test
	public void testDynamicLayer() {
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        try (Transaction tx = graphDb().beginTx()) {
            plugin.addEditableLayer(graphDb(), LAYER, "WKT","wkt");
            Layer layer = spatialService.getLayer(LAYER);
            assertNotNull("Could not find layer '" + LAYER + "'", layer);
            checkResults(plugin.addGeometryWKTToLayer(graphDb(), "POINT(15.2 60.1)", LAYER), 1, layer);
            checkResults(plugin.addGeometryWKTToLayer(graphDb(), "LINESTRING(15.1 60.2, 15.3 60.1)", LAYER), 1, layer);
            checkResults(plugin.addGeometryWKTToLayer(graphDb(), "POLYGON((15.1 60.0, 15.1 60.2, 15.2 60.2, 15.2 60.0, 15.1 60.0))", LAYER), 1, layer);
            checkResults(plugin.addGeometryWKTToLayer(graphDb(), "POINT(15.15 60.15)", LAYER), 1, layer);
            checkResults(plugin.addCQLDynamicLayer(graphDb(), LAYER, "CQL1", "Geometry", "within(the_geom, POLYGON((15.1 60.0, 15.1 60.2, 15.2 60.2, 15.2 60.0, 15.1 60.0)))"), 1, layer);
            checkResults(plugin.addCQLDynamicLayer(graphDb(), LAYER, "CQL2", "Geometry", "within(the_geom, POLYGON((15.14 60.14, 15.14 60.16, 15.16 60.16, 15.16 60.14, 15.14 60.14)))"), 1, layer);
            assertNotNull(spatialService.getLayer("CQL1"));
            assertNotNull(spatialService.getLayer("CQL2"));
            checkResults(plugin.findGeometriesInBBox(graphDb(), 15.0, 15.3, 60.0, 60.2, LAYER), 4, layer);
            checkResults(plugin.findGeometriesInBBox(graphDb(), 15.1, 15.2, 60.0, 60.2, LAYER), 2, layer);
            checkResults(plugin.findGeometriesInBBox(graphDb(), 15.0, 15.3, 60.0, 60.2, "CQL1"), 2, layer);
            checkResults(plugin.findGeometriesInBBox(graphDb(), 15.0, 15.3, 60.0, 60.2, "CQL2"), 1, layer);
            tx.success();
        }
	}
	
	private int checkResults(Iterable<Node> results, int expected, Layer layer) {
		int count = 0;
        try (Transaction tx = graphDb().beginTx()) {
            StringBuffer sb = new StringBuffer();
            for (Node node : results) {
                if (sb.length() > 0)
                    sb.append(", ");
                if (node.hasProperty(Constants.PROP_TYPE)) {
                    sb.append(layer.getGeometryEncoder().decodeGeometry(node).toString());
                } else {
                    sb.append(node.toString());
                }
                count++;
            }
            System.out.println("Found " + count + " results: " + sb);
            assertEquals("Count of geometries not correct", expected, count);
            tx.success();
        }
		return count;
	}

}
