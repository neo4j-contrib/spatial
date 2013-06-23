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
/**
 ' * Copyright (c) 2002-2011 "Neo Technology,"
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

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.operation.distance.DistanceOp;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.SyntaxException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.gis.spatial.indexprovider.LayerNodeIndex;
import org.neo4j.gis.spatial.indexprovider.SpatialIndexProvider;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.test.ImpermanentGraphDatabase;

import javax.script.*;
import java.util.*;

import static org.junit.Assert.*;

public class IndexProviderTest {

    private ImpermanentGraphDatabase db;

    @Before
    public void setup() throws Exception {
        db = new ImpermanentGraphDatabase();
    }

    /**
     * Test that we can create and retrieve indexes
     */
    @Test
    public void testLoadIndex() {
        Map<String, String> config = SpatialIndexProvider.SIMPLE_POINT_CONFIG;
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes("layer1", config);
        assertNotNull(index);

        //Load the an existing index again
        index = indexMan.forNodes("layer1", config);
        assertNotNull(index);

        //Try a different config
        Map<String, String> config2 = SpatialIndexProvider.SIMPLE_WKT_CONFIG;
        index = indexMan.forNodes("layer2", config2);
        assertNotNull(index);

        //Load the index again
        index = indexMan.forNodes("layer2", config2);
        assertNotNull(index);

        //Try loading the same index with a different config
        boolean exceptionThrown = false;
        try {
            index = indexMan.forNodes("layer2", config);
        } catch (IllegalArgumentException iae) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        config = SpatialIndexProvider.SIMPLE_WKT_CONFIG;
        index = indexMan.forNodes("layer2", config);
        assertNotNull(index);

        config = SpatialIndexProvider.SIMPLE_WKB_CONFIG;
        index = indexMan.forNodes("layer3", config);
        assertNotNull(index);
    }

    /**
     * Test that invalid configurations do not leave configurations in the index manager
     */
    @Test
    @Ignore
    //TODO: fix this, issue #71
    public void testInvalidConfig() {
        // An invalid configuration
        Map<String, String> config =
                Collections.unmodifiableMap(MapUtil.stringMap(
                        IndexManager.PROVIDER, SpatialIndexProvider.SERVICE_NAME, SpatialIndexProvider.GEOMETRY_TYPE, LayerNodeIndex.POINT_PARAMETER));
        // Use transaction just in case it matters (not that I can tell)
        Transaction tx = db.beginTx();
        System.out.println("testInvalidConfig: Begun transaction");

        // Try to create the index, ignore IllegalArgumentException to continue
        IndexManager indexMan = db.index();
        try {
            Index<Node> index = indexMan.forNodes("layer1", config);
            System.out.println("testInvalidConfig: invalid index requested, did not throw exception.");
            tx.success();    // Won't happen currently
        } catch (IllegalArgumentException e) {
            // Bail out
            tx.failure();
            System.out.println("testInvalidConfig: invalid index creation failed, good, let the tx rollback");
        }
        tx.finish();
        System.out.println("testInvalidConfig: tx done.");
        // Assert index isn't referenced in the manager
        assertFalse("Index should not exist", indexMan.existsForNodes("layer1"));
    }

    /*
    * Test the deletion of indexes
    */
    @Test
    @Ignore
    //TODO: fix this, issue #70
    public void testDeleteIndex() {
        // Create an index
        Map<String, String> config = SpatialIndexProvider.SIMPLE_POINT_CONFIG;
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes("layer1", config);
        assertNotNull(index);
        // Request deletion
        index.delete();
        // Assert deletion
        assertFalse(indexMan.existsForNodes("layer1"));
        // TODO: we should probably check the internal structure was also cleanly deleted 
    }

    @Test
    public void testNodeIndex() throws SyntaxException, Exception {
        Map<String, String> config = SpatialIndexProvider.SIMPLE_POINT_CONFIG;
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes("layer1", config);
        assertNotNull(index);
        Transaction tx = db.beginTx();
        ExecutionEngine engine = new ExecutionEngine(db);
        ExecutionResult result1 = engine.execute("create (malmo{name:'Malmö',lat:56.2, lon:15.3})-[:TRAIN]->(stockholm{name:'Stockholm',lat:59.3,lon:18.0}) return malmo");
        Node malmo = (Node) result1.iterator().next().get("malmo");
        index.add(malmo, "dummy", "value");
        tx.success();
        tx.finish();
        Map<String, Object> params = new HashMap<String, Object>();
        //within Envelope
        params.put(LayerNodeIndex.ENVELOPE_PARAMETER, new Double[]{15.0,
                16.0, 56.0, 57.0});
        IndexHits<Node> hits = index.query(LayerNodeIndex.WITHIN_QUERY, params);
        assertTrue(hits.hasNext());

        // within BBOX
        hits = index.query(LayerNodeIndex.BBOX_QUERY,
                "[15.0, 16.0, 56.0, 57.0]");
        assertTrue(hits.hasNext());

        //within any WKT geometry
        hits = index.query(LayerNodeIndex.WITHIN_WKT_GEOMETRY_QUERY,
                "POLYGON ((15 56, 15 57, 16 57, 16 56, 15 56))");
        assertTrue(hits.hasNext());
        //polygon with hole, excluding n1
        hits = index.query(LayerNodeIndex.WITHIN_WKT_GEOMETRY_QUERY,
                "POLYGON ((15 56, 15 57, 16 57, 16 56, 15 56)," +
                        "(15.1 56.1, 15.1 56.3, 15.4 56.3, 15.4 56.1, 15.1 56.1))");
        assertFalse(hits.hasNext());


        //within distance
        params.clear();
        params.put(LayerNodeIndex.POINT_PARAMETER, new Double[]{56.5, 15.5});
        params.put(LayerNodeIndex.DISTANCE_IN_KM_PARAMETER, 100.0);
        hits = index.query(LayerNodeIndex.WITHIN_DISTANCE_QUERY,
                params);
        assertTrue(hits.hasNext());

        ExecutionResult result = engine.execute("start malmo=node:layer1('bbox:[15.0, 16.0, 56.0, 57.0]') match p=malmo--other return malmo, other");
        assertTrue(result.iterator().hasNext());
        result = engine.execute("start malmo=node:layer1('withinDistance:[56.0, 15.0,1000.0]') match p=malmo--other return malmo, other");
        assertTrue(result.iterator().hasNext());
        System.out.println(result.dumpToString());
// test Gremlin
        ScriptEngine gremlinEngine = new ScriptEngineManager().getEngineByName("gremlin-groovy");
        final Bindings bindings = new SimpleBindings();
        final Neo4jGraph graph = new Neo4jGraph(db, false);
        bindings.put("g", graph);
        gremlinEngine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
//        assertEquals(
//                2L,
//                gremlinEngine.eval( "g.idx('layer1')[[bbox:'[15.0, 16.0, 56.0, 57.0]']].in().count()" ) );

        // Rather than counting the incoming vertices, we just count the nodes
        // of which there are one, with no incoming edges
        assertEquals(
                1L,
                gremlinEngine.eval("g.idx('layer1')[[bbox:'[15.0, 16.0, 56.0, 57.0]']].out('TRAIN').count()"));

    }

    @Test
    public void testWithinDistanceIndex() {
        Map<String, String> config = SpatialIndexProvider.SIMPLE_WKT_CONFIG;
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes("layer2", config);
        Transaction tx = db.beginTx();
        Node batman = db.createNode();
        String wktPoint = "POINT(41.14 37.88 )";
        batman.setProperty("wkt", wktPoint);
        String batman1 = "batman";
        batman.setProperty("name", batman1);
        index.add(batman, "dummy", "value");
        Map<String, Object> params = new HashMap<String, Object>();
        Double[] point = {37.87, 41.13};
        params.put(LayerNodeIndex.POINT_PARAMETER,
                point);
        params.put(LayerNodeIndex.DISTANCE_IN_KM_PARAMETER, 2.0);
        IndexHits<Node> hits = index.query(
                LayerNodeIndex.WITHIN_DISTANCE_QUERY, params);
        tx.success();
        tx.finish();
        Node node = hits.getSingle();
        assertEquals(node.getId(), batman.getId());
        assertEquals(batman1,node.getProperty("name"));
        assertEquals(1.41f, hits.currentScore(),0.01f);

    }
    
    @Test
    public void testDistance() throws ParseException {
        WKTReader wktRdr = new WKTReader();
        Geometry A = wktRdr.read("POINT(40.00 30.00 )");
        Geometry B = wktRdr.read("POINT(40.00 40.00 )");
        DistanceOp distOp = new DistanceOp(A, B);
        assertEquals(10.0, distOp.distance(), 0);
    }

    @Test
    public void testWithinDistanceIndexViaCypher() {
        Map<String, String> config = SpatialIndexProvider.SIMPLE_WKT_CONFIG;
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes("layer3", config);
        Transaction tx = db.beginTx();
        Node batman = db.createNode();
        batman.setProperty("wkt", "POINT(44.44 33.33)");
        batman.setProperty("name", "robin");
        index.add(batman, "dummy", "value");

        ExecutionEngine engine = new ExecutionEngine(db);
        ExecutionResult result = engine.execute("start n=node:layer3('withinDistance:[33.32, 44.44, 5.0]') return n");

        Iterator<Object> rows = result.columnAs("n");

        assertTrue(rows.hasNext());

        NodeProxy row = (org.neo4j.kernel.impl.core.NodeProxy) rows.next();
        assertEquals("robin", row.getProperty("name"));
        assertEquals("POINT(44.44 33.33)", row.getProperty("wkt"));
    }

    /**
     * Test the performance of LayerNodeIndex.add()
     * Insert up to 100K nodes into the database, and into the index, randomly distributed over [-80,+80][-170+170]
     * Calculate speed over 100-node groups, fail if speed falls under 50 adds/second (typical max speed here 500 adds/second)
     */
    @Test
    public void testAddPerformance() {
        Map<String, String> config = SpatialIndexProvider.SIMPLE_POINT_CONFIG;
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes("pointslayer", config);

        Transaction tx = db.beginTx();
        try {
            Random r = new Random();
            final int stepping = 1000;
            long start = System.currentTimeMillis();
            long previous = start;
            for (int i = 1; i <= 100000; i++) {
                Node newnode = db.createNode();
                newnode.setProperty("lat", (double) r.nextDouble() * 160 - 80);
                newnode.setProperty("lon", (double) r.nextDouble() * 340 - 170);

                index.add(newnode, "dummy", "value");

                if (i % stepping == 0) {
                    long now = System.currentTimeMillis();
                    long duration = now - start;
                    long stepDuration = now - previous;
                    double speed = stepping / (stepDuration / 1000.0);
                    double linearity = (double) stepDuration / i;
                    System.out.println("testAddPerformance(): " + stepping + " nodes added in " + stepDuration + "ms, total " + i + " in " + duration + "ms, speed: " + speed + " adds per second, " + linearity + " ms per step per node in index");
                    final double targetSpeed = 50.0;    // Quite conservative, max speed here 500 adds per second
                    assertTrue("add is too slow at size:" + i + " (" + speed + " adds per second <= " + targetSpeed + ")", speed > targetSpeed);

                    previous = now;

                    // commit transaction
                    tx.success();
                    tx.finish();

                    tx = db.beginTx();
                }
            }
            tx.success();
        } finally {
            System.out.println("testAddPerformance() finished");
            tx.finish();
        }
    }

    @Test
    public void testUpdate() {
        Map<String, String> config = SpatialIndexProvider.SIMPLE_POINT_CONFIG;
        IndexManager indexMan = db.index();
        Index<Node> index = indexMan.forNodes("pointslayer", config);

        Transaction tx = db.beginTx();
        Node n1 = db.createNode();
        n1.setProperty("lat", (double) 56.2);
        n1.setProperty("lon", (double) 15.3);
        index.add(n1, "dummy", "value");

        Map<String, Object> params = new HashMap<String, Object>();
        params.put(LayerNodeIndex.POINT_PARAMETER, new Double[]{56.2, 15.3});
        params.put(LayerNodeIndex.DISTANCE_IN_KM_PARAMETER, 0.0001);
        IndexHits<Node> hits = index.query(LayerNodeIndex.WITHIN_DISTANCE_QUERY, params);
        assertTrue(hits.hasNext());

        n1.setProperty("lat", (double) 46.2);
        n1.setProperty("lon", (double) 25.3);

        // update
        index.add(n1, "dummy", "value");

        tx.success();
        tx.finish();

        params.put(LayerNodeIndex.POINT_PARAMETER, new Double[]{46.2, 25.3});
        params.put(LayerNodeIndex.DISTANCE_IN_KM_PARAMETER, 0.0001);
        hits = index.query(LayerNodeIndex.WITHIN_DISTANCE_QUERY, params);
        assertTrue(hits.hasNext());

        hits.next();

        // make sure there's only one node
        assertFalse(hits.hasNext());

    }
}
