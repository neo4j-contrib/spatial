package org.neo4j.gis.spatial;

import com.vividsolutions.jts.geom.Coordinate;
import org.geotools.geometry.Envelope2D;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.crs.DefaultProjectedCRS;
import org.junit.Test;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.*;
import org.opengis.geometry.Envelope;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class RTreeBulkInsertTest extends Neo4jTestCase {

    private List<Node> populateSquareTestData(int width) {
        GraphDatabaseService db = graphDb();
        ArrayList<Node> nodes = new ArrayList<>(width * width);
        for (int i = 0; i < width; i++) {
            try (Transaction tx = db.beginTx()) {
                for (int j = 0; j < width; j++) {
                    Node node = db.createNode();
                    node.addLabel(Label.label("Coordinates"));
                    node.setProperty("lat", ((double) i / (double) width));
                    node.setProperty("lon", ((double) j / (double) width));
                    nodes.add(node);
                }
                tx.success();
            }
        }
        return nodes;
    }

    private void searchForPos(int numNodes, GraphDatabaseService db) {
        System.out.println("Searching with spatial.withinDistance");
        long start = System.currentTimeMillis();
        try (Transaction tx = db.beginTx()) { // 'points',{longitude:15.0,latitude:60.0},100
            Result result = db.execute("CALL spatial.withinDistance('Coordinates',{longitude:0.5, latitude:0.5},1000.0) yield node as malmo");
            int i = 0;
            ResourceIterator thing = result.columnAs("malmo");
            while (thing.hasNext()) {
                assertNotNull(thing.next());
                i++;
            }
            //assertEquals(i, numNodes);
            tx.success();
        }
        System.out.println("\t" + (System.currentTimeMillis() - start) + "ms");

    }

    private List<Node> setup(int width) {
        long start = System.currentTimeMillis();
        List<Node> nodes = populateSquareTestData(width);
        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to create " + (width * width) + " nodes");
        SpatialDatabaseService sdbs = new SpatialDatabaseService(graphDb());
        CoordinateReferenceSystem crs = DefaultGeographicCRS.WGS84;
        EditableLayer layer = sdbs.getOrCreatePointLayer("Coordinates", "lat", "lon");
        try (Transaction tx = graphDb().beginTx()) {
            layer.setCoordinateReferenceSystem(crs);
            tx.success();
        }
        return nodes;
    }

    private SpatialProcedures spatialProcedures() {
        SpatialProcedures spatialProcedures = new SpatialProcedures();
        spatialProcedures.db = graphDb();
        return spatialProcedures;
    }

    private void queryRTree(Layer layer) {
        Coordinate min = new Coordinate(0.5, 0.5);
        Coordinate max = new Coordinate(0.52, 0.52);
        long count = 0;
        long start = System.currentTimeMillis();
        try (Transaction tx = spatialProcedures().db.beginTx()) {
            Stream<SpatialProcedures.NodeResult> results = spatialProcedures().findGeometriesInBBox(layer.getName(), min, max);
            count = results.count();
            tx.success();
        }
        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to find " + count + " nodes in 4x4 block");
        assertEquals("Expected 9 nodes to be returned", 9, count);
    }

    public void testInsertManyNodes() throws FactoryException {
        int width = 200;
        int blockSize = 10000;
        List<Node> nodes = setup(width);
        Layer layer = new SpatialDatabaseService(graphDb()).getLayer("Coordinates");

        TimedLogger log = new TimedLogger("Inserting " + (width * width) + " nodes into RTree using bulk insert", (width * width), 2000);
        long start = System.currentTimeMillis();
        for (int i = 0; i < width * width / blockSize; i++) {
            List<Node> slice = nodes.subList(i * blockSize, i * blockSize + blockSize);
            try (Transaction tx = spatialProcedures().db.beginTx()) {
                for (Node node : slice) {
                    spatialProcedures().addNodeToLayer(layer.getName(), node);
                }
                tx.success();
            }
            log.log("added to the tree", (i + 1) * blockSize);
        }
        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + (width * width) + " nodes to RTree in bulk");

        queryRTree(layer);
    }

    public void testInsertManyNodesInBulk() throws FactoryException {
        int width = 200;
        int blockSize = 10000;
        List<Node> nodes = setup(width);
        Layer layer = new SpatialDatabaseService(graphDb()).getLayer("Coordinates");

        TimedLogger log = new TimedLogger("Inserting " + (width * width) + " nodes into RTree using bulk insert", (width * width), 2000);
        long start = System.currentTimeMillis();
        for (int i = 0; i < width * width / blockSize; i++) {
            List<Node> slice = nodes.subList(i * blockSize, i * blockSize + blockSize);
            try (Transaction tx = spatialProcedures().db.beginTx()) {
                spatialProcedures().addNodesToLayer(layer.getName(), slice);
                tx.success();
            }
            log.log("added to the tree", (i + 1) * blockSize);
        }
        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + (width * width) + " nodes to RTree in bulk");

        queryRTree(layer);
    }

    class TimedLogger {
        String title;
        long count;
        long gap;
        long start;
        long previous;

        public TimedLogger(String title, long count, long gap) {
            this.title = title;
            this.count = count;
            this.gap = gap;
            this.start = System.currentTimeMillis();
            this.previous = this.start;
            System.out.println(title);
        }

        public void log(String line, long number) {
            long current = System.currentTimeMillis();
            if (current - previous > gap) {
                double percentage = 100.0 * number / count;
                double seconds = (current - start) / 1000.0;
                int rate = (int)(number / seconds);
                System.out.println("\t" + ((int) percentage) + "%\t" + seconds + "s\t" + rate + "n/s:\t" + line);
                previous = current;
            }
        }
    }

    @Test
    public void testIndexAccessAfterBulkInsertion() throws Exception {
        // Use these two lines if you want to examine the output.
//        File dbPath = new File("target/var/BulkTest");
//        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath.getCanonicalPath());
        GraphDatabaseService db = graphDb();
        SpatialDatabaseService sdbs = new SpatialDatabaseService(db);
        EditableLayer layer = sdbs.getOrCreatePointLayer("Coordinates", "lat", "lon");

        final long numNodes = 100000;
        Random rand = new Random();

        System.out.println("Bulk inserting " + numNodes + " nodes");
        long start = System.currentTimeMillis();
        try (Transaction tx = db.beginTx()) {
            List<Node> coordinateNodes = new ArrayList<>();
            for (int i = 0; i < numNodes; i++) {
                Node node = db.createNode();
                node.addLabel(Label.label("Coordinates"));
                node.setProperty("lat", rand.nextDouble());
                node.setProperty("lon", rand.nextDouble());
                coordinateNodes.add(node);
            }
            layer.addAll(coordinateNodes);
            tx.success();
        }
        System.out.println("\t" + (System.currentTimeMillis() - start) + "ms");

        System.out.println("Searching with spatial.withinDistance");
        start = System.currentTimeMillis();
        try (Transaction tx = db.beginTx()) { // 'points',{longitude:15.0,latitude:60.0},100
            Result result = db.execute("CALL spatial.withinDistance('Coordinates',{longitude:0.5, latitude:0.5},1000.0) yield node as malmo");
            int i = 0;
            ResourceIterator thing = result.columnAs("malmo");
            while (thing.hasNext()) {
                assertNotNull(thing.next());
                i++;
            }
            assertEquals(i, numNodes);
            tx.success();
        }
        System.out.println("\t" + (System.currentTimeMillis() - start) + "ms");

        System.out.println("Searching with spatial.withinDistance and Cypher count");
        start = System.currentTimeMillis();
        try (Transaction tx = db.beginTx()) {
            String cypher = "CALL spatial.withinDistance('Coordinates',{longitude:0.5, latitude:0.5},1000.0) yield node\n" +
                    "RETURN COUNT(node) as count";
            Result result = db.execute(cypher);
//           System.out.println(result.columns().toString());
            Object obj = result.columnAs("count").next();
            assertTrue(obj instanceof Long);
            assertTrue(((Long) obj).equals(numNodes));
            tx.success();
        }
        System.out.println("\t" + (System.currentTimeMillis() - start) + "ms");

        System.out.println("Searching with pure Cypher");
        start = System.currentTimeMillis();
        try (Transaction tx = db.beginTx()) {
            String cypher = "MATCH ()-[:RTREE_ROOT]->(n)\n" +
                    "MATCH (n)-[:RTREE_CHILD*]->(m)-[:RTREE_REFERENCE]->(p)\n" +
                    "RETURN COUNT(p) as count";
            Result result = db.execute(cypher);
//           System.out.println(result.columns().toString());
            Object obj = result.columnAs("count").next();
            assertTrue(obj instanceof Long);
            assertTrue(((Long) obj).equals(numNodes));
            tx.success();
        }
        System.out.println("\t" + (System.currentTimeMillis() - start) + "ms");

        db.shutdown();


    }

    @Test
    public void testRTreeBulkInsertion() throws Exception {
        // Use these two lines if you want to examine the output.
//        File dbPath = new File("target/var/BulkTest");
//        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath.getCanonicalPath());
        GraphDatabaseService db = graphDb();

        SpatialDatabaseService sdbs = new SpatialDatabaseService(db);
        int N = 20000;
        int Q = 10;
        Random random = new Random();
        random.setSeed(41);
//        random.setSeed(42);
// leads to: Caused by: org.neo4j.kernel.impl.store.InvalidRecordException: Node[142794,used=false,rel=-1,prop=-1,labels=Inline(0x0:[]),light,secondaryUnitId=-1] not in use

        long totalTimeStart = System.currentTimeMillis();
        for (int j = 0; j < Q; j++) {
            System.out.println("BulkLoadingTestRun " + j);
            try (Transaction tx = db.beginTx()) {
                List<Node> coords = new ArrayList<>(N);

                EditableLayer layer = sdbs.getOrCreatePointLayer("BulkLoader", "lat", "lon");

                for (int i = 0; i < N; i++) {
                    Node n = db.createNode(Label.label("Coordinate"));
                    n.setProperty("lat", random.nextDouble() * 90.0);
                    n.setProperty("lon", random.nextDouble() * 90.0);
                    coords.add(n);
                    //                   layer.add(n);
                }
                long time = System.currentTimeMillis();

                layer.addAll(coords);
                System.out.println("********************** time taken to load " + N + " records: " + (System.currentTimeMillis() - time) + "ms");
                tx.success();
            }
        }
        System.out.println("Total Time for " + (N * Q) + " Nodes in " + Q + " Batches of " + N + " is: ");
        System.out.println(((System.currentTimeMillis() - totalTimeStart) / 1000) + " seconds");

        try (Transaction tx = db.beginTx()) {
            String cypher = "MATCH ()-[:RTREE_ROOT]->(n)\n" +
                    "MATCH (n)-[:RTREE_CHILD]->(m)-[:RTREE_CHILD]->(p)-[:RTREE_REFERENCE]->(q)\n" +
                    "RETURN COUNT(q) as count";
            Result result = db.execute(cypher);
            System.out.println(result.columns().toString());
            long count = result.<Long>columnAs("count").next();
            assertEquals(N * Q, count);
            tx.success();
        }
    }
}
