package org.neo4j.gis.spatial;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.junit.Test;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.gis.spatial.rtree.RTreeMonitor;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.gis.spatial.rtree.TreeMonitor;
import org.neo4j.graphdb.*;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RTreeBulkInsertTest extends Neo4jTestCase {

    public void testDeleteRecursive() {
        int depth = 5;
        int width = 2;
        //Create nodes
        ArrayList<ArrayList<Node>> nodes = new ArrayList<>();
        try (Transaction tx = graphDb().beginTx()) {
            nodes.add(new ArrayList<>());
            nodes.get(0).add(graphDb().createNode());
            nodes.get(0).get(0).setProperty("name", "0-0");

            for (int i = 1; i < depth; i++) {
                ArrayList<Node> children = new ArrayList<Node>();
                nodes.add(children);
                for (Node parent : nodes.get(i - 1)) {
                    for (int j = 0; j < width; j++) {
                        Node node = graphDb().createNode();
                        node.setProperty("name", "" + i + "-" + j);
                        parent.createRelationshipTo(node, RTreeRelationshipTypes.RTREE_CHILD);
                        children.add(node);
                    }
                }
            }
            debugRest();
            //Disconact leafs
            ArrayList<Node> leaves = nodes.get(nodes.size() - 1);
            for (Node leaf : leaves) {
                leaf.getSingleRelationship(RTreeRelationshipTypes
                        .RTREE_CHILD, Direction.INCOMING).delete();

            }

            deleteRecursivelySubtree(nodes.get(0).get(0), null);
            System.out.println("Leaf");
            leaves = nodes.get(nodes.size() - 1);
            for (Node leaf : leaves) {
                System.out.println(leaf);
            }
            tx.success();
        }
        debugRest();
    }

    private void debugRest() {
        try (Transaction tx = graphDb().beginTx()) {
            Result result = graphDb().execute("MATCH (n) OPTIONAL MATCH (n)-[r]-() RETURN ID(n), n.name, count(r)");
            while (result.hasNext()) {

                System.out.println(result.next());
            }
            tx.success();
        }
    }

    private void deleteRecursivelySubtree(Node node, Relationship incoming) {
        for (Relationship relationship : node.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
            deleteRecursivelySubtree(relationship.getEndNode(), relationship);
        }
        if (incoming != null) {
            incoming.delete();
        }
//        Iterator<Relationship> itr = node.getRelationships().iterator();
//        while(itr.hasNext()){
//            itr.next().delete();
//        }
        System.out.println(node.getId());

        node.delete();
    }

    public void testSimpleRTreeInsert() {
        int width = 20;
        int blockSize = 10000;
        GraphDatabaseService db = graphDb();
        SpatialDatabaseService sdbs = new SpatialDatabaseService(graphDb());
        CoordinateReferenceSystem crs = DefaultEngineeringCRS.GENERIC_2D;
        EditableLayer layer = sdbs.getOrCreatePointLayer("Coordinates", "lon", "lat");
        try (Transaction tx = graphDb().beginTx()) {
            layer.setCoordinateReferenceSystem(crs);
            tx.success();
        }
        List<Node> nodes = new ArrayList<>();
        try (Transaction tx = spatialProcedures().db.beginTx()) {
            for (int i = 0; i < width; i++) {
                Node node = db.createNode();
                node.addLabel(Label.label("Coordinates"));
                node.setProperty("lat", i);
                node.setProperty("lon", 0);
                nodes.add(node);
                node.toString();
            }
            tx.success();
        }
//        java.util.Collections.shuffle( nodes,new Random( 1 ) );
        TreeMonitor monitor = new RTreeMonitor();
        layer = (EditableLayer) new SpatialDatabaseService(graphDb()).getLayer("Coordinates");
        layer.getIndex().addMonitor(monitor);
        long start = System.currentTimeMillis();

        List<Node> list1 = nodes.subList(0, nodes.size() / 2 + 8);
        List<Node> list2 = nodes.subList(list1.size(), nodes.size());
        System.out.println(list1.toString());
        System.out.println(list2.toString());
        try (Transaction tx = spatialProcedures().db.beginTx()) {
            layer.addAll(list1);

            tx.success();
        }
        debugIndexTree((RTreeIndex) layer.getIndex());
        //TODO add this part to the test
//        try (Transaction tx = spatialProcedures().db.beginTx()) {
//            layer.addAll(list2);
//            tx.success();
//        }

        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + (width * width) + " nodes to RTree in bulk");

//        queryRTree(layer);
//        debugTree(layer);
        debugIndexTree((RTreeIndex) layer.getIndex());

    }

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
        java.util.Collections.shuffle(nodes, new Random(8));
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
        CoordinateReferenceSystem crs = DefaultEngineeringCRS.GENERIC_2D;
        EditableLayer layer = sdbs.getOrCreatePointLayer("Coordinates", "lon", "lat");
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

    private void queryRTree(Layer layer, TreeMonitor monitor) {
        Coordinate min = new Coordinate(0.5, 0.5);
        Coordinate max = new Coordinate(0.52, 0.52);
        long count = 0;
        long start = System.currentTimeMillis();
        try (Transaction tx = spatialProcedures().db.beginTx()) {
            com.vividsolutions.jts.geom.Envelope envelope = new com.vividsolutions.jts.geom.Envelope(min, max);
            count = GeoPipeline.startWithinSearch(layer, layer.getGeometryFactory().toGeometry(envelope)).count();
            tx.success();
        }
        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to find " + count + " nodes in 4x4 block");
        int touched = monitor.getCaseCounts().get("Geometry Does NOT Match");
        int matched = monitor.getCaseCounts().get("Geometry Matches");
        System.out.println("Matched " + matched + "/" + touched + " touched nodes (" + (100.0 * matched / touched) + "%)");
//        assertEquals("Expected 361 nodes to be returned", 361, count);
    }

    public void testInsertManyNodes() throws FactoryException {
        int width = 100;
        int blockSize = 10000;
        List<Node> nodes = setup(width);
        TreeMonitor monitor = new RTreeMonitor();
        EditableLayer layer = (EditableLayer) new SpatialDatabaseService(graphDb()).getLayer("Coordinates");
        layer.getIndex().addMonitor(monitor);
        TimedLogger log = new TimedLogger("Inserting " + (width * width) + " nodes into RTree using solo insert",
                (width * width), 2000);
        long start = System.currentTimeMillis();
        for (int i = 0; i < width * width / blockSize; i++) {
            List<Node> slice = nodes.subList(i * blockSize, i * blockSize + blockSize);
            try (Transaction tx = spatialProcedures().db.beginTx()) {
                for (Node node : slice) {
                    layer.add(node);
                }
                System.out.println("Rebuilt " + monitor.getNbrRebuilt());
                System.out.println("Splits " + monitor.getNbrSplit());
                tx.success();
            }
            log.log("added to the tree", (i + 1) * blockSize);
        }
        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + (width * width) + " nodes to RTree in bulk");

        queryRTree(layer, monitor);
        debugTree(layer);
    }

    private void debugTree(Layer layer) {
        Node layerNode = layer.getLayerNode();
        String queryDepthAndGeometries =
                "MATCH (layer)-[:RTREE_ROOT]->(root) WHERE ID(layer)={layerNodeId} WITH root " +
                        "MATCH p = (root)-[:RTREE_CHILD*]->(child)-[:RTREE_REFERENCE]->(geometry) " +
                        "RETURN length(p) as depth, count(*) as geometries";

        String queryNumChildren = "MATCH (layer)-[:RTREE_ROOT]->(root) WHERE ID(layer)={layerNodeId} WITH root " +
                "MATCH p = (root)-[:RTREE_CHILD*]->(child) " +
                "WHERE exists((child)-[:RTREE_REFERENCE]->()) " +
                "RETURN length(p) as depth, count (*) as leaves";
        String queryChildrenPerParent = "MATCH (layer)-[:RTREE_ROOT]->(root) WHERE ID(layer)={layerNodeId} WITH root " +
                "MATCH p = (root)-[:RTREE_CHILD*0..]->(parent)-[:RTREE_CHILD]->(child) " +
                "WITH parent, count (*) as children RETURN avg(children) as childrenPerParent,min(children) as " +
                "MinChildrenPerParent,max(children) as MaxChildrenPerParent";
        String queryChildrenPerParent2 =
                "MATCH (layer)-[:RTREE_ROOT]->(root) WHERE ID(layer)={layerNodeId} WITH root " +
                        "MATCH p = (root)-[:RTREE_CHILD*0..]->(parent)-[:RTREE_CHILD]->(child) " +
                        "RETURN parent, length(p) as depth, count (*) as children";
        //TODO add more info about the nodes that have very few children. ie leafs with few children
        String queryChildrenPerParent3 =
                "MATCH (layer)-[:RTREE_ROOT]->(root) WHERE ID(layer)={layerNodeId} WITH root " +
                        "MATCH p = (root)-[:RTREE_CHILD*0..]->(parent)-[:RTREE_CHILD]->(child) " +
                        "RETURN parent, length(p) as depth, count (*) as children";
        Map<String, Object> params = Collections.singletonMap("layerNodeId", layerNode.getId());
        Result resultDepth = graphDb().execute(queryDepthAndGeometries, params);
        int balanced = 0;
        while (resultDepth.hasNext()) {
            balanced++;
            System.out.println(resultDepth.next().toString());
        }
        assertEquals(1, balanced);
        Result resultNumChildren = graphDb().execute(queryNumChildren, params);
        System.out.println(resultNumChildren.next());
        Result resultChildrenPerParent = graphDb().execute(queryChildrenPerParent, params);
        System.out.println(resultChildrenPerParent.next());

        Result resultChildrenPerParent2 = graphDb().execute(queryChildrenPerParent2, params);
        Integer[] histogram = new Integer[11];
        Arrays.fill(histogram, 0);
        while (resultChildrenPerParent2.hasNext()) {
            Map<String, Object> result = resultChildrenPerParent2.next();
            long children = (long) result.get("children");
            if (children < 20) {
                System.out.println("Underfilled index node: " + result);
            }
            histogram[(int) children / 10]++;
        }
        for (int i = 0; i < histogram.length; i++) {
            System.out.println("[" + (i * 10) + ".." + ((i + 1) * 10) + "): " + histogram[i]);
        }
    }

    public void testInsertManyNodesInBulk() throws FactoryException {
        int width = 1000;
        int blockSize = 10000;
//        int blockSize = 1000;
        List<Node> nodes = setup(width);

        EditableLayer layer = (EditableLayer) new SpatialDatabaseService(graphDb()).getLayer("Coordinates");
        RTreeMonitor monitor = new RTreeMonitor();
        layer.getIndex().addMonitor(monitor);
        TimedLogger log = new TimedLogger("Inserting " + (width * width) + " nodes into RTree using bulk insert",
                (width * width), 1000);
        long start = System.currentTimeMillis();
        for (int i = 0; i < width * width / blockSize; i++) {
            List<Node> slice = nodes.subList(i * blockSize, i * blockSize + blockSize);
            try (Transaction tx = spatialProcedures().db.beginTx()) {

                SpatialProcedures s = spatialProcedures();
                layer.addAll(slice);
                tx.success();
            }
//            catch ( Exception e ){
//                Throwable t = e;
//                do {
//                    System.out.println(t.getMessage());
//                    t.printStackTrace(System.out);
//                    t=t.getCause();
//                }while(t!=null);
//            }
            System.out.println("Rebuilt " + monitor.getNbrRebuilt());
            System.out.println("Splits " + monitor.getNbrSplit());
            System.out.println("Cases " + monitor.getCaseCounts());
            log.log("added to the tree", (i + 1) * blockSize);
        }
        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + (width * width) + " nodes to RTree in bulk");

        queryRTree(layer, monitor);
        debugTree(layer);
//        debugIndexTree((RTreeIndex) layer.getIndex());
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
                int rate = (int) (number / seconds);
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
    public void testTreeBuildFromScratch() throws Exception {
//        File dbPath = new File("target/var/BulkTest2");
//        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        GraphDatabaseService db = graphDb();

        SpatialDatabaseService sdbs = new SpatialDatabaseService(db);
        try {
            GeometryEncoder encoder = new SimplePointEncoder();

            Method buildRTreeFromScratch = RTreeIndex.class.getDeclaredMethod("buildRtreeFromScratch", Node.class, List.class, double.class, int.class);
            buildRTreeFromScratch.setAccessible(true);

            Method expectedHeight = RTreeIndex.class.getDeclaredMethod("expectedHeight", double.class, int.class);
            expectedHeight.setAccessible(true);

            Random random = new Random();
            random.setSeed(42);

            List<Integer> range = IntStream.rangeClosed(1, 300).boxed().collect(Collectors.toList());
            //test over the transiton from two to three deep trees
            range.addAll(IntStream.rangeClosed(4700, 5000).boxed().collect(Collectors.toList()));

            for (int i : range) {
                System.out.println("Building a Tree with " + Integer.toString(i) + " nodes");
                try (Transaction tx = db.beginTx()) {

                    RTreeIndex rtree = new RTreeIndex(
                            sdbs.getDatabase(),
                            sdbs.getDatabase().createNode(),
                            encoder
                    );
                    List<Node> coords = new ArrayList<>(i);
                    for (int j = 0; j < i; j++) {
                        Node n = db.createNode(Label.label("Coordinate"));
                        n.setProperty(SimplePointEncoder.DEFAULT_X, random.nextDouble() * 90.0);
                        n.setProperty(SimplePointEncoder.DEFAULT_Y, random.nextDouble() * 90.0);
                        Geometry geometry = encoder.decodeGeometry(n);
                        // add BBOX to Node if it's missing
                        encoder.encodeGeometry(geometry, n);
                        coords.add(n);
                        //                   layer.add(n);
                    }

                    buildRTreeFromScratch.invoke(rtree, rtree.getIndexRoot(), coords, 0.7, 4);
                    RTreeTestUtils testUtils = new RTreeTestUtils(rtree);

                    Map<Long, Long> results = testUtils.get_height_map(db, rtree.getIndexRoot());
                    assertEquals(1, results.size());
                    assertEquals((int) expectedHeight.invoke(rtree, 0.7, coords.size()), results.keySet().iterator().next().intValue());
                    assertTrue(results.values().iterator().next().intValue() == coords.size());
                    tx.success();
                }
            }
        } finally {
            sdbs.getDatabase().shutdown();
//            FileUtils.deleteDirectory(dbPath);
        }
    }

    @Test
    public void testRTreeBulkInsertion() throws Exception {
        // Use these two lines if you want to examine the output.
//        File dbPath = new File("target/var/BulkTest");
//        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        GraphDatabaseService db = graphDb();

        SpatialDatabaseService sdbs = new SpatialDatabaseService(db);
        try {
            int N = 10000;
            int Q = 40;
            Random random = new Random();
            random.setSeed(42);
            //        random.setSeed(42);
            // leads to: Caused by: org.neo4j.kernel.impl.store.InvalidRecordException: Node[142794,used=false,rel=-1,prop=-1,labels=Inline(0x0:[]),light,secondaryUnitId=-1] not in use

            long totalTimeStart = System.currentTimeMillis();
            for (int j = 1; j < Q + 1; j++) {
                System.out.println("BulkLoadingTestRun " + j);
                try (Transaction tx = db.beginTx()) {


                    EditableLayer layer = sdbs.getOrCreatePointLayer("BulkLoader", "lat", "lon");
                    List<Node> coords = new ArrayList<>(N);
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

                    RTreeIndex rtree = (RTreeIndex) layer.getIndex();
                    RTreeTestUtils utils = new RTreeTestUtils(rtree);
                    assertTrue(utils.check_balance(db, rtree.getIndexRoot()));

                    tx.success();
                }
            }
            System.out.println("Total Time for " + (N * Q) + " Nodes in " + Q + " Batches of " + N + " is: ");
            System.out.println(((System.currentTimeMillis() - totalTimeStart) / 1000) + " seconds");

            try (Transaction tx = db.beginTx()) {
                String cypher = "MATCH ()-[:RTREE_ROOT]->(n)\n" +
                        "MATCH (n)-[:RTREE_CHILD]->(m)-[:RTREE_CHILD]->(p)-[:RTREE_CHILD]->(s)-[:RTREE_REFERENCE]->(q)\n" +
                        "RETURN COUNT(q) as count";
                Result result = db.execute(cypher);
                System.out.println(result.columns().toString());
                long count = result.<Long>columnAs("count").next();
                assertEquals(N * Q, count);
                tx.success();
            }

            try (Transaction tx = db.beginTx()) {
                Layer layer = sdbs.getLayer("BulkLoader");
                RTreeIndex rtree = (RTreeIndex) layer.getIndex();

                Node root = rtree.getIndexRoot();
                List<Node> children = new ArrayList<>(100);
                for (Relationship r : root.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
                    children.add(r.getEndNode());
                }
                RTreeTestUtils utils = new RTreeTestUtils(rtree);
                double root_overlap = utils.calculate_overlap(root);
                assertTrue(root_overlap < 0.01); //less than one percent
                System.out.println("********* Bulk Overlap Percentage" + Double.toString(root_overlap));

                double average_child_overlap = children.stream().mapToDouble(utils::calculate_overlap).average().getAsDouble();
                assertTrue(average_child_overlap < 0.02);
                System.out.println("*********** Bulk Average Child Overlap Percentage" + Double.toString(average_child_overlap));
                tx.success();


            }
        } finally {
            sdbs.getDatabase().shutdown();
//            FileUtils.deleteDirectory(dbPath);
        }
    }
}
