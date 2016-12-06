package org.neo4j.gis.spatial;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.io.FileUtils;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.junit.*;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.gis.spatial.rtree.*;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.MapUtil.map;

public class RTreeBulkInsertTest {

    private GraphDatabaseService db;
    private final File storeDir = new File("target/store").getAbsoluteFile();

    @Before
    public void before() throws IOException {
        restart();
    }

    @After
    public void after() throws IOException {
        doCleanShutdown();
    }

    @Ignore
    public void shouldDeleteRecursiveTree() {
        int depth = 5;
        int width = 2;
        //Create nodes
        ArrayList<ArrayList<Node>> nodes = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            nodes.add(new ArrayList<>());
            nodes.get(0).add(db.createNode());
            nodes.get(0).get(0).setProperty("name", "0-0");

            for (int i = 1; i < depth; i++) {
                ArrayList<Node> children = new ArrayList<>();
                nodes.add(children);
                for (Node parent : nodes.get(i - 1)) {
                    for (int j = 0; j < width; j++) {
                        Node node = db.createNode();
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
            nodes.get(nodes.size() - 1).forEach(System.out::println);
            tx.success();
        }
        debugRest();
    }

    private void debugRest() {
        try (Transaction tx = db.beginTx()) {
            Result result = db.execute("MATCH (n) OPTIONAL MATCH (n)-[r]-() RETURN ID(n), n.name, count(r)");
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

    @Ignore
    public void shouldInsertSimpleRTree() {
        int width = 20;
        int blockSize = 10000;
        SpatialDatabaseService sdbs = new SpatialDatabaseService(db);
        CoordinateReferenceSystem crs = DefaultEngineeringCRS.GENERIC_2D;
        EditableLayer layer = sdbs.getOrCreatePointLayer("Coordinates", "lon", "lat");
        try (Transaction tx = db.beginTx()) {
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
        layer = (EditableLayer) new SpatialDatabaseService(db).getLayer("Coordinates");
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
        Neo4jTestUtils.debugIndexTree(db, (RTreeIndex) layer.getIndex());
        //TODO add this part to the test
//        try (Transaction tx = spatialProcedures().db.beginTx()) {
//            layer.addAll(list2);
//            tx.success();
//        }

        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + (width * width) + " nodes to RTree in bulk");

//        queryRTree(layer);
//        verifyTreeStructure(layer);
        Neo4jTestUtils.debugIndexTree(db, (RTreeIndex) layer.getIndex());

    }

    private static class RTreeTestConfig {
        String name;
        int width;
        Coordinate searchMin;
        Coordinate searchMax;
        long totalCount;
        long expectedCount;

        public RTreeTestConfig(String name, int width, Coordinate searchMin, Coordinate searchMax, long expectedCount) {
            this.name = name;
            this.width = width;
            this.searchMin = searchMin;
            this.searchMax = searchMax;
            this.expectedCount = expectedCount;
            this.totalCount = width * width;
        }
    }

    private static final Map<String, RTreeTestConfig> testConfigs = new HashMap<String, RTreeTestConfig>();

    static {
        Coordinate searchMin = new Coordinate(0.5, 0.5);
        Coordinate searchMax = new Coordinate(0.52, 0.52);
        addTestConfig(new RTreeTestConfig("very_small", 100, searchMin, searchMax, 1));
        addTestConfig(new RTreeTestConfig("small", 250, searchMin, searchMax, 16));
        addTestConfig(new RTreeTestConfig("medium", 500, searchMin, searchMax, 81));
        addTestConfig(new RTreeTestConfig("large", 750, searchMin, searchMax, 196));
    }

    static void addTestConfig(RTreeTestConfig config) {
        testConfigs.put(config.name, config);
    }

    /*
     * Very small model 100*100 nodes
     */

    @Test
    public void shouldInsertManyNodesIndividuallyWithQuadraticSplit_very_small() throws FactoryException, IOException {
        insertManyNodesIndividually(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("very_small"));
    }

    @Test
    public void shouldInsertManyNodesIndividuallyGreenesSplit_very_small() throws FactoryException, IOException {
        insertManyNodesIndividually(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("very_small"));
    }

    @Test
    public void shouldInsertManyNodesInBulkWithQuadraticSplit_very_small() throws FactoryException, IOException {
        insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("very_small"));
    }

    @Test
    public void shouldInsertManyNodesInBulkWithGreenesSplit_very_small() throws FactoryException, IOException {
        insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("very_small"));
    }

    /*
     * Small model 250*250 nodes
     */

    @Test
    public void shouldInsertManyNodesIndividuallyWithQuadraticSplit_small() throws FactoryException, IOException {
        insertManyNodesIndividually(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("small"));
    }

    @Test
    public void shouldInsertManyNodesIndividuallyGreenesSplit_small() throws FactoryException, IOException {
        insertManyNodesIndividually(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("small"));
    }

    @Test
    public void shouldInsertManyNodesInBulkWithQuadraticSplit_small() throws FactoryException, IOException {
        insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("small"));
    }

    @Test
    public void shouldInsertManyNodesInBulkWithGreenesSplit_small() throws FactoryException, IOException {
        insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("small"));
    }
    /*
     * Small model 250*250 nodes (shallow tree)
     */

    @Test
    public void shouldInsertManyNodesIndividuallyWithQuadraticSplit_small_100() throws FactoryException, IOException {
        insertManyNodesIndividually(RTreeIndex.QUADRATIC_SPLIT, 5000, 100, testConfigs.get("small"));
    }

    @Test
    public void shouldInsertManyNodesIndividuallyGreenesSplit_small_100() throws FactoryException, IOException {
        insertManyNodesIndividually(RTreeIndex.GREENES_SPLIT, 5000, 100, testConfigs.get("small"));
    }

    @Test
    public void shouldInsertManyNodesInBulkWithQuadraticSplit_small_100() throws FactoryException, IOException {
        insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 100, testConfigs.get("small"));
    }

    @Test
    public void shouldInsertManyNodesInBulkWithGreenesSplit_small_100() throws FactoryException, IOException {
        insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 100, testConfigs.get("small"));
    }

    /*
     * Medium model 500*500 nodes
     */

    @Ignore
    public void shouldInsertManyNodesIndividuallyWithQuadraticSplit_medium() throws FactoryException, IOException {
        insertManyNodesIndividually(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("medium"));
    }

    @Ignore
    public void shouldInsertManyNodesIndividuallyGreenesSplit_medium() throws FactoryException, IOException {
        insertManyNodesIndividually(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("medium"));
    }

    @Test
    public void shouldInsertManyNodesInBulkWithQuadraticSplit_medium() throws FactoryException, IOException {
        insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("medium"));
    }

    @Test
    public void shouldInsertManyNodesInBulkWithGreenesSplit_medium() throws FactoryException, IOException {
        insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("medium"));
    }

    /*
     * Medium model 500*500 nodes (shallow tree)
     */

    @Ignore
    public void shouldInsertManyNodesIndividuallyWithQuadraticSplit_medium_100() throws FactoryException, IOException {
        insertManyNodesIndividually(RTreeIndex.QUADRATIC_SPLIT, 5000, 100, testConfigs.get("medium"));
    }

    @Ignore
    public void shouldInsertManyNodesIndividuallyGreenesSplit_medium_100() throws FactoryException, IOException {
        insertManyNodesIndividually(RTreeIndex.GREENES_SPLIT, 5000, 100, testConfigs.get("medium"));
    }

    @Test
    public void shouldInsertManyNodesInBulkWithQuadraticSplit_medium_100() throws FactoryException, IOException {
        insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 100, testConfigs.get("medium"));
    }

    @Test
    public void shouldInsertManyNodesInBulkWithGreenesSplit_medium_100() throws FactoryException, IOException {
        insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 100, testConfigs.get("medium"));
    }

    /*
     * Large model 750*750 nodes
     */

    @Ignore
    public void shouldInsertManyNodesIndividuallyWithQuadraticSplit_large() throws FactoryException, IOException {
        insertManyNodesIndividually(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("large"));
    }

    @Ignore
    public void shouldInsertManyNodesIndividuallyGreenesSplit_large() throws FactoryException, IOException {
        insertManyNodesIndividually(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("large"));
    }

    @Test
    public void shouldInsertManyNodesInBulkWithQuadraticSplit_large() throws FactoryException, IOException {
        insertManyNodesInBulk(RTreeIndex.QUADRATIC_SPLIT, 5000, 10, testConfigs.get("large"));
    }

    @Test
    public void shouldInsertManyNodesInBulkWithGreenesSplit_large() throws FactoryException, IOException {
        insertManyNodesInBulk(RTreeIndex.GREENES_SPLIT, 5000, 10, testConfigs.get("large"));
    }

    /*
     * Private methods used by the above tests
     */

    class TreePrintingMonitor extends RTreeMonitor {
        private final RTreeImageExporter imageExporter;
        private final String splitMode;
        private final String insertMode;
        private HashMap<String, Integer> called = new HashMap<>();
        TreePrintingMonitor(RTreeImageExporter imageExporter, String insertMode, String splitMode) {
            this.imageExporter = imageExporter;
            this.splitMode = splitMode;
            this.insertMode = insertMode;
        }
        private Integer getCalled(String key) {
            if (!called.containsKey(key)) {
                called.put(key, 0);
            }
            return called.get(key);
        }
        @Override
        public void addNbrRebuilt(RTreeIndex rtree) {
            super.addNbrRebuilt(rtree);
            printRTreeImage("rebuilt", rtree.getIndexRoot(), new ArrayList<>());
        }

        @Override
        public void addSplit(Node indexNode) {
            super.addSplit(indexNode);
            printRTreeImage("split", indexNode, new ArrayList<>());
        }

        @Override
        public void beforeMergeTree(Node indexNode, List<RTreeIndex.NodeWithEnvelope> right) {
            super.beforeMergeTree(indexNode, right);

            printRTreeImage("before-merge", indexNode, right.stream().map(e -> e.envelope).collect(Collectors.toList()));

        }

        @Override
        public void afterMergeTree(Node indexNode) {
            super.afterMergeTree(indexNode);
            printRTreeImage("after-merge", indexNode, new ArrayList<>());
        }


        private void printRTreeImage(String context, Node rootNode, List<Envelope> envelopes) {
            try (Transaction tx = db.beginTx()) {
                int count = getCalled(context);
                imageExporter.saveRTreeLayers(new File("rtree-" + insertMode + "-" + splitMode + "/debug-" + context + "/rtree-" + count + ".png"),
                        rootNode, envelopes, 7);
                called.put(context, count + 1);
                tx.success();
            } catch (IOException e) {
                System.out.println("Failed to print RTree to disk: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void insertManyNodesIndividually(String splitMode, int blockSize, int maxNodeReferences, RTreeTestConfig config)
            throws FactoryException, IOException {
        TestStats stats = new TestStats(config, "Single", splitMode, blockSize, maxNodeReferences);
        List<Node> nodes = setup(config.width);
        EditableLayer layer = (EditableLayer) new SpatialDatabaseService(db).getLayer("Coordinates");
        TreeMonitor monitor = new RTreeMonitor();
        layer.getIndex().addMonitor(monitor);
        layer.getIndex().configure(map(RTreeIndex.KEY_SPLIT, splitMode, RTreeIndex.KEY_MAX_NODE_REFERENCES, maxNodeReferences));
        TimedLogger log = new TimedLogger("Inserting " + config.totalCount + " nodes into RTree using solo insert and "
                + splitMode + " split with " + maxNodeReferences + " maxNodeReferences", config.totalCount);
        long start = System.currentTimeMillis();
        for (int i = 0; i < config.totalCount / blockSize; i++) {
            List<Node> slice = nodes.subList(i * blockSize, i * blockSize + blockSize);
            try (Transaction tx = spatialProcedures().db.beginTx()) {
                for (Node node : slice) {
                    layer.add(node);
                }
                tx.success();
            }
            log.log("Splits: " + monitor.getNbrSplit(), (i + 1) * blockSize);
        }
        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + config.totalCount + " nodes to RTree in bulk");
        stats.setInsertTime(start);
        stats.put("Insert Splits", monitor.getNbrSplit());

        queryRTree(layer, monitor, stats);
        verifyTreeStructure(layer, splitMode, stats);
    }

    /*
     * Run this manually to generate images of RTree that can be used for animation.
     * ffmpeg -f image2 -r 12 -i rtree-single/rtree-%d.png -r 12 -s 1280x960 rtree-single2_12fps.mp4
     */
    @Ignore
    public void shouldInsertManyNodesIndividuallyAndGenerateImagesForAnimation() throws FactoryException, IOException {
        RTreeTestConfig config = testConfigs.get("medium");
        int blockSize = 5;
        int maxBlockSize = 1000;
        int maxNodeReferences = 10;
        String splitMode = RTreeIndex.GREENES_SPLIT;
        TestStats stats = new TestStats(config, "Single", splitMode, blockSize, maxNodeReferences);
        List<Node> nodes = setup(config.width);

        EditableLayer layer = (EditableLayer) new SpatialDatabaseService(db).getLayer("Coordinates");
        RTreeIndex rtree = (RTreeIndex) layer.getIndex();
        RTreeImageExporter imageExporter;
        try (Transaction tx = db.beginTx()) {
            imageExporter = new RTreeImageExporter(layer, rtree, new Coordinate(0.0, 0.0), new Coordinate(1.0, 1.0));
            tx.success();
        }

        TreeMonitor monitor = new TreePrintingMonitor(imageExporter, "single", splitMode);
        layer.getIndex().addMonitor(monitor);
        layer.getIndex().configure(map(RTreeIndex.KEY_SPLIT, splitMode, RTreeIndex.KEY_MAX_NODE_REFERENCES, maxNodeReferences));
        TimedLogger log = new TimedLogger("Inserting " + config.totalCount + " nodes into RTree using solo insert and "
                + splitMode + " split with " + maxNodeReferences + " maxNodeReferences", config.totalCount);
        long start = System.currentTimeMillis();
        int prevBlock = 0;
        int i = 0;
        int currBlock = 1;
        while (currBlock < nodes.size()) {
            List<Node> slice = nodes.subList(prevBlock, currBlock);
            long startIndexing = System.currentTimeMillis();
            try (Transaction tx = spatialProcedures().db.beginTx()) {
                for (Node node : slice) {
                    layer.add(node);
                }
                tx.success();
            }
            log.log("Splits: " + monitor.getNbrSplit(), currBlock);
            try (Transaction tx = db.beginTx()) {
                imageExporter.saveRTreeLayers(new File("rtree-single-" + splitMode + "/rtree-" + i + ".png"), 7);
                tx.success();
            }
            i++;
            prevBlock = currBlock;
            currBlock += Math.min(blockSize, maxBlockSize);
            blockSize *= 1.33;
        }
        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + config.totalCount + " nodes to RTree in bulk");
        stats.setInsertTime(start);
        stats.put("Insert Splits", monitor.getNbrSplit());

        monitor.reset();
        List<Node> found = queryRTree(layer, monitor, stats, false);
        verifyTreeStructure(layer, splitMode, stats);
        imageExporter.saveRTreeLayers(new File("rtree-single-" + splitMode + "/rtree.png"), 7, monitor, found, config.searchMin, config.searchMax);
    }

    private void insertManyNodesInBulk(String splitMode, int blockSize, int maxNodeReferences, RTreeTestConfig config)
            throws FactoryException, IOException {
        TestStats stats = new TestStats(config, "Bulk", splitMode, blockSize, maxNodeReferences);
        List<Node> nodes = setup(config.width);
        EditableLayer layer = (EditableLayer) new SpatialDatabaseService(db).getLayer("Coordinates");
        RTreeMonitor monitor = new RTreeMonitor();
        layer.getIndex().addMonitor(monitor);
        layer.getIndex().configure(map(RTreeIndex.KEY_SPLIT, splitMode, RTreeIndex.KEY_MAX_NODE_REFERENCES, maxNodeReferences));
        TimedLogger log = new TimedLogger("Inserting " + config.totalCount + " nodes into RTree using bulk insert and "
                + splitMode + " split with " + maxNodeReferences + " maxNodeReferences", config.totalCount);
        long start = System.currentTimeMillis();
        for (int i = 0; i < nodes.size() / blockSize; i++) {
            List<Node> slice = nodes.subList(i * blockSize, i * blockSize + blockSize);
            long startIndexing = System.currentTimeMillis();
            try (Transaction tx = db.beginTx()) {
                layer.addAll(slice);
                tx.success();
            }
            log.log(startIndexing, "Rebuilt: " + monitor.getNbrRebuilt() + ", Splits: " + monitor.getNbrSplit() + ", Cases " + monitor.getCaseCounts(), (i + 1) * blockSize);
        }
        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + config.totalCount + " nodes to RTree in bulk");
        stats.setInsertTime(start);
        stats.put("Insert Splits", monitor.getNbrSplit());

        monitor.reset();
        queryRTree(layer, monitor, stats);
        verifyTreeStructure(layer, splitMode, stats);
//        debugIndexTree((RTreeIndex) layer.getIndex());
    }

    /*
     * Run this manually to generate images of RTree that can be used for animation.
     * ffmpeg -f image2 -r 12 -i rtree-single/rtree-%d.png -r 12 -s 1280x960 rtree-single2_12fps.mp4
     */
    @Test
    public void shouldInsertManyNodesInBulkAndGenerateImagesForAnimation() throws FactoryException, IOException {
        RTreeTestConfig config = testConfigs.get("medium");
        int blockSize = 1000;
        int maxNodeReferences = 10;
        String splitMode = RTreeIndex.GREENES_SPLIT;
        TestStats stats = new TestStats(config, "Bulk", splitMode, blockSize, maxNodeReferences);
        List<Node> nodes = setup(config.width);

        EditableLayer layer = (EditableLayer) new SpatialDatabaseService(db).getLayer("Coordinates");
        RTreeIndex rtree = (RTreeIndex) layer.getIndex();
        RTreeImageExporter imageExporter;
        try (Transaction tx = db.beginTx()) {
            imageExporter = new RTreeImageExporter(layer, rtree, new Coordinate(0.0, 0.0), new Coordinate(1.0, 1.0));
            tx.success();
        }

        TreeMonitor monitor = new TreePrintingMonitor(imageExporter, "bulk", splitMode);
        layer.getIndex().addMonitor(monitor);
        layer.getIndex().configure(map(RTreeIndex.KEY_SPLIT, splitMode, RTreeIndex.KEY_MAX_NODE_REFERENCES, maxNodeReferences));
        TimedLogger log = new TimedLogger("Inserting " + config.totalCount + " nodes into RTree using bulk insert and "
                + splitMode + " split with " + maxNodeReferences + " maxNodeReferences", config.totalCount);
        long start = System.currentTimeMillis();
        for (int i = 0; i < nodes.size() / blockSize; i++) {
            List<Node> slice = nodes.subList(i * blockSize, i * blockSize + blockSize);
            long startIndexing = System.currentTimeMillis();
            try (Transaction tx = db.beginTx()) {
                layer.addAll(slice);
                tx.success();
            }
            log.log(startIndexing, "Rebuilt: " + monitor.getNbrRebuilt() + ", Splits: " + monitor.getNbrSplit() + ", Cases " + monitor.getCaseCounts(), (i + 1) * blockSize);
            try (Transaction tx = db.beginTx()) {
                imageExporter.saveRTreeLayers(new File("rtree-bulk-" + splitMode + "/rtree-" + i + ".png"), 7);
                tx.success();
            }
        }
        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to add " + config.totalCount + " nodes to RTree in bulk");
        stats.setInsertTime(start);
        stats.put("Insert Splits", monitor.getNbrSplit());

        monitor.reset();
        List<Node> found = queryRTree(layer, monitor, stats, false);
        verifyTreeStructure(layer, splitMode, stats);
        imageExporter.saveRTreeLayers(new File("rtree-bulk-" + splitMode + "/rtree.png"), 7, monitor, found, config.searchMin, config.searchMax);
//        debugIndexTree((RTreeIndex) layer.getIndex());
    }

    @Ignore
    public void shouldAccessIndexAfterBulkInsertion() throws Exception {
        // Use these two lines if you want to examine the output.
//        File dbPath = new File("target/var/BulkTest");
//        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath.getCanonicalPath());
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

    @Ignore
    public void shouldBuildTreeFromScratch() throws Exception {
//        File dbPath = new File("target/var/BulkTest2");
//        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        GraphDatabaseService db = this.db;

        SpatialDatabaseService sdbs = new SpatialDatabaseService(db);
        try {
            GeometryEncoder encoder = new SimplePointEncoder();

            Method decodeEnvelopes = RTreeIndex.class.getDeclaredMethod("decodeEnvelopes", List.class);
            decodeEnvelopes.setAccessible(true);

            Method buildRTreeFromScratch = RTreeIndex.class.getDeclaredMethod("buildRtreeFromScratch", Node.class, List.class, double.class);
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

                    buildRTreeFromScratch.invoke(rtree, rtree.getIndexRoot(), decodeEnvelopes.invoke(rtree, coords), 0.7);
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

    @Ignore
    public void shouldPerformRTreeBulkInsertion() throws Exception {
        // Use these two lines if you want to examine the output.
//        File dbPath = new File("target/var/BulkTest");
//        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);

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
//            sdbs.getDatabase().shutdown();
//            FileUtils.deleteDirectory(dbPath);
        }
    }

    private List<Node> populateSquareTestData(int width) {
        GraphDatabaseService db = this.db;
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

    private List<Node> populateSquareTestDataHeavy(int width) {
        List<Node> nodes = populateSquareTestData(width);
        Random rand = new Random(42);

        for (int i = 0; i < width / 2; i++) {
            try (Transaction tx = db.beginTx()) {
                for (int j = 0; j < width / 2; j++) {
                    Node node = db.createNode();
                    node.addLabel(Label.label("Coordinates"));

                    node.setProperty("lat", ((double) rand.nextInt(width / 10) / (double) width));
                    node.setProperty("lon", ((double) rand.nextInt(width / 10) / (double) width));
                    nodes.add(node);
                }
                tx.success();
            }
        }
        java.util.Collections.shuffle(nodes, new Random(8));
        return nodes;
    }

    private List<Node> populateSquareWithStreets(int width) {
        List<Node> nodes = new ArrayList<>();
        double squareValue = 0.25;
        for (int i = 1; i < 4; i += 2) {
            try (Transaction tx = db.beginTx()) {
                for (int j = (int) squareValue * width; j < 2 * squareValue * width; j++) {
                    Node node = db.createNode();
                    node.addLabel(Label.label("Coordinates"));
                    node.setProperty("lat", i * squareValue);
                    node.setProperty("lon", (j + squareValue) / width + squareValue);
                    nodes.add(node);
                    Node node2 = db.createNode();
                    node2.addLabel(Label.label("Coordinates"));
                    node2.setProperty("lat", (j + squareValue) / width + squareValue);
                    node2.setProperty("lon", i * squareValue);
                    nodes.add(node2);

                }
                tx.success();
            }
        }
        for (int i = 0; i < width; i++) {
            try (Transaction tx = db.beginTx()) {

                Node node = db.createNode();
                node.addLabel(Label.label("Coordinates"));
                node.setProperty("lat", ((double) i / (double) width));
                node.setProperty("lon", ((double) i / (double) width));
                nodes.add(node);
                Node node2 = db.createNode();
                node2.addLabel(Label.label("Coordinates"));
                node2.setProperty("lat", ((double) (width - i) / (double) width));
                node2.setProperty("lon", ((double) i / (double) width));
                nodes.add(node2);
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
            Result result = db.execute("CALL spatial.withinDistance('Coordinates',{longitude:0.5, latitude:0.5},1000.0) yield node");
            int i = 0;
            ResourceIterator thing = result.columnAs("node");
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
        SpatialDatabaseService sdbs = new SpatialDatabaseService(db);
        CoordinateReferenceSystem crs = DefaultEngineeringCRS.GENERIC_2D;
        EditableLayer layer = sdbs.getOrCreatePointLayer("Coordinates", "lon", "lat");
        try (Transaction tx = db.beginTx()) {
            layer.setCoordinateReferenceSystem(crs);
            tx.success();
        }
        return nodes;
    }

    private SpatialProcedures spatialProcedures() {
        SpatialProcedures spatialProcedures = new SpatialProcedures();
        spatialProcedures.db = db;
        return spatialProcedures;
    }

    class NodeWithEnvelope {
        Envelope envelope;
        Node node;

        NodeWithEnvelope(Node node, Envelope envelope) {
            this.node = node;
            this.envelope = envelope;
        }
    }

    private void checkIndexOverlaps(Layer layer, TestStats stats) {
        RTreeIndex index = (RTreeIndex) layer.getIndex();
        Node root = index.getIndexRoot();
        ArrayList<ArrayList<NodeWithEnvelope>> nodes = new ArrayList<>();
        nodes.add(new ArrayList<>());
        nodes.get(0).add(new NodeWithEnvelope(root, index.getIndexNodeEnvelope(root)));
        do {
            ArrayList<NodeWithEnvelope> children = new ArrayList<>();
            for (NodeWithEnvelope parent : nodes.get(nodes.size() - 1)) {
                for (Relationship rel : parent.node.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
                    Node child = rel.getEndNode();
                    children.add(new NodeWithEnvelope(child, index.getIndexNodeEnvelope(child)));
                }
            }
            if (children.isEmpty()) {
                break;
            } else {
                nodes.add(children);
            }
        } while (true);
        System.out.println("Comparison of index node areas to root area for " + nodes.size() + " index levels:");
        for (int level = 0; level < nodes.size(); level++) {
            double[] overlap = calculateOverlap(nodes.get(0).get(0), nodes.get(level));
            System.out.println("\t" + level + "\t" + nodes.get(level).size() + "\t" + overlap[0] + "\t" + overlap[1]);
            stats.put("Leaf Overlap Delta", overlap[0]);
            stats.put("Leaf Overlap Ratio", overlap[1]);
        }
    }

    private double[] calculateOverlap(NodeWithEnvelope root, List<NodeWithEnvelope> nodes) {
        double rootArea = root.envelope.getArea();
        double nodesArea = 0.0;
        for (NodeWithEnvelope entry : nodes) {
            nodesArea += entry.envelope.getArea();
        }
        return new double[]{nodesArea - rootArea, nodesArea / rootArea};
    }

    private List<Node> queryRTree(Layer layer, TreeMonitor monitor, TestStats stats) {
        return queryRTree(layer, monitor, stats, true);
    }

    private List<Node> queryRTree(Layer layer, TreeMonitor monitor, TestStats stats, boolean assertTouches) {
        List<Node> nodes;
        RTreeTestConfig config = stats.config;
        long start = System.currentTimeMillis();
        int maxNodeReferences = stats.maxNodeReferences;
        try (Transaction tx = db.beginTx()) {
            com.vividsolutions.jts.geom.Envelope envelope = new com.vividsolutions.jts.geom.Envelope(config.searchMin, config.searchMax);
            nodes = GeoPipeline.startWithinSearch(layer, layer.getGeometryFactory().toGeometry(envelope)).stream().map(GeoPipeFlow::getGeomNode).collect(Collectors.toList());
            tx.success();
        }
        long count = nodes.size();
        long queryTime = System.currentTimeMillis() - start;
        allStats.add(stats);
        stats.put("Query Time (ms)", queryTime);
        System.out.println("Took " + queryTime + "ms to find " + count + " nodes in 4x4 block");
        int indexTouched = monitor.getCaseCounts().get("Index Does NOT Match");
        int indexMatched = monitor.getCaseCounts().get("Index Matches");
        int touched = monitor.getCaseCounts().get("Geometry Does NOT Match");
        int matched = monitor.getCaseCounts().get("Geometry Matches");
        int geometrySize = layer.getIndex().count();
        int indexSize = 0;
        try (Transaction tx = db.beginTx()) {
            for (Node n : ((RTreeIndex) layer.getIndex()).getAllIndexInternalNodes()) {
                indexSize++;
            }
            tx.success();
        }
        stats.put("Index Size", indexSize);
        stats.put("Indexed", geometrySize);
        stats.put("Found", matched);
        stats.put("Touched", touched);
        stats.put("Index Found", indexMatched);
        stats.put("Index Touched", indexTouched);
        System.out.println("Searched index of " + indexSize + " nodes in tree of height " + monitor.getHeight());
        System.out.println("Matched " + matched + "/" + touched + " touched nodes (" + (100.0 * matched / touched) + "%)");
        System.out.println("Having matched " + indexMatched + "/" + indexTouched + " touched index nodes (" + (100.0 * indexMatched / indexTouched) + "%)");
        System.out.println("Which means we touched " + indexTouched + "/" + indexSize + " index nodes (" + (100.0 * indexTouched / indexSize) + "%)");
        System.out.println("Index contains " + geometrySize + " geometries");
        assertEquals("Expected " + config.expectedCount + " nodes to be returned", config.expectedCount, count);
        assertEquals("Expected " + config.expectedCount + " nodes to be matched", config.expectedCount, matched);
        int maxExpectedGeometriesTouched = matched * maxNodeReferences;
        if (count > 1 && assertTouches) {
            assertThat("Should not touch more geometries than " + maxNodeReferences + "*matched", touched, lessThanOrEqualTo(maxExpectedGeometriesTouched));
            int maxExpectedIndexTouched = indexMatched * maxNodeReferences;
            assertThat("Should not touch more index nodes than " + maxNodeReferences + "*matched", indexTouched, lessThanOrEqualTo(maxExpectedIndexTouched));
        }
        return nodes;
    }

    private class TimedLogger {
        String title;
        long count;
        long gap;
        long start;
        long previous;

        public TimedLogger(String title, long count) {
            this(title, count, 1000);
        }

        public TimedLogger(String title, long count, long gap) {
            this.title = title;
            this.count = count;
            this.gap = gap;
            this.start = System.currentTimeMillis();
            this.previous = this.start;
            System.out.println(title);
        }

        public void log(long previous, String line, long number) {
            this.previous = previous;
            log(line, number);
        }

        public void log(String line, long number) {
            long current = System.currentTimeMillis();
            if (current - previous > gap) {
                double percentage = 100.0 * number / count;
                double seconds = (current - start) / 1000.0;
                int rate = (int) (number / seconds);
                System.out.println("\t" + ((int) percentage) + "%\t" + number + "\t" + seconds + "s\t" + rate + "n/s:\t" + line);
                previous = current;
            }
        }
    }

    private void verifyTreeStructure(Layer layer, String splitMode, TestStats stats) {
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
                        "MATCH p = (root)-[:RTREE_CHILD*0..]->(parent)-[:RTREE_CHILD|:RTREE_REFERENCE]->(child) " +
                        "RETURN parent, length(p) as depth, count (*) as children";
        Map<String, Object> params = Collections.singletonMap("layerNodeId", layerNode.getId());
        Result resultDepth = db.execute(queryDepthAndGeometries, params);
        int balanced = 0;
        long geometries = 0;
        while (resultDepth.hasNext()) {
            balanced++;
            Map<String, Object> depthMap = resultDepth.next();
            geometries = (long) depthMap.get("geometries");
            System.out.println("Tree depth to all geometries: " + depthMap);
        }
        assertEquals("All geometries should be at the same depth", 1, balanced);
        Result resultNumChildren = db.execute(queryNumChildren, params);
        Map<String, Object> leafMap = resultNumChildren.next();
        System.out.println("Tree depth to all leaves that have geomtries: " + leafMap);
        Result resultChildrenPerParent = db.execute(queryChildrenPerParent, params);
        System.out.println("Children per parent: " + resultChildrenPerParent.next());

        Result resultChildrenPerParent2 = db.execute(queryChildrenPerParent2, params);
        Integer[] histogram = new Integer[11];
        int totalNodes = 0;
        int underfilledNodes = 0;
        int blockSize = Math.max(10, stats.maxNodeReferences) / 10;
        Arrays.fill(histogram, 0);
        while (resultChildrenPerParent2.hasNext()) {
            Map<String, Object> result = resultChildrenPerParent2.next();
            long children = (long) result.get("children");
            if (children < blockSize * 3) {
                underfilledNodes++;
            }
            totalNodes++;
            histogram[(int) children / blockSize]++;
        }
        allStats.add(stats);
        stats.put("Underfilled%", 100.0 * underfilledNodes / totalNodes);

        System.out.println("Histogram of child count for " + totalNodes + " index nodes, with " + underfilledNodes + " (" + (100 * underfilledNodes / totalNodes) + "%) underfilled (less than 30% or " + (blockSize * 3) + ")");
        for (int i = 0; i < histogram.length; i++) {
            System.out.println("[" + (i * blockSize) + ".." + ((i + 1) * blockSize) + "): " + histogram[i]);
        }
        if (!splitMode.equals(RTreeIndex.QUADRATIC_SPLIT)) {
            assertThat("Expected to have less than 30% of nodes underfilled", underfilledNodes, lessThan(3 * totalNodes / 10));
        }
        long leafCountFactor = splitMode.equals(RTreeIndex.QUADRATIC_SPLIT) ? 20 : 2;
        long maxLeafCount = leafCountFactor * geometries / stats.maxNodeReferences;
        assertThat("In " + splitMode + " we expected leaves to be no more than " + leafCountFactor + "x(geometries/maxNodeReferences)", (long) leafMap.get("leaves"), lessThanOrEqualTo(maxLeafCount));
        checkIndexOverlaps(layer, stats);
    }

    private void restart() throws IOException {
        if (db != null) {
            db.shutdown();
        }
        if (storeDir.exists()) {
            System.out.println("Deleting previous database: " + storeDir);
            FileUtils.deleteDirectory(storeDir);
        }
        FileUtils.forceMkdir(storeDir);
        TestGraphDatabaseFactory dbFactory = new TestGraphDatabaseFactory();
        db = dbFactory.newEmbeddedDatabaseBuilder(storeDir).newGraphDatabase();
    }

    private void doCleanShutdown() throws IOException {
        try {
            System.out.println("Shutting down database");
            db.shutdown();
            //TODO: Uncomment this once all tests are stable
//            FileUtils.deleteDirectory(storeDir);
        } finally {
            db = null;
        }
    }

    private static class TestStats {
        private RTreeTestConfig config;
        private String insertMode;
        private int dataSize;
        private int blockSize;
        private String splitMode;
        private int maxNodeReferences;
        static LinkedHashSet<String> knownKeys = new LinkedHashSet<>();
        private HashMap<String, Object> data = new HashMap<>();

        public TestStats(RTreeTestConfig config, String insertMode, String splitMode, int blockSize, int maxNodeReferences) {
            this.config = config;
            this.insertMode = insertMode;
            this.dataSize = config.width * config.width;
            this.blockSize = blockSize;
            this.splitMode = splitMode;
            this.maxNodeReferences = maxNodeReferences;
        }

        public void setInsertTime(long start) {
            long current = System.currentTimeMillis();
            double seconds = (current - start) / 1000.0;
            int rate = (int) (dataSize / seconds);
            put("Insert Time (s)", seconds);
            put("Insert Rate (n/s)", rate);
        }

        public void put(String key, Object value) {
            knownKeys.add(key);
            data.put(key, value);
        }

        public void get(String key) {
            data.get(key);
        }

        public static String[] headerArray() {
            return new String[]{"Size Name", "Insert Mode", "Split Mode", "Data Width", "Data Size", "Block Size", "Max Node References"};
        }

        public Object[] fieldArray() {
            return new Object[]{config.name, insertMode, splitMode, config.width, dataSize, blockSize, maxNodeReferences};
        }

        public static List<String> headerList() {
            ArrayList<String> fieldList = new ArrayList<>();
            fieldList.addAll(Arrays.asList(headerArray()));
            fieldList.addAll(knownKeys);
            return fieldList;
        }

        public List<Object> asList() {
            ArrayList<Object> fieldList = new ArrayList<>();
            fieldList.addAll(Arrays.asList(fieldArray()));
            fieldList.addAll(knownKeys.stream().map(k -> data.get(k)).map(v -> (v == null) ? "" : v).collect(Collectors.toList()));
            return fieldList;
        }

        public static String headerString() {
            return String.join("\t", headerList());
        }

        public String toString() {
            return String.join("\t", asList().stream().map(Object::toString).collect(Collectors.toList()));
        }
    }

    private static final LinkedHashSet<TestStats> allStats = new LinkedHashSet<>();

    @AfterClass
    public static void afterClass() {
        System.out.println("\n\nComposite stats for " + allStats.size() + " tests run");
        System.out.println(TestStats.headerString());
        for (TestStats stats : allStats) {
            System.out.println(stats);
        }
    }
}
