/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.rtree;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.MultiMap;
import org.neo4j.gis.spatial.SpatialRelationshipTypes;
import org.neo4j.gis.spatial.Utilities;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.rtree.filter.SearchResults;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser.Order;


/**
 *
 */
public class RTreeIndex implements SpatialIndexWriter {

    public static final String INDEX_PROP_BBOX = "bbox_xx";

    // Constructor

    public RTreeIndex(GraphDatabaseService database, Node rootNode, EnvelopeDecoder envelopeEncoder) {
        this(database, rootNode, envelopeEncoder, 100);
    }

    public RTreeIndex(GraphDatabaseService database, Node rootNode, EnvelopeDecoder envelopeDecoder, int maxNodeReferences) {
        this.database = database;
        this.rootNode = rootNode;
        this.envelopeDecoder = envelopeDecoder;
        this.maxNodeReferences = maxNodeReferences;

        if (envelopeDecoder == null) {
            throw new NullPointerException("envelopeDecoder is NULL");
        }

        initIndexRoot();
        initIndexMetadata();
    }


    // Public methods

    @Override
    public EnvelopeDecoder getEnvelopeDecoder() {
        return this.envelopeDecoder;
    }


    @Override
    public void add(Node geomNode) {
        // initialize the search with root
        Node parent = getIndexRoot();

        addBelow(parent, geomNode);

        countSaved = false;
        totalGeometryCount++;
    }

    /**
     * This method will add the node somewhere below the parent.
     * @param parent
     * @param geomNode
     */
    private void addBelow(Node parent, Node geomNode){
        // choose a path down to a leaf
        while (!nodeIsLeaf(parent)) {
            parent = chooseSubTree(parent, geomNode);
        }

        if (countChildren(parent, RTreeRelationshipTypes.RTREE_REFERENCE) >= maxNodeReferences) {
            insertInLeaf(parent, geomNode);
            splitAndAdjustPathBoundingBox(parent);
        } else {
            if (insertInLeaf(parent, geomNode)) {
                // bbox enlargement needed
                adjustPathBoundingBox(parent);
            }
        }
    }



    /**
     * Use this method if you want to insert an index node as a child of a given index node. This will recursively
     * update the bounding boxes above the parent to keep the tree consistent.
     * @param parent
     * @param child
     */
    private void insertIndexNodeOnParent(Node parent, Node child){
        if(countChildren(parent, RTreeRelationshipTypes.RTREE_CHILD) < maxNodeReferences){
            if(addChild(parent, RTreeRelationshipTypes.RTREE_CHILD, child)){
                adjustPathBoundingBox(parent);
            }
        } else {
            addChild(parent, RTreeRelationshipTypes.RTREE_CHILD, child);
            splitAndAdjustPathBoundingBox(parent);
        }
    }


    /**
     * Depending on the size of the incumbent tree, this will either attempt to rebuild the entire index from scratch
     * (strategy used if the insert larger than 40% of the current tree size - may give heap out of memory errors for
     * large inserts as has O(n) space complexity in the total tree size. It has nlog(n) time complexity. See fuction
     * partition for more details.) or it will insert using the method of seeded clustering, where you attempt to use the
     * existing tree structure to partition your data.
     *
     * This is based on the Paper "Bulk Insertion for R-trees by seeded clustering" by T.Lee, S.Lee & B Moon.
     * Repeated use of this strategy will lead to degraded query performance, especially if used for
     * many relatively small insertions compared to tree size. Though not worse than one by one insertion.
     * In practice, it should be fine for most uses.
     * @param geomNodes
     */
    @Override
    public void add(List<Node> geomNodes){
        System.out.println("TotalGeomCount = " + totalGeometryCount);
        System.out.println(geomNodes.size());
        System.out.println(getRootNode().toString());
        //If the insertion is large relative to the size of the tree, simply rebuild the whole tree.
        if(geomNodes.size() > totalGeometryCount*0.4){
            for(Node n : getAllIndexedNodes()){
                geomNodes.add(n);
            }
            for(Node n : getAllIndexInternalNodes()){
                deleteNode(n);
            }

            buildRtreeFromScratch(getIndexRoot(), geomNodes, 0.7, 10);
            countSaved = false;
            totalGeometryCount = geomNodes.size();
            System.out.println("TotalGeomCount = " + totalGeometryCount);
            return;
        } else {
            System.out.println("Attempting Bulk Insertion");
            List<Node> outliers = bulkInsertion(getIndexRoot(), getHeight(getIndexRoot(), 0), geomNodes, 0.7);
            System.out.println("There are " + outliers.size() + " outliers to be inserted individually");
            countSaved = false;
            totalGeometryCount = totalGeometryCount + (geomNodes.size() - outliers.size());
            for(Node n : outliers){
                add(n);
            }
            System.out.println("TotalGeomCount = " + totalGeometryCount);
            return;
        }
    }



    /**
     * Returns the height of the tree, starting with the rootNode and adding one for each subsequent level. Relies on the
     * balanced property of the RTree that all leaves are on the same level and no index nodes are empty.
     * @param rootNode
     * @param height
     * @return
     */
    private int getHeight(Node rootNode, int height){
        Iterator<Relationship> rels = rootNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD).iterator();
        if(rels.hasNext()){
            return getHeight(rels.next().getEndNode(), height+1);
        } else {
            return height+1;
        }
    }

    private List<Node> getIndexChildren(Node rootNode){
        List<Node> result = new ArrayList<Node>();
        for(Relationship r : rootNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)){
            result.add(r.getEndNode());
        }
        return result;
    }

    private List<Node> getIndexChildren(Node rootNode, int depth){
        if(depth < 1){
            throw new IllegalArgumentException("Depths must be at least one");
        } else if(depth == 1){
            return getIndexChildren(rootNode);
        } else{
            List<Node> result = new ArrayList<>();
            for(Node child : getIndexChildren(rootNode)){
                result.addAll(getIndexChildren(child, depth-1));
            }
            return result;
        }
    }

    private List<Node> bulkInsertion(Node rootNode, int rootNodeHeight, final List<Node> geomNodes, final double loadingFactor){
        List<Node> children = getIndexChildren(rootNode);
        Collections.sort(children, new indexNodeAreaComparator());
        Map<Node, List<Node>> map = new HashMap<>(children.size());
        //initialiseMap
        List<Node> outliers = new ArrayList<Node>();
        for(Node n : children){
            map.put(n, new ArrayList<Node>());
        }
        for(Node n : geomNodes){
            Envelope env = envelopeDecoder.decodeEnvelope(n);
            boolean flag = true;

            //exploits that the iterator returns the list inorder, which is sorted by size, as above. Thus child
            //is always added to the smallest existing envelope which contains it.
            for(Node c : children){
                if(getIndexNodeEnvelope(c).contains(env)){
                    map.get(c).add(n); //add to smallest area envelope which contains the child;
                    flag = false;
                    break;
                }
            }
            // else add to outliers.
            if(flag){
                outliers.add(n);
            }
        }

        for(Node child : children){
            List<Node> cluster = map.get(child);

            if(cluster.isEmpty()){
                continue;
            } //if the number of nodes is too small, advance down teh tree and try again.
            else if(expectedHeight(loadingFactor, cluster.size()) < rootNodeHeight -2){
                outliers.addAll(bulkInsertion(child, rootNodeHeight -1, cluster, loadingFactor));
            } //if constructed tree is the correct size insert it here.
            else if(expectedHeight(loadingFactor, cluster.size()) == rootNodeHeight -2){
                //Do not create underfull nodes, instead use the add logic, except we know the root not to add them too.
                if(cluster.size() < maxNodeReferences*loadingFactor/2){
                    for(Node n : cluster){
                        addBelow(getIndexNodeParent(child), n);
                        //getParent because addition might cause a split. This strategy not ideal, but does tend to limit overlap more than adding to the child exclusively.
                    }
                } else {
                    Node newRootNode = database.createNode();
                    buildRtreeFromScratch(newRootNode,cluster,loadingFactor, 4);
                    insertIndexNodeOnParent(child, newRootNode);
                }

            } else{
                Node newRootNode = database.createNode();
                buildRtreeFromScratch(newRootNode,cluster,loadingFactor, 4);
                List<Node> childrenToBeInserted = getIndexChildren(newRootNode, getHeight(newRootNode, 0) - (rootNodeHeight-2));
                for(Node n : childrenToBeInserted){
                    Relationship relationship = n.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING);
                    relationship.delete();
                    insertIndexNodeOnParent(child, n);
                }
                deleteRecursivelySubtree(newRootNode); // remove the buffer tree remnants
            }
        }

        return outliers;
    }



    private int expectedHeight(double loadingFactor, int size){
        final int targetLoading = (int) Math.round(maxNodeReferences*loadingFactor);
        return (int) Math.ceil(Math.log(size)/Math.log(targetLoading)); //exploit change of base formula
    }

    /**
     * This algorithm is based on Overlap Minimizing Top-down Bulk Loading Algorithm for R-tree by T Lee and S Lee.
     * This is effectively a wrapper function around the function Partition which will attempt to parallelise the task.
     * This can work better or worse since the top level may have as few as two nodes, in which case it fails is not optimal.
     * //TODO - Better parallelisation strategy.
     * @param rootNode
     * @param geomNodes
     * @param loadingFactor - Must be between 0.1 and 1, this is how full each node will be, approximately.
     *                      Use 1 for static trees, lower numbers if there are to be many subsequent updates.
     * @param NThreads - This will attempt to paralellise the task if N >1;
     *
     */
    private void buildRtreeFromScratch(Node rootNode, final List<Node> geomNodes, double loadingFactor, int NThreads){

        //will at some point contain the logic for paralellisation.
        partition(rootNode, geomNodes, 0, 0.7);


    }

    private class PartitionRunnable implements Runnable {
        final Node root;
        final List<Node> geomNodes;
        final int depth;
        final double loadingFactor;
        PartitionRunnable(Node newIndexNode, List<Node> geomNodes, int depth, double loadingFactor){
            this.root = newIndexNode;
            this.geomNodes = geomNodes;
            this.depth = depth;
            this.loadingFactor = loadingFactor;
        }


        @Override
        public void run() {
            partition(root, geomNodes, depth, loadingFactor);
        }
    }

    /**
     * This will partition a node into
     * @param rootNode
     * @param nodes
     * @param depth
     * @param loadingFactor - what fraction of the max references will be filled.
     * @return
     */
    private void partition(Node rootNode, List<Node> nodes, int depth, final double loadingFactor){
        Comparator<Node> comparator = (depth%2==0) ? new Utilities.ComparatorOnXMin(this.envelopeDecoder) : new Utilities.ComparatorOnYMin(this.envelopeDecoder);
        Collections.sort(nodes, comparator);

        //work out the number of times to partition it:
        final int targetLoading = (int) Math.round(maxNodeReferences*loadingFactor);
        final int height = (int) Math.ceil(Math.log(nodes.size())/Math.log(targetLoading)); //exploit change of base formula
        final int subTreeSize = (int) Math.round(Math.pow(targetLoading, height - 1));
        final int numberOfPartitions = (int) Math.ceil((double) nodes.size() / (double) subTreeSize);

        if(nodes.size() <= targetLoading){
            if(nodes.size() < 35){
                System.out.println("*************HOW IS THIS POSSIBLE!!!!*********** : " + nodes.size());
            }
            for(Node n : nodes){
                if(insertInLeaf(rootNode, n)){
                    adjustPathBoundingBox(rootNode);
                };
            }
            return;
        }else {
            //quick dirty way to partition a set into equal sized disjoint subsets
            List<List<Node>> listOfLists = new ArrayList<List<Node>>(numberOfPartitions);
            for(int i = 0; i < numberOfPartitions; i++){
                listOfLists.add(new ArrayList<Node>(nodes.size()/numberOfPartitions + 1));
            }
            for(int i = 0; i<nodes.size(); i++){
                listOfLists.get(i%numberOfPartitions).add(nodes.get(i));
            }
            //recurse on each partition
            for(List<Node> list : listOfLists){
                Node newIndexNode = database.createNode();
                partition(newIndexNode, list, depth+1, loadingFactor);
                insertIndexNodeOnParent(rootNode, newIndexNode);
            }

            return;
        }

    }

    @Override
    public void remove(long geomNodeId, boolean deleteGeomNode) {
        remove(geomNodeId, deleteGeomNode, true);
    }

    public void remove(long geomNodeId, boolean deleteGeomNode, boolean throwExceptionIfNotFound) {
        Node geomNode = database.getNodeById(geomNodeId);
        if ( geomNode==null && !throwExceptionIfNotFound) {
            //fail silently
            return;
        }

        // be sure geomNode is inside this RTree
        Node indexNode = findLeafContainingGeometryNode(geomNode, throwExceptionIfNotFound);
        if (indexNode == null) return;

        // remove the entry
        final Relationship geometryRtreeReference = geomNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING);
        if (geometryRtreeReference != null) {
            geometryRtreeReference.delete();
        }
        if (deleteGeomNode) deleteNode(geomNode);

        // reorganize the tree if needed
        if (countChildren(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE) == 0) {
            indexNode = deleteEmptyTreeNodes(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE);
            adjustParentBoundingBox(indexNode, RTreeRelationshipTypes.RTREE_CHILD);
        } else {
            adjustParentBoundingBox(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE);
        }

        adjustPathBoundingBox(indexNode);

        countSaved = false;
        totalGeometryCount--;
    }

    private Node deleteEmptyTreeNodes(Node indexNode, RelationshipType relType) {
        if (countChildren(indexNode, relType) == 0) {
            Node parent = getIndexNodeParent(indexNode);
            if (parent != null) {
                indexNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING).delete();
                indexNode.delete();
                return deleteEmptyTreeNodes(parent, RTreeRelationshipTypes.RTREE_CHILD);
            } else {
                // root
                return indexNode;
            }
        } else {
            return indexNode;
        }
    }

    @Override
    public void removeAll(final boolean deleteGeomNodes, final Listener monitor) {
        Node indexRoot = getIndexRoot();

        monitor.begin(count());
        try {
            // delete all geometry nodes
            visitInTx(new SpatialIndexVisitor() {
                public boolean needsToVisit(Envelope indexNodeEnvelope) {
                    return true;
                }

                public void onIndexReference(Node geomNode) {
                    geomNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING).delete();
                    if (deleteGeomNodes) deleteNode(geomNode);

                    monitor.worked(1);
                }
            }, indexRoot.getId());
        } finally {
            monitor.done();
        }

        Transaction tx = database.beginTx();
        try {
            // delete index root relationship
            indexRoot.getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.INCOMING).delete();

            // delete tree
            deleteRecursivelySubtree(indexRoot);

            // delete tree metadata
            Relationship metadataNodeRelationship = getRootNode().getSingleRelationship(RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING);
            Node metadataNode = metadataNodeRelationship.getEndNode();
            metadataNodeRelationship.delete();
            metadataNode.delete();

            tx.success();
        } finally {
            tx.finish();
        }

        countSaved = false;
        totalGeometryCount = 0;
    }

    @Override
    public void clear(final Listener monitor) {
        try (Transaction tx = database.beginTx()) {
            removeAll(false, new NullListener());
            initIndexRoot();
            initIndexMetadata();
            tx.success();
        }
    }

    @Override
    public Envelope getBoundingBox() {
        try (Transaction tx = database.beginTx()) {
            Envelope result = getIndexNodeEnvelope(getIndexRoot());
            tx.success();
            return result;
        }
    }

    @Override
    public int count() {
        saveCount();
        return totalGeometryCount;
    }

    @Override
    public boolean isEmpty() {
        Node indexRoot = getIndexRoot();
        return !indexRoot.hasProperty(INDEX_PROP_BBOX);
    }

    @Override
    public boolean isNodeIndexed(Long geomNodeId) {
        Node geomNode = database.getNodeById(geomNodeId);
        // be sure geomNode is inside this RTree
        return findLeafContainingGeometryNode(geomNode, false) != null;
    }

    public void warmUp() {
        visit(new WarmUpVisitor(), getIndexRoot());
    }

    public Iterable<Node> getAllIndexInternalNodes() {
        return getIndexRoot().traverse(Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE,
                RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);
    }

    @Override
    public Iterable<Node> getAllIndexedNodes() {
        return new IndexNodeToGeometryNodeIterable(getAllIndexInternalNodes());
    }

    private class SearchEvaluator implements ReturnableEvaluator, StopEvaluator {

        private SearchFilter filter;
        private boolean isReturnableNode;
        private boolean isStopNode;

        public SearchEvaluator(SearchFilter filter) {
            this.filter = filter;
        }

        void checkPosition(TraversalPosition position) {
            Relationship rel = position.lastRelationshipTraversed();
            Node node = position.currentNode();
            if (rel == null) {
                isStopNode = false;
                isReturnableNode = false;
            } else if (rel.isType(RTreeRelationshipTypes.RTREE_CHILD)) {
                isReturnableNode = false;
                isStopNode = !filter.needsToVisit(getIndexNodeEnvelope(node));
            } else if (rel.isType(RTreeRelationshipTypes.RTREE_REFERENCE)) {
                isReturnableNode = filter.geometryMatches(node);
                isStopNode = true;
            }
        }

        @Override
        public boolean isReturnableNode(TraversalPosition position) {
            checkPosition(position);
            return isReturnableNode;
        }

        @Override
        public boolean isStopNode(TraversalPosition position) {
            checkPosition(position);
            return isStopNode;
        }
    }

    public SearchResults searchIndex(SearchFilter filter) {
        // TODO: Refactor to new traversal API
        try (Transaction tx = database.beginTx()) {
            SearchEvaluator searchEvaluator = new SearchEvaluator(filter);
            SearchResults results = new SearchResults(getIndexRoot().traverse(Order.DEPTH_FIRST, searchEvaluator,
                    searchEvaluator, RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING,
                    RTreeRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING));
            tx.success();
            return results;
        }
    }

    public void visit(SpatialIndexVisitor visitor, Node indexNode) {
        if (!visitor.needsToVisit(getIndexNodeEnvelope(indexNode))) return;

        try (Transaction tx = database.beginTx()) {
            if (indexNode.hasRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
                // Node is not a leaf
                for (Relationship rel : indexNode.getRelationships(RTreeRelationshipTypes.RTREE_CHILD,
                        Direction.OUTGOING)) {
                    Node child = rel.getEndNode();
                    // collect children results
                    visit(visitor, child);
                }
            } else if (indexNode.hasRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING)) {
                // Node is a leaf
                for (Relationship rel : indexNode.getRelationships(RTreeRelationshipTypes.RTREE_REFERENCE,
                        Direction.OUTGOING)) {
                    visitor.onIndexReference(rel.getEndNode());
                }
            }
            tx.success();
        }
    }

    public Node getIndexRoot() {
        try (Transaction tx = database.beginTx()) {
            Node indexRoot = getRootNode().getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.OUTGOING)
                    .getEndNode();
            tx.success();
            return indexRoot;
        }
    }


    // Private methods

    private Envelope getChildNodeEnvelope(Node child, RelationshipType relType) {
        if (relType.name().equals(RTreeRelationshipTypes.RTREE_REFERENCE.name())) {
            return getLeafNodeEnvelope(child);
        } else {
            return getIndexNodeEnvelope(child);
        }
    }

    /**
     * The leaf nodes belong to the domain model, and as such need to use the
     * layers domain-specific GeometryEncoder for decoding the envelope.
     */
    private Envelope getLeafNodeEnvelope(Node geomNode) {
        return envelopeDecoder.decodeEnvelope(geomNode);
    }

    /**
     * The index nodes do NOT belong to the domain model, and as such need to
     * use the indexes internal knowledge of the index tree and node structure
     * for decoding the envelope.
     */
    protected Envelope getIndexNodeEnvelope(Node indexNode) {
        if (indexNode == null) indexNode = getIndexRoot();
        try (Transaction tx = database.beginTx()) {
            if (!indexNode.hasProperty(INDEX_PROP_BBOX)) {
                // this is ok after an index node split
                tx.success();
                return null;
            }

            double[] bbox = (double[]) indexNode.getProperty(INDEX_PROP_BBOX);
            tx.success();
            // Envelope parameters: xmin, xmax, ymin, ymax
            return new Envelope(bbox[0], bbox[2], bbox[1], bbox[3]);
        }
    }

    private void visitInTx(SpatialIndexVisitor visitor, Long indexNodeId) {
        Node indexNode = database.getNodeById(indexNodeId);
        if (!visitor.needsToVisit(getIndexNodeEnvelope(indexNode))) return;

        if (indexNode.hasRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
            // Node is not a leaf

            // collect children
            List<Long> children = new ArrayList<Long>();
            for (Relationship rel : indexNode.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
                children.add(rel.getEndNode().getId());
            }

            // visit children
            for (Long child : children) {
                visitInTx(visitor, child);
            }
        } else if (indexNode.hasRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING)) {
            // Node is a leaf
            Transaction tx = database.beginTx();
            try {
                for (Relationship rel : indexNode.getRelationships(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING)) {
                    visitor.onIndexReference(rel.getEndNode());
                }

                tx.success();
            } finally {
                tx.finish();
            }
        }
    }

    private void initIndexMetadata() {
        Node layerNode = getRootNode();
        if (layerNode.hasRelationship(RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING)) {
            // metadata already present
            metadataNode = layerNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING).getEndNode();

            maxNodeReferences = (Integer) metadataNode.getProperty("maxNodeReferences");
        } else {
            // metadata initialization
            metadataNode = database.createNode();
            layerNode.createRelationshipTo(metadataNode, RTreeRelationshipTypes.RTREE_METADATA);

            metadataNode.setProperty("maxNodeReferences", maxNodeReferences);
        }

        saveCount();
    }

    private void initIndexRoot() {
        Node layerNode = getRootNode();
        if (!layerNode.hasRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.OUTGOING)) {
            // index initialization
            Node root = database.createNode();
            layerNode.createRelationshipTo(root, RTreeRelationshipTypes.RTREE_ROOT);
        }
    }

    private Node getMetadataNode() {
        if (metadataNode == null) {
            metadataNode = getRootNode().getSingleRelationship(RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING).getEndNode();
        }

        return metadataNode;
    }

    /**
     * Save the geometry count to the database if it has not been saved yet.
     * However, if the count is zero, first do an exhaustive search of the tree
     * and count everything before saving it.
     */
    private void saveCount() {
        if (totalGeometryCount == 0) {
            SpatialIndexRecordCounter counter = new SpatialIndexRecordCounter();
            visit(counter, getIndexRoot());
            totalGeometryCount = counter.getResult();
            countSaved = false;
        }

        if (!countSaved) {
            Transaction tx = database.beginTx();
            try {
                System.out.println("saving new count: " + totalGeometryCount);
                getMetadataNode().setProperty("totalGeometryCount", totalGeometryCount);
                countSaved = true;
                tx.success();
            } finally {
                tx.close();
            }
        }
    }

    private boolean nodeIsLeaf(Node node) {
        return !node.hasRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);
    }

    private Node chooseSubTree(Node parentIndexNode, Node geomRootNode) {
        // children that can contain the new geometry
        List<Node> indexNodes = new ArrayList<Node>();

        // pick the child that contains the new geometry bounding box
        Iterable<Relationship> relationships = parentIndexNode.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);
        for (Relationship relation : relationships) {
            Node indexNode = relation.getEndNode();
            if (getIndexNodeEnvelope(indexNode).contains(getLeafNodeEnvelope(geomRootNode))) {
                indexNodes.add(indexNode);
            }
        }

        if (indexNodes.size() > 1) {
            return chooseIndexNodeWithSmallestArea(indexNodes);
        } else if (indexNodes.size() == 1) {
            return indexNodes.get(0);
        }

        // pick the child that needs the minimum enlargement to include the new geometry
        double minimumEnlargement = Double.POSITIVE_INFINITY;
        relationships = parentIndexNode.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);
        for (Relationship relation : relationships) {
            Node indexNode = relation.getEndNode();
            double enlargementNeeded = getAreaEnlargement(indexNode, geomRootNode);

            if (enlargementNeeded < minimumEnlargement) {
                indexNodes.clear();
                indexNodes.add(indexNode);
                minimumEnlargement = enlargementNeeded;
            } else if (enlargementNeeded == minimumEnlargement) {
                indexNodes.add(indexNode);
            }
        }

        if (indexNodes.size() > 1) {
            return chooseIndexNodeWithSmallestArea(indexNodes);
        } else if (indexNodes.size() == 1) {
            return indexNodes.get(0);
        } else {
            // this shouldn't happen
            throw new RuntimeException("No IndexNode found for new geometry");
        }
    }

    private double getAreaEnlargement(Node indexNode, Node geomRootNode) {
        Envelope before = getIndexNodeEnvelope(indexNode);

        Envelope after = getLeafNodeEnvelope(geomRootNode);
        after.expandToInclude(before);

        return getArea(after) - getArea(before);
    }

    private Node chooseIndexNodeWithSmallestArea(List<Node> indexNodes) {
        Node result = null;
        double smallestArea = -1;

        for (Node indexNode : indexNodes) {
            double area = getArea(getIndexNodeEnvelope(indexNode));
            if (result == null || area < smallestArea) {
                result = indexNode;
                smallestArea = area;
            }
        }

        return result;
    }

    private int countChildren(Node indexNode, RelationshipType relationshipType) {
        int counter = 0;
        Iterator<Relationship> iterator = indexNode.getRelationships(relationshipType, Direction.OUTGOING).iterator();
        while (iterator.hasNext()) {
            iterator.next();
            counter++;
        }
        return counter;
    }

    /**
     * @return is enlargement needed?
     */
    private boolean insertInLeaf(Node indexNode, Node geomRootNode) {
        return addChild(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE, geomRootNode);
    }

    private void splitAndAdjustPathBoundingBox(Node indexNode) {
        // create a new node and distribute the entries
        Node newIndexNode = quadraticSplit(indexNode);
        Node parent = getIndexNodeParent(indexNode);
        if (parent == null) {
            // if indexNode is the root
            createNewRoot(indexNode, newIndexNode);
        } else {
            expandParentBoundingBoxAfterNewChild(parent, (double[]) indexNode.getProperty(INDEX_PROP_BBOX));

            addChild(parent, RTreeRelationshipTypes.RTREE_CHILD, newIndexNode);

            if (countChildren(parent, RTreeRelationshipTypes.RTREE_CHILD) > maxNodeReferences) {
                splitAndAdjustPathBoundingBox(parent);
            } else {
                adjustPathBoundingBox(parent);
            }
        }
    }

    private Node quadraticSplit(Node indexNode) {
        if (nodeIsLeaf(indexNode)) return quadraticSplit(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE);
        else return quadraticSplit(indexNode, RTreeRelationshipTypes.RTREE_CHILD);
    }

    private Node quadraticSplit(Node indexNode, RelationshipType relationshipType) {
        List<Node> entries = new ArrayList<Node>();

        Iterable<Relationship> relationships = indexNode.getRelationships(relationshipType, Direction.OUTGOING);
        for (Relationship relationship : relationships) {
            entries.add(relationship.getEndNode());
            relationship.delete();
        }

        // pick two seed entries such that the dead space is maximal
        Node seed1 = null;
        Node seed2 = null;
        double worst = Double.NEGATIVE_INFINITY;
        for (Node e : entries) {
            Envelope eEnvelope = getChildNodeEnvelope(e, relationshipType);
            for (Node e1 : entries) {
                Envelope e1Envelope = getChildNodeEnvelope(e1, relationshipType);
                double deadSpace = getArea(createEnvelope(eEnvelope, e1Envelope)) - getArea(eEnvelope) - getArea(e1Envelope);
                if (deadSpace > worst) {
                    worst = deadSpace;
                    seed1 = e;
                    seed2 = e1;
                }
            }
        }

        List<Node> group1 = new ArrayList<Node>();
        group1.add(seed1);
        Envelope group1envelope = getChildNodeEnvelope(seed1, relationshipType);

        List<Node> group2 = new ArrayList<Node>();
        group2.add(seed2);
        Envelope group2envelope = getChildNodeEnvelope(seed2, relationshipType);

        entries.remove(seed1);
        entries.remove(seed2);
        while (entries.size() > 0) {
            // compute the cost of inserting each entry
            List<Node> bestGroup = null;
            Envelope bestGroupEnvelope = null;
            Node bestEntry = null;
            double expansionMin = Double.POSITIVE_INFINITY;
            for (Node e : entries) {
                Envelope nodeEnvelope = getChildNodeEnvelope(e, relationshipType);
                double expansion1 = getArea(createEnvelope(nodeEnvelope, group1envelope)) - getArea(group1envelope);
                double expansion2 = getArea(createEnvelope(nodeEnvelope, group2envelope)) - getArea(group2envelope);

                if (expansion1 < expansion2 && expansion1 < expansionMin) {
                    bestGroup = group1;
                    bestGroupEnvelope = group1envelope;
                    bestEntry = e;
                    expansionMin = expansion1;
                } else if (expansion2 < expansion1 && expansion2 < expansionMin) {
                    bestGroup = group2;
                    bestGroupEnvelope = group2envelope;
                    bestEntry = e;
                    expansionMin = expansion2;
                } else if (expansion1 == expansion2 && expansion1 < expansionMin) {
                    // in case of equality choose the group with the smallest area
                    if (getArea(group1envelope) < getArea(group2envelope)) {
                        bestGroup = group1;
                        bestGroupEnvelope = group1envelope;
                    } else {
                        bestGroup = group2;
                        bestGroupEnvelope = group2envelope;
                    }
                    bestEntry = e;
                    expansionMin = expansion1;
                }
            }

            // insert the best candidate entry in the best group
            bestGroup.add(bestEntry);
            bestGroupEnvelope.expandToInclude(getChildNodeEnvelope(bestEntry, relationshipType));

            entries.remove(bestEntry);
        }

        // reset bounding box and add new children
        indexNode.removeProperty(INDEX_PROP_BBOX);
        for (Node node : group1) {
            addChild(indexNode, relationshipType, node);
        }

        // create new node from split
        Node newIndexNode = database.createNode();
        for (Node node: group2) {
            addChild(newIndexNode, relationshipType, node);
        }

        return newIndexNode;
    }

    private void createNewRoot(Node oldRoot, Node newIndexNode) {
        Node newRoot = database.createNode();
        addChild(newRoot, RTreeRelationshipTypes.RTREE_CHILD, oldRoot);
        addChild(newRoot, RTreeRelationshipTypes.RTREE_CHILD, newIndexNode);

        Node layerNode = getRootNode();
        layerNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.OUTGOING).delete();
        layerNode.createRelationshipTo(newRoot, RTreeRelationshipTypes.RTREE_ROOT);
    }

    private boolean addChild(Node parent, RelationshipType type, Node newChild) {
        Envelope childEnvelope = getChildNodeEnvelope(newChild, type);
        double[] childBBox = new double[] {
                childEnvelope.getMinX(), childEnvelope.getMinY(),
                childEnvelope.getMaxX(), childEnvelope.getMaxY() };
        parent.createRelationshipTo(newChild, type);
        return expandParentBoundingBoxAfterNewChild(parent, childBBox);
    }

    private void adjustPathBoundingBox(Node indexNode) {
        Node parent = getIndexNodeParent(indexNode);
        if (parent != null) {
            if (adjustParentBoundingBox(parent, RTreeRelationshipTypes.RTREE_CHILD)) {
                // entry has been modified: adjust the path for the parent
                adjustPathBoundingBox(parent);
            }
        }
    }

    /**
     * Fix an IndexNode bounding box after a child has been removed
     * @param indexNode
     * @return true if something has changed
     */
    private boolean adjustParentBoundingBox(Node indexNode, RelationshipType relationshipType) {
        double[] old = null;
        if (indexNode.hasProperty(INDEX_PROP_BBOX)) {
            old = (double[]) indexNode.getProperty(INDEX_PROP_BBOX);
        }

        Envelope bbox = null;

        Iterator<Relationship> iterator = indexNode.getRelationships(relationshipType, Direction.OUTGOING).iterator();
        while (iterator.hasNext()) {
            Node childNode = iterator.next().getEndNode();

            if (bbox == null) {
                bbox = new Envelope(getChildNodeEnvelope(childNode, relationshipType));
            } else {
                bbox.expandToInclude(getChildNodeEnvelope(childNode, relationshipType));
            }
        }

        if (bbox == null) {
            // this could happen in an empty tree
            bbox = new Envelope(0, 0, 0, 0);
        }

        if (old == null || old.length != 4 ||
                bbox.getMinX() != old[0] ||
                bbox.getMinY() != old[1] ||
                bbox.getMaxX() != old[2] ||
                bbox.getMaxY() != old[3])
        {
            indexNode.setProperty(INDEX_PROP_BBOX, new double[] { bbox.getMinX(), bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY() });
            return true;
        } else {
            return false;
        }
    }

    /**
     * Adjust IndexNode bounding box according to the new child inserted
     * @param parent IndexNode
     * @param childBBox geomNode inserted
     * @return is bbox changed?
     */
    private boolean expandParentBoundingBoxAfterNewChild(Node parent, double[] childBBox) {
        if (!parent.hasProperty(INDEX_PROP_BBOX)) {
            parent.setProperty(INDEX_PROP_BBOX, new double[] { childBBox[0], childBBox[1], childBBox[2], childBBox[3] });
            return true;
        }

        double[] parentBBox = (double[]) parent.getProperty(INDEX_PROP_BBOX);

        boolean valueChanged = setMin(parentBBox, childBBox, 0);
        valueChanged = setMin(parentBBox, childBBox, 1) || valueChanged;
        valueChanged = setMax(parentBBox, childBBox, 2) || valueChanged;
        valueChanged = setMax(parentBBox, childBBox, 3) || valueChanged;

        if (valueChanged) {
            parent.setProperty(INDEX_PROP_BBOX, parentBBox);

        }

        return valueChanged;
    }

    private boolean setMin(double[] parent, double[] child, int index) {
        if (parent[index] > child[index]) {
            parent[index] = child[index];
            return true;
        } else {
            return false;
        }
    }

    private boolean setMax(double[] parent, double[] child, int index) {
        if (parent[index] < child[index]) {
            parent[index] = child[index];
            return true;
        } else {
            return false;
        }
    }

    private Node getIndexNodeParent(Node indexNode) {
        Relationship relationship = indexNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING);
        if (relationship == null) return null;
        else return relationship.getStartNode();
    }

    private double getArea(Envelope e) {
        return e.getWidth() * e.getHeight();
    }

    private void deleteRecursivelySubtree(Node indexNode) {
        for (Relationship relationship : indexNode.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
            deleteRecursivelySubtree(relationship.getEndNode());
        }

        Relationship relationshipWithFather = indexNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING);
        // the following check is needed because rootNode doesn't have this relationship
        if (relationshipWithFather != null) {
            relationshipWithFather.delete();
        }

        indexNode.delete();
    }

    protected Node findLeafContainingGeometryNode(Node geomNode, boolean throwExceptionIfNotFound) {
        if (!geomNode.hasRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING)) {
            if (throwExceptionIfNotFound) {
                throw new RuntimeException("GeometryNode not indexed with an RTree: " + geomNode.getId());
            } else {
                return null;
            }
        }

        Node indexNodeLeaf = geomNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING).getStartNode();

        Node root = null;
        Node child = indexNodeLeaf;
        while (root == null) {
            Node parent = getIndexNodeParent(child);
            if (parent == null) root = child;
            else child = parent;
        }

        if (root.getId() != getIndexRoot().getId()) {
            if (throwExceptionIfNotFound) {
                throw new RuntimeException("GeometryNode not indexed in this RTree: " + geomNode.getId());
            } else {
                return null;
            }
        } else {
            return indexNodeLeaf;
        }
    }

    private void deleteNode(Node node) {
        for (Relationship r : node.getRelationships()) {
            r.delete();
        }
        node.delete();
    }

    private Node getRootNode() {
        return rootNode;
    }

    /**
     * Create a bounding box encompassing the two bounding boxes passed in.
     */
    private static Envelope createEnvelope(Envelope e, Envelope e1) {
        Envelope result = new Envelope(e);
        result.expandToInclude(e1);
        return result;
    }


    // Attributes

    public GraphDatabaseService getDatabase() {
        return database;
    }

    private GraphDatabaseService database;

    private Node rootNode;
    private EnvelopeDecoder envelopeDecoder;
    private int maxNodeReferences;

    private Node metadataNode;
    private int totalGeometryCount = 0;
    private boolean countSaved = false;

    // Private classes

    private class WarmUpVisitor implements SpatialIndexVisitor {

        public boolean needsToVisit(Envelope indexNodeEnvelope) { return true; }

        public void onIndexReference(Node geomNode) { }
    }

    /**
     * In order to wrap one iterable or iterator in another that converts the
     * objects from one type to another without loading all into memory, we need
     * to use this ugly java-magic. Man, I miss Ruby right now!
     *
     * @author Craig
     */
    private class IndexNodeToGeometryNodeIterable implements Iterable<Node> {

        private Iterator<Node> allIndexNodeIterator;

        private class GeometryNodeIterator implements Iterator<Node> {

            Iterator<Node> geometryNodeIterator = null;

            public boolean hasNext() {
                checkGeometryNodeIterator();
                return geometryNodeIterator != null && geometryNodeIterator.hasNext();
            }

            public Node next() {
                checkGeometryNodeIterator();
                return geometryNodeIterator == null ? null : geometryNodeIterator.next();
            }

            private void checkGeometryNodeIterator() {
                while ((geometryNodeIterator == null || !geometryNodeIterator.hasNext()) && allIndexNodeIterator.hasNext()) {
                    geometryNodeIterator = allIndexNodeIterator.next().traverse(Order.DEPTH_FIRST, StopEvaluator.DEPTH_ONE,
                            ReturnableEvaluator.ALL_BUT_START_NODE, RTreeRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING)
                            .iterator();
                }
            }

            public void remove() {
            }
        }

        public IndexNodeToGeometryNodeIterable(Iterable<Node> allIndexNodes) {
            this.allIndexNodeIterator = allIndexNodes.iterator();
        }

        public Iterator<Node> iterator() {
            return new GeometryNodeIterator();
        }


    }

    private class indexNodeAreaComparator implements Comparator<Node>{

        @Override
        public int compare(Node o1, Node o2) {
            return Double.compare(getIndexNodeEnvelope(o1).getArea(), getIndexNodeEnvelope(o2).getArea());
        }
    }
}