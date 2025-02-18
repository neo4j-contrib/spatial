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
package org.neo4j.gis.spatial.rtree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.neo4j.gis.spatial.encoders.Configurable;
import org.neo4j.gis.spatial.index.SpatialIndexWriter;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.rtree.filter.SearchResults;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.impl.StandardExpander;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.PathEvaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;

/**
 * This is an in-graph index utilizing the concepts of an R-Tree.
 * As in in-graph index it is part of the data model, and nodes explicitly added to this index
 * are connected to the existing data model.
 * Queries to the index also need to be explicit.
 */
public class RTreeIndex implements SpatialIndexWriter, Configurable {

	public static final String INDEX_PROP_BBOX = "bbox";

	public static final String KEY_SPLIT = "splitMode";
	public static final String QUADRATIC_SPLIT = "quadratic";
	public static final String GREENES_SPLIT = "greene";

	public static final String KEY_MAX_NODE_REFERENCES = "maxNodeReferences";
	public static final String KEY_SHOULD_MERGE_TREES = "shouldMergeTrees";
	public static final String REFERENCE_RELATIONSHIP_TYPE = "referenceRelationshipType";
	public static final int MIN_MAX_NODE_REFERENCES = 10;
	public static final int MAX_MAX_NODE_REFERENCES = 1000000;
	public static final int DEFAULT_MAX_NODE_REFERENCES = 100;

	private TreeMonitor monitor;
	private String rootNodeId;
	private EnvelopeDecoder envelopeDecoder;
	private int maxNodeReferences;
	private String splitMode = GREENES_SPLIT;
	private boolean shouldMergeTrees = false;
	private RelationshipType referenceRelationshipType = RTreeRelationshipTypes.RTREE_REFERENCE;

	private int totalGeometryCount = 0;
	private boolean countSaved = false;

	@Override
	public void addMonitor(TreeMonitor monitor) {
		this.monitor = monitor;
	}

	public void init(Transaction tx, Node layerNode, EnvelopeDecoder envelopeDecoder, int maxNodeReferences) {
		this.rootNodeId = layerNode.getElementId();
		this.envelopeDecoder = envelopeDecoder;
		this.maxNodeReferences = maxNodeReferences;
		monitor = new EmptyMonitor();
		if (envelopeDecoder == null) {
			throw new NullPointerException("envelopeDecoder is NULL");
		}
		initIndexRoot(tx);
		initIndexMetadata(tx);
	}

	// Public methods
	@Override
	public EnvelopeDecoder getEnvelopeDecoder() {
		return this.envelopeDecoder;
	}

	@Override
	public void setConfiguration(String jsonConfig) {
		if (jsonConfig == null || jsonConfig.isBlank()) {
			return;
		}
		JSONObject jsonObject = (JSONObject) JSONValue.parse(jsonConfig);
		HashMap<String, Object> config = new HashMap<>();
		for (Object key : jsonObject.keySet()) {
			config.put(key.toString(), jsonObject.get(key));
		}
		configure(config);
	}

	@Override
	public String getConfiguration() {
		HashMap<String, Object> config = new HashMap<>();
		config.put(KEY_SPLIT, this.splitMode);
		config.put(KEY_MAX_NODE_REFERENCES, this.maxNodeReferences);
		config.put(KEY_SHOULD_MERGE_TREES, this.shouldMergeTrees);
		config.put(REFERENCE_RELATIONSHIP_TYPE, this.referenceRelationshipType.name());
		return JSONObject.toJSONString(config);
	}

	@Override
	public void configure(Map<String, Object> config) {
		config.forEach((key, rawValue) -> {
			switch (key) {
				case KEY_SPLIT:
					String value = rawValue.toString();
					switch (value) {
						case QUADRATIC_SPLIT:
						case GREENES_SPLIT:
							splitMode = value;
							break;
						default:
							throw new IllegalArgumentException(
									"No such RTreeIndex value for '" + key + "': " + rawValue);
					}
					break;
				case KEY_MAX_NODE_REFERENCES:
					int intValue = Integer.parseInt(rawValue.toString());
					if (intValue < MIN_MAX_NODE_REFERENCES) {
						throw new IllegalArgumentException(
								"RTreeIndex does not allow " + key + " less than " + MIN_MAX_NODE_REFERENCES);
					}
					if (intValue > MAX_MAX_NODE_REFERENCES) {
						throw new IllegalArgumentException(
								"RTreeIndex does not allow " + key + " greater than " + MAX_MAX_NODE_REFERENCES);
					}
					this.maxNodeReferences = intValue;
					break;
				case KEY_SHOULD_MERGE_TREES:
					this.shouldMergeTrees = Boolean.parseBoolean(rawValue.toString());
					break;
				case REFERENCE_RELATIONSHIP_TYPE:
					this.referenceRelationshipType = RelationshipType.withName(rawValue.toString());
					break;
				default:
					throw new IllegalArgumentException("No such RTreeIndex configuration key: " + key);
			}
		});
	}

	@Override
	public void add(Transaction tx, Node geomNode) {
		// initialize the search with root
		Node parent = getIndexRoot(tx);
		addBelow(tx, parent, geomNode);
		countSaved = false;
		totalGeometryCount++;
	}

	/**
	 * This method will add the node somewhere below the parent.
	 */
	private void addBelow(Transaction tx, Node parent, Node geomNode) {
		// choose a path down to a leaf
		while (!nodeIsLeaf(parent)) {
			parent = chooseSubTree(parent, geomNode);
		}
		// bbox enlargement needed
		if (countChildren(parent, referenceRelationshipType) >= maxNodeReferences) {
			insertInLeaf(parent, geomNode);
			splitAndAdjustPathBoundingBox(tx, parent);
		} else if (insertInLeaf(parent, geomNode)) {
			adjustPathBoundingBox(parent);
		}
	}


	/**
	 * Use this method if you want to insert an index node as a child of a given index node. This will recursively
	 * update the bounding boxes above the parent to keep the tree consistent.
	 */
	private void insertIndexNodeOnParent(Transaction tx, Node parent, Node child) {
		int numChildren = countChildren(parent, RTreeRelationshipTypes.RTREE_CHILD);
		boolean needExpansion = addChild(parent, RTreeRelationshipTypes.RTREE_CHILD, child);
		if (numChildren < maxNodeReferences) {
			if (needExpansion) {
				adjustPathBoundingBox(parent);
			}
		} else {
			splitAndAdjustPathBoundingBox(tx, parent);
		}
	}


	/**
	 * Depending on the size of the incumbent tree, this will either attempt to rebuild the entire index from scratch
	 * (strategy used if the insert larger than 40% of the current tree size - may give heap out of memory errors for
	 * large inserts as has O(n) space complexity in the total tree size. It has n*log(n) time complexity. See function
	 * partition for more details.) or it will insert using the method of seeded clustering, where you attempt to use
	 * the
	 * existing tree structure to partition your data.
	 * <p>
	 * This is based on the Paper "Bulk Insertion for R-trees by seeded clustering" by T.Lee, S.Lee & B Moon.
	 * Repeated use of this strategy will lead to degraded query performance, especially if used for
	 * many relatively small insertions compared to tree size. Though not worse than one by one insertion.
	 * In practice, it should be fine for most uses.
	 */
	@Override
	public void add(Transaction tx, List<Node> geomNodes) {

		//If the insertion is large relative to the size of the tree, simply rebuild the whole tree.
		if (geomNodes.size() > totalGeometryCount * 0.4) {
			List<Node> nodesToAdd = new ArrayList<>(geomNodes.size() + totalGeometryCount);
			for (Node n : getAllIndexedNodes(tx)) {
				nodesToAdd.add(n);
			}
			nodesToAdd.addAll(geomNodes);
			detachGeometryNodes(tx, false, getIndexRoot(tx), new NullListener());
			deleteTreeBelow(getIndexRoot(tx));
			buildRtreeFromScratch(tx, getIndexRoot(tx), decodeGeometryNodeEnvelopes(nodesToAdd), 0.7);
			countSaved = false;
			totalGeometryCount = nodesToAdd.size();
			monitor.addNbrRebuilt(this, tx);
		} else {

			List<NodeWithEnvelope> outliers = bulkInsertion(tx, getIndexRoot(tx), getHeight(getIndexRoot(tx), 0),
					decodeGeometryNodeEnvelopes(geomNodes), 0.7);
			countSaved = false;
			totalGeometryCount = totalGeometryCount + (geomNodes.size() - outliers.size());
			for (NodeWithEnvelope n : outliers) {
				add(tx, n.node);
			}
		}
	}

	private List<NodeWithEnvelope> decodeGeometryNodeEnvelopes(List<Node> nodes) {
		return nodes.stream().map(GeometryNodeWithEnvelope::new).collect(Collectors.toList());
	}

	public static class NodeWithEnvelope {

		public final Envelope envelope;
		Node node;

		public NodeWithEnvelope(Node node, Envelope envelope) {
			this.node = node;
			this.envelope = envelope;
		}

		/**
		 * Ensure this node is valid in the specified transaction
		 */
		public NodeWithEnvelope refresh(Transaction tx) {
			this.node = tx.getNodeByElementId(this.node.getElementId());
			return this;
		}
	}

	public class GeometryNodeWithEnvelope extends NodeWithEnvelope {

		GeometryNodeWithEnvelope(Node node) {
			super(node, envelopeDecoder.decodeEnvelope(node));
		}
	}

	/**
	 * Returns the height of the tree, starting with the rootNode and adding one for each subsequent level. Relies on
	 * the
	 * balanced property of the RTree that all leaves are on the same level and no index nodes are empty. In the
	 * convention
	 * the index is level 0, so if there is just the index and the leaf nodes, the leaf nodes are level one and the
	 * height is one.
	 * Thus, the lowest level is 1.
	 */
	static int getHeight(Node rootNode, int height) {
		try (var relationships = rootNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
			if (relationships.iterator().hasNext()) {
				return getHeight(relationships.iterator().next().getEndNode(), height + 1);
			}
			// Add one to account for the step to leaf nodes.
			return height + 1; // todo should this really be +1 ?
		}
	}

	static List<NodeWithEnvelope> getIndexChildren(Node rootNode) {
		List<NodeWithEnvelope> result = new ArrayList<>();
		try (var relationships = rootNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
			for (Relationship r : relationships) {
				Node child = r.getEndNode();
				result.add(new NodeWithEnvelope(child, getIndexNodeEnvelope(child)));
			}
		}
		return result;
	}

	private static List<NodeWithEnvelope> getIndexChildren(Node rootNode, int depth) {
		if (depth < 1) {
			throw new IllegalArgumentException("Depths must be at least one");
		}

		List<NodeWithEnvelope> rootChildren = getIndexChildren(rootNode);
		if (depth == 1) {
			return rootChildren;
		}
		List<NodeWithEnvelope> result = new ArrayList<>(rootChildren.size() * 5);
		for (NodeWithEnvelope child : rootChildren) {
			result.addAll(getIndexChildren(child.node, depth - 1));
		}
		return result;
	}

	private List<NodeWithEnvelope> bulkInsertion(Transaction tx, Node rootNode, int rootNodeHeight,
			final List<NodeWithEnvelope> geomNodes, final double loadingFactor) {
		List<NodeWithEnvelope> children = getIndexChildren(rootNode);
		if (children.isEmpty()) {
			return geomNodes;
		}
		children.sort(new IndexNodeAreaComparator());

		Map<NodeWithEnvelope, List<NodeWithEnvelope>> map = new HashMap<>(children.size());
		int nodesPerRootSubTree = Math.max(16, geomNodes.size() / children.size());
		for (NodeWithEnvelope n : children) {
			map.put(n, new ArrayList<>(nodesPerRootSubTree));
		}

		// The outliers are those nodes which do not fit into the existing tree hierarchy.
		List<NodeWithEnvelope> outliers = new ArrayList<>(geomNodes.size() / 10); // 10% outliers
		for (NodeWithEnvelope n : geomNodes) {
			Envelope env = n.envelope;
			boolean flag = true;

			//exploits that the iterator returns the list inorder, which is sorted by size, as above. Thus, child
			//is always added to the smallest existing envelope which contains it.
			for (NodeWithEnvelope c : children) {
				if (c.envelope.contains(env)) {
					map.get(c).add(n); //add to the smallest area envelope which contains the child;
					flag = false;
					break;
				}
			}
			// else add to outliers.
			if (flag) {
				outliers.add(n);
			}
		}
		for (NodeWithEnvelope child : children) {
			List<NodeWithEnvelope> cluster = map.get(child);

			if (cluster.isEmpty()) {
				continue;
			}

			// todo move each branch into a named method
			int expectedHeight = expectedHeight(loadingFactor, cluster.size());

			//In a rtree is this height it will add as a single child to the current child node.
			int currentRTreeHeight = rootNodeHeight - 2;
			if (expectedHeight < currentRTreeHeight) {
				monitor.addCase("h_i < l_t ");
				//if the height is smaller than that recursively sort and split.
				outliers.addAll(bulkInsertion(tx, child.node, rootNodeHeight - 1, cluster, loadingFactor));
			} //if constructed tree is the correct size insert it here.
			else //Do not create underfull nodes, instead use the add logic, except we know the root not to add them too.
				//this handles the case where the number of nodes in a cluster is small.
				if (expectedHeight == currentRTreeHeight) {
					if (cluster.size() < maxNodeReferences * loadingFactor / 2) {
						monitor.addCase("h_i == l_t && small cluster");
						// getParent because addition might cause a split. This strategy not ideal,
						// but does tend to limit overlap more than adding to the child exclusively.

						for (NodeWithEnvelope n : cluster) {
							addBelow(tx, rootNode, n.node);
						}
					} else {
						monitor.addCase("h_i == l_t && big cluster");
						Node newRootNode = tx.createNode();
						buildRtreeFromScratch(tx, newRootNode, cluster, loadingFactor);
						if (shouldMergeTrees) {
							NodeWithEnvelope nodeWithEnvelope = new NodeWithEnvelope(newRootNode,
									getIndexNodeEnvelope(newRootNode));
							List<NodeWithEnvelope> insert = new ArrayList<>(
									Collections.singletonList(nodeWithEnvelope));
							monitor.beforeMergeTree(child.node, insert);
							mergeTwoSubtrees(tx, child, insert);
							monitor.afterMergeTree(child.node);
						} else {
							insertIndexNodeOnParent(tx, child.node, newRootNode);
						}
					}
				} else {
					Node newRootNode = tx.createNode();
					buildRtreeFromScratch(tx, newRootNode, cluster, loadingFactor);
					int newHeight = getHeight(newRootNode, 0);
					if (newHeight == 1) {
						monitor.addCase("h_i > l_t (d==1)");
						try (var relationships = newRootNode.getRelationships(referenceRelationshipType)) {
							for (Relationship geom : relationships) {
								addBelow(tx, child.node, geom.getEndNode());
								geom.delete();
							}
						}
					} else {
						monitor.addCase("h_i > l_t (d>1)");
						int insertDepth = newHeight - (currentRTreeHeight);
						List<NodeWithEnvelope> childrenToBeInserted = getIndexChildren(newRootNode, insertDepth);
						for (NodeWithEnvelope n : childrenToBeInserted) {
							Relationship relationship = n.node.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD,
									Direction.INCOMING);
							relationship.delete();
							if (!shouldMergeTrees) {
								insertIndexNodeOnParent(tx, child.node, n.node);
							}
						}
						if (shouldMergeTrees) {
							monitor.beforeMergeTree(child.node, childrenToBeInserted);
							mergeTwoSubtrees(tx, child, childrenToBeInserted);
							monitor.afterMergeTree(child.node);
						}
					}
					// todo wouldn't it be better for this temporary tree to only live in memory?
					deleteRecursivelySubtree(newRootNode, null); // remove the buffer tree remnants
				}
		}
		monitor.addSplit(rootNode); // for debugging via images

		return outliers;
	}

	static class NodeTuple {

		private final double overlap;
		final NodeWithEnvelope left;
		final NodeWithEnvelope right;

		NodeTuple(NodeWithEnvelope left, NodeWithEnvelope right) {
			this.left = left;
			this.right = right;
			this.overlap = left.envelope.overlap(right.envelope);
		}

		boolean contains(NodeWithEnvelope entry) {
			return left.node.equals(entry.node) || right.node.equals(entry.node);
		}
	}

	protected void mergeTwoSubtrees(Transaction tx, NodeWithEnvelope parent, List<NodeWithEnvelope> right) {
		ArrayList<NodeTuple> pairs = new ArrayList<>();
		HashSet<NodeWithEnvelope> disconnectedChildren = new HashSet<>();
		List<NodeWithEnvelope> left = getIndexChildren(parent.node);
		for (NodeWithEnvelope leftNode : left) {
			for (NodeWithEnvelope rightNode : right) {
				NodeTuple pair = new NodeTuple(leftNode, rightNode);
				if (pair.overlap > 0.1) {
					pairs.add(pair);
				}
			}
		}
		pairs.sort(Comparator.comparingDouble(o -> o.overlap));
		while (!pairs.isEmpty()) {
			NodeTuple pair = pairs.remove(pairs.size() - 1);
			Envelope merged = new Envelope(pair.left.envelope);
			merged.expandToInclude(pair.right.envelope);
			NodeWithEnvelope newNode = new NodeWithEnvelope(pair.left.node, merged);
			setIndexNodeEnvelope(newNode.node, newNode.envelope);
			List<NodeWithEnvelope> rightChildren = getIndexChildren(pair.right.node);
			pairs.removeIf(t -> t.contains(pair.left) || t.contains(pair.right));
			try (var relationships = pair.right.node.getRelationships()) {
				for (Relationship rel : relationships) {
					rel.delete();
				}
			}
			disconnectedChildren.add(pair.right);
			mergeTwoSubtrees(tx, newNode, rightChildren);
		}

		right.removeIf(disconnectedChildren::contains);
		disconnectedChildren.forEach(t -> t.node.delete());

		for (NodeWithEnvelope n : right) {
			n.node.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING);
			parent.node.createRelationshipTo(n.node, RTreeRelationshipTypes.RTREE_CHILD);
			parent.envelope.expandToInclude(n.envelope);
		}
		setIndexNodeEnvelope(parent.node, parent.envelope);
		if (countChildren(parent.node, RTreeRelationshipTypes.RTREE_CHILD) > maxNodeReferences) {
			splitAndAdjustPathBoundingBox(tx, parent.node);
		} else {
			adjustPathBoundingBox(parent.node);
		}
	}

	private int expectedHeight(double loadingFactor, int size) {
		if (size == 1) {
			return 1;
		}
		final int targetLoading = (int) Math.floor(maxNodeReferences * loadingFactor);
		return (int) Math.ceil(Math.log(size) / Math.log(targetLoading)); //exploit change of base formula

	}

	/**
	 * This algorithm is based on Overlap Minimizing Top-down Bulk Loading Algorithm for R-tree by T Lee and S Lee.
	 * This is effectively a wrapper function around the function Partition which will attempt to parallelise the task.
	 * This can work better or worse since the top level may have as few as two nodes, in which case it fails is not
	 * optimal.
	 * The loadingFactor must be between 0.1 and 1, this is how full each node will be, approximately.
	 * Use 1 for static trees (will not be added to after build built), lower numbers if there are to be many subsequent
	 * updates.
	 * //TODO - Better parallelisation strategy.
	 */
	private void buildRtreeFromScratch(Transaction tx, Node rootNode, final List<NodeWithEnvelope> geomNodes,
			double loadingFactor) {
		partition(tx, rootNode, geomNodes, 0, loadingFactor);
	}

	/**
	 * This will partition a collection of nodes under the specified index node. The nodes are clustered into one
	 * or more groups based on the loading factor, and the tree is expanded if necessary. If the nodes all fit
	 * into the parent, they are added directly, otherwise the depth is increased and partition called for each
	 * cluster at the deeper depth based on a new root node for each cluster.
	 */
	private void partition(Transaction tx, Node indexNode, List<NodeWithEnvelope> nodes, int depth,
			final double loadingFactor) {

		// We want to split by the longest dimension to avoid degrading into extremely thin envelopes
		int longestDimension = findLongestDimension(nodes);

		// Sort the entries by the longest dimension and then create envelopes around left and right halves
		nodes.sort(new SingleDimensionNodeEnvelopeComparator(longestDimension));

		//work out the number of times to partition it:
		final int targetLoading = (int) Math.round(maxNodeReferences * loadingFactor);
		int nodeCount = nodes.size();

		if (nodeCount <= targetLoading) {
			// We have few enough nodes to add them directly to the current index node
			boolean expandRootNodeBoundingBox = false;
			for (NodeWithEnvelope n : nodes) {
				expandRootNodeBoundingBox |= insertInLeaf(indexNode, n.node);
			}
			if (expandRootNodeBoundingBox) {
				adjustPathBoundingBox(indexNode);
			}
		} else {
			// We have more geometries than can fit in the current index node - create clusters and index them
			final int height = expectedHeight(loadingFactor, nodeCount); //exploit change of base formula
			final int subTreeSize = (int) Math.round(Math.pow(targetLoading, height - 1));
			final int numberOfPartitions = (int) Math.ceil((double) nodeCount / (double) subTreeSize);
			// - TODO change this to use the sort function above
			List<List<NodeWithEnvelope>> partitions = partitionList(nodes, numberOfPartitions);

			//recurse on each partition
			for (List<NodeWithEnvelope> partition : partitions) {
				Node newIndexNode = tx.createNode();
				if (partition.size() > 1) {
					partition(tx, newIndexNode, partition, depth + 1, loadingFactor);
				} else {
					addBelow(tx, newIndexNode, partition.get(0).node);
				}
				insertIndexNodeOnParent(tx, indexNode, newIndexNode);
			}
			monitor.addSplit(indexNode);
		}
	}

	// quick dirty way to partition a set into equal sized disjoint subsets
	// - TODO why not use list.sublist() without copying ?

	private static List<List<NodeWithEnvelope>> partitionList(List<NodeWithEnvelope> nodes, int numberOfPartitions) {
		int nodeCount = nodes.size();
		List<List<NodeWithEnvelope>> partitions = new ArrayList<>(numberOfPartitions);

		int partitionSize = nodeCount
				/ numberOfPartitions; //it is critical that partitionSize is always less than the target loading.
		if (nodeCount % numberOfPartitions > 0) {
			partitionSize++;
		}
		for (int i = 0; i < numberOfPartitions; i++) {
			partitions.add(nodes.subList(i * partitionSize, Math.min((i + 1) * partitionSize, nodeCount)));
		}
		return partitions;
	}

	@Override
	public void remove(Transaction tx, String geomNodeId, boolean deleteGeomNode, boolean throwExceptionIfNotFound) {
		Node geomNode = null;
		// getNodeByElementId throws NotFoundException if node is already removed
		try {
			geomNode = tx.getNodeByElementId(geomNodeId);

		} catch (NotFoundException nfe) {

			// propagate exception only if flag is set
			if (throwExceptionIfNotFound) {
				throw nfe;
			}
		}
		if (geomNode != null && isGeometryNodeIndexed(geomNode)) {

			Node indexNode = findLeafContainingGeometryNode(geomNode);

			// be sure geomNode is inside this RTree
			if (isIndexNodeInThisIndex(tx, indexNode)) {

				// remove the entry
				final Relationship geometryRtreeReference = geomNode.getSingleRelationship(
						referenceRelationshipType, Direction.INCOMING);
				if (geometryRtreeReference != null) {
					geometryRtreeReference.delete();
				}
				if (deleteGeomNode) {
					deleteNode(geomNode);
				}

				// reorganize the tree if needed
				if (countChildren(indexNode, referenceRelationshipType) == 0) {
					indexNode = deleteEmptyTreeNodes(indexNode, referenceRelationshipType);
					adjustParentBoundingBox(indexNode, RTreeRelationshipTypes.RTREE_CHILD);
				} else {
					adjustParentBoundingBox(indexNode, referenceRelationshipType);
				}

				adjustPathBoundingBox(indexNode);

				countSaved = false;
				totalGeometryCount--;
			} else if (throwExceptionIfNotFound) {
				throw new RuntimeException("GeometryNode not indexed in this RTree: " + geomNodeId);
			}
		} else if (throwExceptionIfNotFound) {
			throw new RuntimeException("GeometryNode not indexed with an RTree: " + geomNodeId);
		}
	}

	private static Node deleteEmptyTreeNodes(Node indexNode, RelationshipType relType) {
		if (countChildren(indexNode, relType) == 0) {
			Node parent = getIndexNodeParent(indexNode);
			if (parent != null) {
				indexNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING).delete();

				indexNode.delete();
				return deleteEmptyTreeNodes(parent, RTreeRelationshipTypes.RTREE_CHILD);
			}
		}
		// root
		return indexNode;
	}

	private void detachGeometryNodes(Transaction tx, final boolean deleteGeomNodes, Node indexRoot,
			final Listener monitor) {
		monitor.begin(count(tx));
		try {
			// delete all geometry nodes
			visitInTx(tx, new SpatialIndexVisitor() {
				@Override
				public boolean needsToVisit(Envelope indexNodeEnvelope) {
					return true;
				}

				@Override
				public void onIndexReference(Node geomNode) {
					geomNode.getSingleRelationship(referenceRelationshipType, Direction.INCOMING).delete();
					if (deleteGeomNodes) {
						deleteNode(geomNode);
					}

					monitor.worked(1);
				}
			}, indexRoot.getElementId());
		} finally {
			monitor.done();
		}
	}

	@Override
	public void removeAll(Transaction tx, final boolean deleteGeomNodes, final Listener monitor) {
		Node indexRoot = getIndexRoot(tx);

		detachGeometryNodes(tx, deleteGeomNodes, indexRoot, monitor);

		// delete index root relationship
		indexRoot.getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.INCOMING).delete();

		// delete tree
		deleteRecursivelySubtree(indexRoot, null);

		// delete tree metadata
		Relationship metadataNodeRelationship = getRootNode(tx).getSingleRelationship(
				RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING);
		Node metadataNode = metadataNodeRelationship.getEndNode();
		metadataNodeRelationship.delete();
		metadataNode.delete();

		countSaved = false;
		totalGeometryCount = 0;
	}

	@Override
	public void clear(Transaction tx, final Listener monitor) {
		removeAll(tx, false, new NullListener());
		initIndexRoot(tx);
		initIndexMetadata(tx);
	}

	@Override
	public Envelope getBoundingBox(Transaction tx) {
		return getIndexNodeEnvelope(getIndexRoot(tx));
	}

	@Override
	public int count(Transaction tx) {
		saveCount(tx);
		return totalGeometryCount;
	}

	@Override
	public boolean isEmpty(Transaction tx) {
		Node indexRoot = getIndexRoot(tx);
		return !indexRoot.hasProperty(INDEX_PROP_BBOX);
	}

	@Override
	public boolean isNodeIndexed(Transaction tx, String geomNodeId) {
		Node geomNode = tx.getNodeByElementId(geomNodeId);
		// be sure geomNode is inside this RTree
		return geomNode != null && isGeometryNodeIndexed(geomNode) && isIndexNodeInThisIndex(tx,
				findLeafContainingGeometryNode(geomNode));
	}

	public void warmUp(Transaction tx) {
		visit(tx, new WarmUpVisitor(), getIndexRoot(tx));
	}

	public Iterable<Node> getAllIndexInternalNodes(Transaction tx) {
		MonoDirectionalTraversalDescription traversal = new MonoDirectionalTraversalDescription();
		TraversalDescription td = traversal
				.breadthFirst()
				.relationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)
				.evaluator(Evaluators.all());
		return td.traverse(getIndexRoot(tx)).nodes();
	}

	@Override
	public Iterable<Node> getAllIndexedNodes(Transaction tx) {
		return new IndexNodeToGeometryNodeIterable(getAllIndexInternalNodes(tx));
	}

	private class SearchEvaluator extends PathEvaluator.Adapter<SearchFilter.EnvelopFilterResult> {

		private final SearchFilter filter;
		private final Transaction tx;

		public SearchEvaluator(Transaction tx, SearchFilter filter) {
			this.tx = tx;
			this.filter = filter;
		}

		@Override
		public Evaluation evaluate(Path path, BranchState<SearchFilter.EnvelopFilterResult> state) {
			Relationship rel = path.lastRelationship();
			Node node = path.endNode();
			if (rel == null) {
				return Evaluation.EXCLUDE_AND_CONTINUE;
			}
			if (rel.isType(RTreeRelationshipTypes.RTREE_CHILD)) {
				boolean shouldContinue;
				if (state.getState() == SearchFilter.EnvelopFilterResult.INCLUDE_ALL) {
					shouldContinue = true;
				} else {
					SearchFilter.EnvelopFilterResult envelopFilterResult = filter.needsToVisitExtended(
							getIndexNodeEnvelope(node));
					state.setState(envelopFilterResult);
					shouldContinue = envelopFilterResult != SearchFilter.EnvelopFilterResult.EXCLUDE_ALL;
				}
				if (shouldContinue) {
					monitor.matchedTreeNode(path.length(), node);
				}
				monitor.addCase(shouldContinue ? "Index Matches" : "Index Does NOT Match");
				return shouldContinue ?
						Evaluation.EXCLUDE_AND_CONTINUE :
						Evaluation.EXCLUDE_AND_PRUNE;
			}
			if (rel.isType(referenceRelationshipType)) {
				boolean found;
				if (state.getState() == SearchFilter.EnvelopFilterResult.INCLUDE_ALL) {
					found = true;
				} else {
					found = filter.geometryMatches(tx, node);
				}
				monitor.addCase(found ? "Geometry Matches" : "Geometry Does NOT Match");
				if (found) {
					monitor.setHeight(path.length());
				}
				return found ?
						Evaluation.INCLUDE_AND_PRUNE :
						Evaluation.EXCLUDE_AND_PRUNE;
			}
			return null;
		}
	}

	@Override
	public SearchResults searchIndex(Transaction tx, SearchFilter filter) {
		SearchEvaluator searchEvaluator = new SearchEvaluator(tx, filter);
		MonoDirectionalTraversalDescription traversal = new MonoDirectionalTraversalDescription();
		TraversalDescription td = traversal
				.depthFirst()
				.expand(StandardExpander.DEFAULT, path -> SearchFilter.EnvelopFilterResult.FILTER)
				.relationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)
				.relationships(referenceRelationshipType, Direction.OUTGOING)
				.evaluator(searchEvaluator);
		Traverser traverser = td.traverse(getIndexRoot(tx));
		return new SearchResults(traverser.nodes());
	}

	public void visit(Transaction tx, SpatialIndexVisitor visitor, Node indexNode) {
		if (!visitor.needsToVisit(getIndexNodeEnvelope(indexNode))) {
			return;
		}

		// Node is not a leaf
		if (indexNode.hasRelationship(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
			try (var relationships = indexNode.getRelationships(Direction.OUTGOING,
					RTreeRelationshipTypes.RTREE_CHILD)) {
				for (Relationship rel : relationships) {
					Node child = rel.getEndNode();
					// collect children results
					visit(tx, visitor, child);
				}
			}
		} else // Node is a leaf
			if (indexNode.hasRelationship(Direction.OUTGOING, referenceRelationshipType)) {
				try (var relationships = indexNode.getRelationships(Direction.OUTGOING,
						referenceRelationshipType)) {
					for (Relationship rel : relationships) {
						visitor.onIndexReference(rel.getEndNode());
					}
				}
			}
	}

	public Node getIndexRoot(Transaction tx) {
		return getRootNode(tx).getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.OUTGOING)
				.getEndNode();
	}

	// Private methods

	/***
	 * This will get the envelope of the child. The relationshipType acts as flag to allow the function to
	 * know whether the child is a leaf or an index node.
	 */
	private Envelope getChildNodeEnvelope(Node child, RelationshipType relType) {
		if (relType.name().equals(referenceRelationshipType.name())) {
			return getLeafNodeEnvelope(child);
		}
		return getIndexNodeEnvelope(child);
	}

	/**
	 * The leaf nodes belong to the domain model, and as such need to use
	 * the layers domain-specific GeometryEncoder for decoding the envelope.
	 */
	public Envelope getLeafNodeEnvelope(Node geomNode) {
		return envelopeDecoder.decodeEnvelope(geomNode);
	}

	/**
	 * The index nodes do NOT belong to the domain model, and as such need
	 * to use the indexes internal knowledge of the index tree and node
	 * structure for decoding the envelope.
	 */
	public static Envelope getIndexNodeEnvelope(Node indexNode) {
		// this is ok after an index node split
		if (!indexNode.hasProperty(INDEX_PROP_BBOX)) {
			return null;
		}

		double[] bbox = (double[]) indexNode.getProperty(INDEX_PROP_BBOX);
		// Envelope parameters: xmin, xmax, ymin, ymax
		return new Envelope(bbox[0], bbox[2], bbox[1], bbox[3]);
	}

	private void visitInTx(Transaction tx, SpatialIndexVisitor visitor, String indexNodeId) {
		Node indexNode = tx.getNodeByElementId(indexNodeId);
		if (!visitor.needsToVisit(getIndexNodeEnvelope(indexNode))) {
			return;
		}

		if (indexNode.hasRelationship(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
			// Node is not a leaf

			// collect children
			List<String> children = new ArrayList<>();
			try (var relationships = indexNode.getRelationships(Direction.OUTGOING,
					RTreeRelationshipTypes.RTREE_CHILD)) {
				for (Relationship rel : relationships) {
					children.add(rel.getEndNode().getElementId());
				}
			}

			// visit children
			for (String child : children) {
				visitInTx(tx, visitor, child);
			}
		} else // Node is a leaf
			if (indexNode.hasRelationship(Direction.OUTGOING, referenceRelationshipType)) {
				try (var relationships = indexNode.getRelationships(Direction.OUTGOING,
						referenceRelationshipType)) {
					for (Relationship rel : relationships) {
						visitor.onIndexReference(rel.getEndNode());
					}
				}
			}
	}

	private void initIndexMetadata(Transaction tx) {
		Node layerNode = getRootNode(tx);
		if (layerNode.hasRelationship(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_METADATA)) {
			// metadata already present
			Node metadataNode = layerNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_METADATA,
					Direction.OUTGOING).getEndNode();

			maxNodeReferences = (Integer) metadataNode.getProperty("maxNodeReferences");
		} else {
			// metadata initialization
			Node metadataNode = tx.createNode();
			layerNode.createRelationshipTo(metadataNode, RTreeRelationshipTypes.RTREE_METADATA);

			metadataNode.setProperty("maxNodeReferences", maxNodeReferences);
		}

		saveCount(tx);
	}

	private void initIndexRoot(Transaction tx) {
		Node layerNode = getRootNode(tx);
		if (!layerNode.hasRelationship(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_ROOT)) {
			// index initialization
			Node root = tx.createNode();
			layerNode.createRelationshipTo(root, RTreeRelationshipTypes.RTREE_ROOT);
		}
	}

	private Node getMetadataNode(Transaction tx) {
		return getRootNode(tx).getSingleRelationship(RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING)
				.getEndNode();
	}

	/**
	 * Save the geometry count to the database if it has not been saved yet.
	 * However, if the count is zero, first do an exhaustive search of the
	 * tree and count everything before saving it.
	 */
	private void saveCount(Transaction tx) {
		if (totalGeometryCount == 0) {
			SpatialIndexRecordCounter counter = new SpatialIndexRecordCounter();
			visit(tx, counter, getIndexRoot(tx));
			totalGeometryCount = counter.getResult();

			int savedGeometryCount = (int) getMetadataNode(tx).getProperty("totalGeometryCount", 0);
			countSaved = savedGeometryCount == totalGeometryCount;
		}

		if (!countSaved) {
			getMetadataNode(tx).setProperty("totalGeometryCount", totalGeometryCount);
			countSaved = true;
		}
	}

	private static boolean nodeIsLeaf(Node node) {
		return !node.hasRelationship(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD);
	}

	private Node chooseSubTree(Node parentIndexNode, Node geomRootNode) {
		// children that can contain the new geometry
		List<Node> indexNodes = new ArrayList<>();

		// pick the child that contains the new geometry bounding box
		try (var relationships = parentIndexNode.getRelationships(Direction.OUTGOING,
				RTreeRelationshipTypes.RTREE_CHILD)) {
			for (Relationship relation : relationships) {
				Node indexNode = relation.getEndNode();
				if (getIndexNodeEnvelope(indexNode).contains(getLeafNodeEnvelope(geomRootNode))) {
					indexNodes.add(indexNode);
				}
			}
		}

		if (indexNodes.size() > 1) {
			return chooseIndexNodeWithSmallestArea(indexNodes);
		}
		if (indexNodes.size() == 1) {
			return indexNodes.get(0);
		}

		// pick the child that needs the minimum enlargement to include the new geometry
		double minimumEnlargement = Double.POSITIVE_INFINITY;
		try (var relationships = parentIndexNode.getRelationships(Direction.OUTGOING,
				RTreeRelationshipTypes.RTREE_CHILD)) {
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
		}

		if (indexNodes.size() > 1) {
			return chooseIndexNodeWithSmallestArea(indexNodes);
		}
		// this shouldn't happen
		if (indexNodes.size() == 1) {
			return indexNodes.get(0);
		}
		throw new RuntimeException("No IndexNode found for new geometry");
	}

	private double getAreaEnlargement(Node indexNode, Node geomRootNode) {
		Envelope before = getIndexNodeEnvelope(indexNode);

		Envelope after = getLeafNodeEnvelope(geomRootNode);
		after.expandToInclude(before);

		return getArea(after) - getArea(before);
	}

	private static Node chooseIndexNodeWithSmallestArea(List<Node> indexNodes) {
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

	private static int countChildren(Node indexNode, RelationshipType relationshipType) {
		try (var relationships = indexNode.getRelationships(Direction.OUTGOING, relationshipType)) {
			return (int) StreamSupport.stream(relationships.spliterator(), false).count();
		}
	}

	/**
	 * @return is enlargement needed?
	 */
	private boolean insertInLeaf(Node indexNode, Node geomRootNode) {
		return addChild(indexNode, referenceRelationshipType, geomRootNode);
	}

	private void splitAndAdjustPathBoundingBox(Transaction tx, Node indexNode) {
		// create a new node and distribute the entries
		Node newIndexNode =
				splitMode.equals(GREENES_SPLIT) ? greenesSplit(tx, indexNode) : quadraticSplit(tx, indexNode);
		Node parent = getIndexNodeParent(indexNode);
		// if indexNode is the root
		if (parent == null) {
			createNewRoot(tx, indexNode, newIndexNode);
		} else {
			expandParentBoundingBoxAfterNewChild(parent, (double[]) indexNode.getProperty(INDEX_PROP_BBOX));

			addChild(parent, RTreeRelationshipTypes.RTREE_CHILD, newIndexNode);

			if (countChildren(parent, RTreeRelationshipTypes.RTREE_CHILD) > maxNodeReferences) {
				splitAndAdjustPathBoundingBox(tx, parent);
			} else {
				adjustPathBoundingBox(parent);
			}
		}
		monitor.addSplit(newIndexNode);
	}

	private Node quadraticSplit(Transaction tx, Node indexNode) {
		if (nodeIsLeaf(indexNode)) {
			return quadraticSplit(tx, indexNode, referenceRelationshipType);
		}
		return quadraticSplit(tx, indexNode, RTreeRelationshipTypes.RTREE_CHILD);
	}

	private Node greenesSplit(Transaction tx, Node indexNode) {
		if (nodeIsLeaf(indexNode)) {
			return greenesSplit(tx, indexNode, referenceRelationshipType);
		}
		return greenesSplit(tx, indexNode, RTreeRelationshipTypes.RTREE_CHILD);
	}

	private static NodeWithEnvelope[] mostDistantByDeadSpace(List<NodeWithEnvelope> entries) {
		NodeWithEnvelope seed1 = entries.get(0);
		NodeWithEnvelope seed2 = entries.get(0);
		double worst = Double.NEGATIVE_INFINITY;
		for (int i = 0; i < entries.size(); ++i) {
			NodeWithEnvelope e = entries.get(i);
			for (int j = i + 1; j < entries.size(); ++j) {
				NodeWithEnvelope e1 = entries.get(j);
				double deadSpace = e.envelope.separation(e1.envelope);
				if (deadSpace > worst) {
					worst = deadSpace;
					seed1 = e;
					seed2 = e1;
				}
			}
		}
		return new NodeWithEnvelope[]{seed1, seed2};
	}

	private static int findLongestDimension(List<NodeWithEnvelope> entries) {
		if (!entries.isEmpty()) {
			Envelope env = new Envelope(entries.get(0).envelope);
			for (NodeWithEnvelope entry : entries) {
				env.expandToInclude(entry.envelope);
			}
			int longestDimension = 0;
			double maxWidth = Double.NEGATIVE_INFINITY;
			for (int i = 0; i < env.getDimension(); i++) {
				double width = env.getWidth(i);
				if (width > maxWidth) {
					maxWidth = width;
					longestDimension = i;
				}
			}
			return longestDimension;
		}
		return 0;
	}

	private List<NodeWithEnvelope> extractChildNodesWithEnvelopes(Node indexNode, RelationshipType relationshipType) {
		List<NodeWithEnvelope> entries = new ArrayList<>();

		try (var relationships = indexNode.getRelationships(Direction.OUTGOING, relationshipType)) {
			for (Relationship relationship : relationships) {
				Node node = relationship.getEndNode();
				entries.add(new NodeWithEnvelope(node, getChildNodeEnvelope(node, relationshipType)));
				relationship.delete();
			}
		}
		return entries;
	}

	private Node greenesSplit(Transaction tx, Node indexNode, RelationshipType relationshipType) {
		// Disconnect all current children from the index and return them with their envelopes
		List<NodeWithEnvelope> entries = extractChildNodesWithEnvelopes(indexNode, relationshipType);

		// We want to split by the longest dimension to avoid degrading into extremely thin envelopes
		int longestDimension = findLongestDimension(entries);

		// Sort the entries by the longest dimension and then create envelopes around left and right halves
		entries.sort(new SingleDimensionNodeEnvelopeComparator(longestDimension));
		int splitAt = entries.size() / 2;
		List<NodeWithEnvelope> left = entries.subList(0, splitAt);
		List<NodeWithEnvelope> right = entries.subList(splitAt, entries.size());

		return reconnectTwoChildGroups(tx, indexNode, left, right, relationshipType);
	}

	private record SingleDimensionNodeEnvelopeComparator(int dimension) implements Comparator<NodeWithEnvelope> {

		@Override
		public int compare(NodeWithEnvelope o1, NodeWithEnvelope o2) {
			double length = o2.envelope.centre(dimension) - o1.envelope.centre(dimension);
			return Double.compare(length, 0.0);
		}
	}

	private Node quadraticSplit(Transaction tx, Node indexNode, RelationshipType relationshipType) {
		// Disconnect all current children from the index and return them with their envelopes
		List<NodeWithEnvelope> entries = extractChildNodesWithEnvelopes(indexNode, relationshipType);

		// pick two seed entries such that the dead space is maximal
		NodeWithEnvelope[] seeds = mostDistantByDeadSpace(entries);

		List<NodeWithEnvelope> group1 = new ArrayList<>();
		group1.add(seeds[0]);
		Envelope group1envelope = seeds[0].envelope;

		List<NodeWithEnvelope> group2 = new ArrayList<>();
		group2.add(seeds[1]);
		Envelope group2envelope = seeds[1].envelope;

		entries.remove(seeds[0]);
		entries.remove(seeds[1]);
		while (!entries.isEmpty()) {
			// compute the cost of inserting each entry
			List<NodeWithEnvelope> bestGroup = null;
			Envelope bestGroupEnvelope = null;
			NodeWithEnvelope bestEntry = null;
			double expansionMin = Double.POSITIVE_INFINITY;
			for (NodeWithEnvelope e : entries) {
				double expansion1 = getArea(createEnvelope(e.envelope, group1envelope)) - getArea(group1envelope);
				double expansion2 = getArea(createEnvelope(e.envelope, group2envelope)) - getArea(group2envelope);

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

			if (bestEntry == null) {
				throw new RuntimeException(
						"Should not be possible to fail to find a best entry during quadratic split");
			}
			// insert the best candidate entry in the best group
			bestGroup.add(bestEntry);
			bestGroupEnvelope.expandToInclude(bestEntry.envelope);

			entries.remove(bestEntry);
		}

		return reconnectTwoChildGroups(tx, indexNode, group1, group2, relationshipType);
	}

	private Node reconnectTwoChildGroups(Transaction tx, Node indexNode, List<NodeWithEnvelope> group1,
			List<NodeWithEnvelope> group2, RelationshipType relationshipType) {
		// reset bounding box and add new children
		indexNode.removeProperty(INDEX_PROP_BBOX);
		for (NodeWithEnvelope entry : group1) {
			addChild(indexNode, relationshipType, entry.node);
		}

		// create new node from split
		Node newIndexNode = tx.createNode();
		for (NodeWithEnvelope entry : group2) {
			addChild(newIndexNode, relationshipType, entry.node);
		}

		return newIndexNode;
	}

	private void createNewRoot(Transaction tx, Node oldRoot, Node newIndexNode) {
		Node newRoot = tx.createNode();
		addChild(newRoot, RTreeRelationshipTypes.RTREE_CHILD, oldRoot);
		addChild(newRoot, RTreeRelationshipTypes.RTREE_CHILD, newIndexNode);

		Node layerNode = getRootNode(tx);
		layerNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.OUTGOING).delete();
		layerNode.createRelationshipTo(newRoot, RTreeRelationshipTypes.RTREE_ROOT);
	}

	private boolean addChild(Node parent, RelationshipType type, Node newChild) {
		Envelope childEnvelope = getChildNodeEnvelope(newChild, type);
		double[] childBBox = new double[]{
				childEnvelope.getMinX(), childEnvelope.getMinY(),
				childEnvelope.getMaxX(), childEnvelope.getMaxY()};
		parent.createRelationshipTo(newChild, type);
		return expandParentBoundingBoxAfterNewChild(parent, childBBox);
	}

	private void adjustPathBoundingBox(Node node) {
		Node parent = getIndexNodeParent(node);
		// entry has been modified: adjust the path for the parent
		if (parent != null) {
			if (adjustParentBoundingBox(parent, RTreeRelationshipTypes.RTREE_CHILD)) {
				adjustPathBoundingBox(parent);
			}
		}
	}

	/**
	 * Fix an IndexNode bounding box after a child has been added or removed. Return true if something was
	 * changed so that parents can also be adjusted.
	 */
	private boolean adjustParentBoundingBox(Node indexNode, RelationshipType relationshipType) {
		double[] old = null;
		if (indexNode.hasProperty(INDEX_PROP_BBOX)) {
			old = (double[]) indexNode.getProperty(INDEX_PROP_BBOX);
		}

		Envelope bbox = null;

		try (var relationships = indexNode.getRelationships(Direction.OUTGOING, relationshipType)) {
			for (Relationship relationship : relationships) {
				Node childNode = relationship.getEndNode();

				if (bbox == null) {
					bbox = new Envelope(getChildNodeEnvelope(childNode, relationshipType));
				} else {
					bbox.expandToInclude(getChildNodeEnvelope(childNode, relationshipType));
				}
			}
		}

		// this could happen in an empty tree
		if (bbox == null) {
			bbox = new Envelope(0, 0, 0, 0);
		}

		if (old == null || old.length != 4
				|| bbox.getMinX() != old[0]
				|| bbox.getMinY() != old[1]
				|| bbox.getMaxX() != old[2]
				|| bbox.getMaxY() != old[3]) {
			setIndexNodeEnvelope(indexNode, bbox);
			return true;
		}
		return false;
	}

	protected static void setIndexNodeEnvelope(Node indexNode, Envelope bbox) {
		indexNode.setProperty(INDEX_PROP_BBOX,
				new double[]{bbox.getMinX(), bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY()});
	}

	/**
	 * Adjust IndexNode bounding box according to the new child inserted
	 *
	 * @param parent    IndexNode
	 * @param childBBox geomNode inserted
	 * @return is bbox changed?
	 */
	protected static boolean expandParentBoundingBoxAfterNewChild(Node parent, double[] childBBox) {
		if (!parent.hasProperty(INDEX_PROP_BBOX)) {
			parent.setProperty(INDEX_PROP_BBOX, new double[]{childBBox[0], childBBox[1], childBBox[2], childBBox[3]});
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

	private static boolean setMin(double[] parent, double[] child, int index) {
		if (parent[index] > child[index]) {
			parent[index] = child[index];
			return true;
		}
		return false;
	}

	private static boolean setMax(double[] parent, double[] child, int index) {
		if (parent[index] < child[index]) {
			parent[index] = child[index];
			return true;
		}
		return false;
	}

	private static Node getIndexNodeParent(Node indexNode) {
		Relationship relationship = indexNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD,
				Direction.INCOMING);
		if (relationship == null) {
			return null;
		}
		return relationship.getStartNode();
	}

	private static double getArea(Envelope e) {
		return e.getArea();
	}

	private static void deleteTreeBelow(Node rootNode) {
		try (var relationships = rootNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
			for (Relationship relationship : relationships) {
				deleteRecursivelySubtree(relationship.getEndNode(), relationship);
			}
		}
	}

	private static void deleteRecursivelySubtree(Node node, Relationship incoming) {
		try (var relationships = node.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
			for (Relationship relationship : relationships) {
				deleteRecursivelySubtree(relationship.getEndNode(), relationship);
			}
		}
		if (incoming != null) {
			incoming.delete();
		}
		try (var relationships = node.getRelationships()) {
			for (Relationship rel : relationships) {
				System.out.println("Unexpected relationship found on " + node + ": " + rel.toString());
				rel.delete();
			}
		}
		node.delete();
	}

	protected boolean isGeometryNodeIndexed(Node geomNode) {
		return geomNode.hasRelationship(Direction.INCOMING, referenceRelationshipType);
	}

	protected Node findLeafContainingGeometryNode(Node geomNode) {
		return geomNode.getSingleRelationship(referenceRelationshipType, Direction.INCOMING)
				.getStartNode();
	}

	protected boolean isIndexNodeInThisIndex(Transaction tx, Node indexNode) {
		Node child = indexNode;
		Node root = null;
		while (root == null) {
			Node parent = getIndexNodeParent(child);
			if (parent == null) {
				root = child;
			} else {
				child = parent;
			}
		}
		return root.getElementId().equals(getIndexRoot(tx).getElementId());
	}

	private static void deleteNode(Node node) {
		try (var relationships = node.getRelationships()) {
			for (Relationship r : relationships) {
				r.delete();
			}
		}
		node.delete();
	}

	private Node getRootNode(Transaction tx) {
		return tx.getNodeByElementId(rootNodeId);
	}

	/**
	 * Create a bounding box encompassing the two bounding boxes passed in.
	 */
	private static Envelope createEnvelope(Envelope e, Envelope e1) {
		Envelope result = new Envelope(e);
		result.expandToInclude(e1);
		return result;
	}

	// Private classes
	private static class WarmUpVisitor implements SpatialIndexVisitor {

		@Override
		public boolean needsToVisit(Envelope indexNodeEnvelope) {
			return true;
		}

		@Override
		public void onIndexReference(Node geomNode) {
		}
	}

	/**
	 * In order to wrap one iterable or iterator in another that converts
	 * the objects from one type to another without loading all into memory,
	 * we need to use this ugly java-magic. Man, I miss Ruby right now!
	 */
	private class IndexNodeToGeometryNodeIterable implements Iterable<Node> {

		private final Iterator<Node> allIndexNodeIterator;

		private class GeometryNodeIterator implements Iterator<Node> {

			Iterator<Node> geometryNodeIterator = null;

			@Override
			public boolean hasNext() {
				checkGeometryNodeIterator();
				return geometryNodeIterator != null && geometryNodeIterator.hasNext();
			}

			@Override
			public Node next() {
				checkGeometryNodeIterator();
				return geometryNodeIterator == null ? null : geometryNodeIterator.next();
			}

			private void checkGeometryNodeIterator() {
				MonoDirectionalTraversalDescription traversal = new MonoDirectionalTraversalDescription();
				TraversalDescription td = traversal
						.depthFirst()
						.relationships(referenceRelationshipType, Direction.OUTGOING)
						.evaluator(Evaluators.excludeStartPosition())
						.evaluator(Evaluators.toDepth(1));
				while ((geometryNodeIterator == null || !geometryNodeIterator.hasNext()) &&
						allIndexNodeIterator.hasNext()) {
					geometryNodeIterator = td.traverse(allIndexNodeIterator.next()).nodes().iterator();
				}
			}

			@Override
			public void remove() {
			}
		}

		public IndexNodeToGeometryNodeIterable(Iterable<Node> allIndexNodes) {
			this.allIndexNodeIterator = allIndexNodes.iterator();
		}

		@Override
		@Nonnull
		public Iterator<Node> iterator() {
			return new GeometryNodeIterator();
		}
	}

	private static class IndexNodeAreaComparator implements Comparator<NodeWithEnvelope> {

		@Override
		public int compare(NodeWithEnvelope o1, NodeWithEnvelope o2) {
			return Double.compare(o1.envelope.getArea(), o2.envelope.getArea());
		}
	}

	@Override
	public void finalizeTransaction(Transaction tx) {
		saveCount(tx);
	}
}
