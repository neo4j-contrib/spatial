/*
 * Copyright (c) 2010
 *
 * This program is free software: you can redistribute it and/or modify
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

import static org.neo4j.gis.spatial.GeometryUtils.getEnvelope;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.gis.spatial.query.SearchAll;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import com.vividsolutions.jts.geom.Envelope;


/**
 * @author Davide Savazzi
 */
public class RTreeIndex implements SpatialIndexReader, SpatialIndexWriter, Constants {

	// Constructor
	
	public RTreeIndex(GraphDatabaseService database, Layer layer) {
		this(database, layer, 100, 40);
	}
	
	public RTreeIndex(GraphDatabaseService database, Layer layer, int maxNodeReferences, int minNodeReferences) {
		this.database = database;
		this.layer = layer;
		this.maxNodeReferences = maxNodeReferences;
		this.minNodeReferences = minNodeReferences;
		
		initIndexMetadata();
		initIndexRoot();
	}
	
	
	// Public methods
	
	@Override
	public void add(Node geomNode) {
		// initialize the search with root
		Node parent = getIndexRoot();
		
		// choose a path down to a leaf
		while (!nodeIsLeaf(parent)) {
			parent = chooseSubTree(parent, geomNode);
		}
		
		if (countChildren(parent, SpatialRelationshipTypes.RTREE_REFERENCE) == maxNodeReferences) {
			insertInLeaf(parent, geomNode);
			splitAndAdjustPathBoundingBox(parent);
		} else {
			if (insertInLeaf(parent, geomNode)) {
				// bbox enlargement needed
				adjustPathBoundingBox(parent);							
			}
		}
	}
	
	@Override
	public void delete(long geomNodeId, boolean removeGeomNode) {
		Node geomNode = database.getNodeById(geomNodeId);
		
		// be sure geomNode is inside this RTree
		Node indexNode = findLeafContainingGeometryNode(geomNode);
		
		// remove the entry 
		geomNode.getSingleRelationship(SpatialRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING).delete();
		if (removeGeomNode) geomNode.delete();
		
		// reorganize the tree if needed
		if (getIndexNodeParent(indexNode) != null && countChildren(indexNode, SpatialRelationshipTypes.RTREE_REFERENCE) < minNodeReferences) {
			// indexNode is not the root and contain less than the minimum number of entries
			// tree needs reorganization
			
			// find the parent that must be deleted (its children < minNodeReferences) nearest to the root
			Node lastParentNodeToDelete = findIndexNodeToDeleteNearestToRoot(indexNode);
			
			// find all geomNodes in the subtree
			SearchAll search = new SearchAll();
			search.setGeometryFactory(layer.getGeometryFactory());
			visit(search, lastParentNodeToDelete);
			List<SpatialDatabaseRecord> orphanedGeometryNodes = search.getResults();

			// remove all geomNode in the subtree
			for (SpatialDatabaseRecord orphan : orphanedGeometryNodes) {
				orphan.getGeomNode().getSingleRelationship(SpatialRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING).delete();
			}
			
			deleteRecursivelyEmptySubtree(lastParentNodeToDelete);

			// adjust tree
			adjustParentBoundingBox(getIndexNodeParent(lastParentNodeToDelete), SpatialRelationshipTypes.RTREE_CHILD);
			adjustPathBoundingBox(getIndexNodeParent(lastParentNodeToDelete));
			
			// add orphaned geomNodes
			for (SpatialDatabaseRecord orphan : orphanedGeometryNodes) {
				add(orphan.getGeomNode());
			}			
		} else {
			// indexNode is root or contains more than the minimum number of geomNode references
			adjustParentBoundingBox(indexNode, SpatialRelationshipTypes.RTREE_REFERENCE);
			adjustPathBoundingBox(indexNode);
		}
	}
	
	@Override
	public Envelope getLayerBoundingBox() {
		Node root = getIndexRoot();
		return getEnvelope(root);
	}
	
	@Override
	public int count() {
		RecordCounter counter = new RecordCounter();
		visit(counter, getIndexRoot());
		return counter.getResult();
	}

	@Override
	public void executeSearch(Search search) {
		search.setGeometryFactory(layer.getGeometryFactory());
		visit(search, getIndexRoot());
	}
	
	public void warmUp() {
		visit(new WarmUpVisitor(), getIndexRoot());
	}
	
	
	// Private methods

	private void visit(SpatialIndexVisitor visitor, Node indexNode) {
		if (!visitor.needsToVisit(indexNode)) return;
		
		if (indexNode.hasRelationship(SpatialRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
			// Node is not a leaf
			for (Relationship rel : indexNode.getRelationships(SpatialRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
				Node child = rel.getEndNode();
				// collect children results
				visit(visitor, child);
			}
		} else if (indexNode.hasRelationship(SpatialRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING)) {
			// Node is a leaf
			for (Relationship rel : indexNode.getRelationships(SpatialRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING)) {
				visitor.onIndexReference(rel.getEndNode());
			}
		}
	}
	
	private void initIndexMetadata() {
		Node layerNode = database.getNodeById(layer.getLayerNodeId());
		if (layerNode.hasRelationship(SpatialRelationshipTypes.RTREE_METADATA, Direction.OUTGOING)) {
			// metadata already present
			Node metadataNode = layerNode.getSingleRelationship(SpatialRelationshipTypes.RTREE_METADATA, Direction.OUTGOING).getEndNode();
			
			maxNodeReferences = (Integer) metadataNode.getProperty("maxNodeReferences");
			minNodeReferences = (Integer) metadataNode.getProperty("minNodeReferences");
		} else {
			// metadata initialization
			Node metadataNode = database.createNode();
			layerNode.createRelationshipTo(metadataNode, SpatialRelationshipTypes.RTREE_METADATA);
			
			metadataNode.setProperty("maxNodeReferences", maxNodeReferences);
			metadataNode.setProperty("minNodeReferences", minNodeReferences);
		}
	}

	private void initIndexRoot() {
		Node layerNode = database.getNodeById(layer.getLayerNodeId());
		if (layerNode.hasRelationship(SpatialRelationshipTypes.RTREE_ROOT, Direction.OUTGOING)) {
			// index already present
			indexRootNodeId = layerNode.getSingleRelationship(SpatialRelationshipTypes.RTREE_ROOT, Direction.OUTGOING).getEndNode().getId();
		} else {
			// index initialization
			Node root = database.createNode();
			layerNode.createRelationshipTo(root, SpatialRelationshipTypes.RTREE_ROOT);
			indexRootNodeId = root.getId();
		}
	}
	
	private Node getIndexRoot() {
		return database.getNodeById(indexRootNodeId);
	}
	
	private boolean nodeIsLeaf(Node node) {
		return !node.hasRelationship(SpatialRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);
	}
	
	private Node chooseSubTree(Node parentIndexNode, Node geomRootNode) {
		// children that can contain the new geometry
		List<Node> indexNodes = new ArrayList<Node>();
		
		// pick the child that contains the new geometry bounding box		
		Iterable<Relationship> relationships = parentIndexNode.getRelationships(SpatialRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);		
		for (Relationship relation : relationships) {
			Node indexNode = relation.getEndNode();
			if (getEnvelope(indexNode).contains(getEnvelope(geomRootNode))) {
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
		relationships = parentIndexNode.getRelationships(SpatialRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);
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
			throw new SpatialDatabaseException("No IndexNode found for new geometry");
		}
	}

    private double getAreaEnlargement(Node indexNode, Node geomRootNode) {
    	Envelope before = getEnvelope(indexNode);
    	
    	Envelope after = getEnvelope(geomRootNode);
    	after.expandToInclude(before);
    	
    	return getArea(after) - getArea(before);
    }
	
	private Node chooseIndexNodeWithSmallestArea(List<Node> indexNodes) {
		Node result = null;
		double smallestArea = -1;

		for (Node indexNode : indexNodes) {
			double area = getArea(indexNode);
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
		return addChild(indexNode, SpatialRelationshipTypes.RTREE_REFERENCE, geomRootNode);
	}

	private void splitAndAdjustPathBoundingBox(Node indexNode) {
		// create a new node and distribute the entries
		Node newIndexNode = quadraticSplit(indexNode);
		Node parent = getIndexNodeParent(indexNode);
		if (parent == null) {
			// if indexNode is the root
			createNewRoot(indexNode, newIndexNode);
		} else {
			adjustParentBoundingBox(parent, indexNode);
			
			addChild(parent, SpatialRelationshipTypes.RTREE_CHILD, newIndexNode);

			if (countChildren(parent, SpatialRelationshipTypes.RTREE_CHILD) > maxNodeReferences) {
				splitAndAdjustPathBoundingBox(parent);
			} else {
				adjustPathBoundingBox(parent);
			}
		}
	}

	private Node quadraticSplit(Node indexNode) {
		if (nodeIsLeaf(indexNode)) return quadraticSplit(indexNode, SpatialRelationshipTypes.RTREE_REFERENCE);
		else return quadraticSplit(indexNode, SpatialRelationshipTypes.RTREE_CHILD);
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
			Envelope eEnvelope = getEnvelope(e);
			for (Node e1 : entries) {
				Envelope e1Envelope = getEnvelope(e1);
				double deadSpace = getArea(getEnvelope(eEnvelope, e1Envelope)) - getArea(eEnvelope) - getArea(e1Envelope);
				if (deadSpace > worst) {
					worst = deadSpace;
					seed1 = e;
					seed2 = e1;
				}
			}
		}
		
		List<Node> group1 = new ArrayList<Node>();
		group1.add(seed1);
		Envelope group1envelope = getEnvelope(seed1);
		
		List<Node> group2 = new ArrayList<Node>();
		group2.add(seed2);
		Envelope group2envelope = getEnvelope(seed2);
		
		entries.remove(seed1);
		entries.remove(seed2);
		while (entries.size() > 0) {
			// compute the cost of inserting each entry
			List<Node> bestGroup = null;
			Envelope bestGroupEnvelope = null;
			Node bestEntry = null;
			double expansionMin = Double.POSITIVE_INFINITY;
			for (Node e : entries) {
				Envelope nodeEnvelope = getEnvelope(e);
				double expansion1 = getArea(getEnvelope(nodeEnvelope, group1envelope)) - getArea(group1envelope);
				double expansion2 = getArea(getEnvelope(nodeEnvelope, group2envelope)) - getArea(group2envelope);
						
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
			bestGroupEnvelope.expandToInclude(getEnvelope(bestEntry));

			entries.remove(bestEntry);
			
			// each group must contain at least minNodeReferences entries.
			// if the group size added to the number of remaining entries is equal to minNodeReferences
			// just add them to the group
			
			if (group1.size() + entries.size() == minNodeReferences) {
				group1.addAll(entries);
				entries.clear();
			}
			
			if (group2.size() + entries.size() == minNodeReferences) {
				group2.addAll(entries);
				entries.clear();
			}
		}
		
		for (Node node : group1) {
			addChild(indexNode, relationshipType, node);
		}

		Node newIndexNode = database.createNode();
		for (Node node: group2) {
			addChild(newIndexNode, relationshipType, node);
		}
		
		return newIndexNode;
	}

	private void createNewRoot(Node oldRoot, Node newIndexNode) {
		Node newRoot = database.createNode();
		addChild(newRoot, SpatialRelationshipTypes.RTREE_CHILD, oldRoot);
		addChild(newRoot, SpatialRelationshipTypes.RTREE_CHILD, newIndexNode);
		
		Node layerNode = database.getNodeById(layer.getLayerNodeId());
		layerNode.getSingleRelationship(SpatialRelationshipTypes.RTREE_ROOT, Direction.OUTGOING).delete();
		layerNode.createRelationshipTo(newRoot, SpatialRelationshipTypes.RTREE_ROOT);
	}
	
	private boolean addChild(Node parent, RelationshipType type, Node newChild) {
		parent.createRelationshipTo(newChild, type);
		return adjustParentBoundingBox(parent, newChild);
	}
	
	private void adjustPathBoundingBox(Node indexNode) {
		Node parent = getIndexNodeParent(indexNode);
		if (parent != null) {
			if (adjustParentBoundingBox(parent, indexNode)) {
				// entry has been modified: adjust the path for the parent
				adjustPathBoundingBox(parent);
			}
		}
	}

	/**
	 * Fix an IndexNode bounding box after a child has been removed
	 * @param indexNode
	 */
	private void adjustParentBoundingBox(Node indexNode, RelationshipType relationshipType) {
		Envelope bbox = null;
		
		Iterator<Relationship> iterator = indexNode.getRelationships(relationshipType, Direction.OUTGOING).iterator();
		while (iterator.hasNext()) {
			Node childNode = iterator.next().getEndNode();
			if (bbox == null) bbox = getEnvelope(childNode);
			else bbox.expandToInclude(getEnvelope(childNode));
		}

		indexNode.setProperty(PROP_BBOX, new double[] { bbox.getMinX(), bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY() });
	}
		
	/**
	 * Adjust IndexNode bounding box according to the new child inserted
	 * @param parent IndexNode
	 * @param child geomNode inserted
	 * @return is bbox changed?
	 */
	private boolean adjustParentBoundingBox(Node parent, Node child) {
		if (!parent.hasProperty(PROP_BBOX)) {
			double[] childBBox = (double[]) child.getProperty(PROP_BBOX);
			parent.setProperty(PROP_BBOX, new double[] { childBBox[0], childBBox[1], childBBox[2], childBBox[3] });
			return true;
		}
		
		double[] parentBBox = (double[]) parent.getProperty(PROP_BBOX);
		double[] childBBox = (double[]) child.getProperty(PROP_BBOX);
		
		boolean valueChanged = setMin(parentBBox, childBBox, 0);
		valueChanged = setMin(parentBBox, childBBox, 1) || valueChanged;
		valueChanged = setMax(parentBBox, childBBox, 2) || valueChanged;
		valueChanged = setMax(parentBBox, childBBox, 3) || valueChanged;
		
		if (valueChanged) parent.setProperty(PROP_BBOX, parentBBox);
		
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
		Relationship relationship = indexNode.getSingleRelationship(SpatialRelationshipTypes.RTREE_CHILD, Direction.INCOMING);
		if (relationship == null) return null;
		else return relationship.getStartNode();
	}	
	
	private double getArea(Node node) {
		return getArea(getEnvelope(node));
	}

	private double getArea(Envelope e) {
		return e.getWidth() * e.getHeight();
	}

	private Node findIndexNodeToDeleteNearestToRoot(Node indexNode) {
		Node indexNodeParent = getIndexNodeParent(indexNode);
		
		if (getIndexNodeParent(indexNodeParent) != null && countChildren(indexNodeParent, SpatialRelationshipTypes.RTREE_CHILD) == minNodeReferences) {
			// indexNodeParent is not the root and will contain less than the minimum number of entries
			return findIndexNodeToDeleteNearestToRoot(indexNodeParent);
		} else {
			return indexNode;
		}
	}
	
	private void deleteRecursivelyEmptySubtree(Node indexNode) {
		for (Relationship relationship : indexNode.getRelationships(SpatialRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
			deleteRecursivelyEmptySubtree(relationship.getEndNode());
		}
		
		indexNode.getSingleRelationship(SpatialRelationshipTypes.RTREE_CHILD, Direction.INCOMING).delete();
		indexNode.delete();
	}
	
	private Node findLeafContainingGeometryNode(Node geomNode) {
		if (!geomNode.hasRelationship(SpatialRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING)) {
			throw new SpatialDatabaseException("GeometryNode not indexed with an RTree: " + geomNode.getId());
		}

		Node indexNodeLeaf = geomNode.getSingleRelationship(SpatialRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING).getStartNode();		
		
		// check if geometryNode is indexed in this rtre
		Node root = null;
		Node child = indexNodeLeaf;
		while (root == null) {
			Node parent = getIndexNodeParent(child);
			if (parent == null) root = child;
			else child = parent;
		}
		
		if (root.getId() != indexRootNodeId) {
			throw new SpatialDatabaseException("GeometryNode not indexed in this RTree: " + geomNode.getId());
		} else {
			return indexNodeLeaf;
		}
	}
		
	
	// Attributes
	
	private GraphDatabaseService database;
	private Layer layer;
	private long indexRootNodeId;
	private int maxNodeReferences;
	private int minNodeReferences;
}

class RecordCounter implements SpatialIndexVisitor {
	
	public boolean needsToVisit(Node indexNode) { return true; }	
	
	public void onIndexReference(Node geomNode) { count++; }
	
	public int getResult() { return count; }
	
	private int count = 0;
}

class WarmUpVisitor implements SpatialIndexVisitor {
	
	public boolean needsToVisit(Node indexNode) { getEnvelope(indexNode); return true; }	
	
	public void onIndexReference(Node geomNode) { }	
}