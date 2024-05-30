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

import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class TestRTreeIndex extends RTreeIndex {

	// TODO: Rather pass tx into init after construction (bad pattern to pass tx to constructor, as if it will be saved)
	public TestRTreeIndex(Transaction tx) {
		init(tx, tx.createNode(), new SimplePointEncoder(), DEFAULT_MAX_NODE_REFERENCES);
	}

	public static RTreeIndex.NodeWithEnvelope makeChildIndexNode(Transaction tx, NodeWithEnvelope parent,
			Envelope bbox) {
		Node indexNode = tx.createNode();
		setIndexNodeEnvelope(indexNode, bbox);
		parent.node.createRelationshipTo(indexNode, RTreeRelationshipTypes.RTREE_CHILD);
		expandParentBoundingBoxAfterNewChild(parent.node,
				new double[]{bbox.getMinX(), bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY()});
		return new NodeWithEnvelope(indexNode, bbox);
	}

	public static void setIndexNodeEnvelope(NodeWithEnvelope indexNode) {
		setIndexNodeEnvelope(indexNode.node, indexNode.envelope);
	}

	public void mergeTwoTrees(Transaction tx, NodeWithEnvelope left, NodeWithEnvelope right) {
		super.mergeTwoSubtrees(tx, left, getIndexChildren(right.node));
	}
}
