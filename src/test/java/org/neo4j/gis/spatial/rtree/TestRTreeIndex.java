/**
 * Copyright (c) 2002-2013 "Neo Technology," Network Engine for Objects in Lund
 * AB [http://neotechnology.com]
 *
 * This file is part of Neo4j Spatial.
 *
 * Neo4j is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.rtree;

import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public class TestRTreeIndex extends RTreeIndex {

    public TestRTreeIndex(GraphDatabaseService database) {
        init(database, database.createNode(), new SimplePointEncoder());
    }

    public RTreeIndex.NodeWithEnvelope makeChildIndexNode(NodeWithEnvelope parent, Envelope bbox) {
        Node indexNode = getDatabase().createNode();
        setIndexNodeEnvelope(indexNode, bbox);
        parent.node.createRelationshipTo(indexNode, RTreeRelationshipTypes.RTREE_CHILD);
        expandParentBoundingBoxAfterNewChild(parent.node, new double[]{bbox.getMinX(), bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY()});
        return new NodeWithEnvelope(indexNode, bbox);
    }

    public void setIndexNodeEnvelope(NodeWithEnvelope indexNode) {
        setIndexNodeEnvelope(indexNode.node, indexNode.envelope);
    }

    public void mergeTwoTrees(NodeWithEnvelope left, NodeWithEnvelope right) {
        super.mergeTwoSubtrees(left, this.getIndexChildren(right.node));
    }
}
