/**
 * Copyright (c) 2002-2013 "Neo Technology," Network Engine for Objects in Lund
 * AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
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


import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Map;

public interface TreeMonitor
{
    void setHeight(int height);

    int getHeight();

    void addNbrRebuilt(RTreeIndex rtree);

    int getNbrRebuilt();

    void addSplit(Node indexNode);

    void beforeMergeTree(Node indexNode, List<RTreeIndex.NodeWithEnvelope> right);

    void afterMergeTree(Node indexNode);

    int getNbrSplit();

    void addCase(String key);

    Map<String, Integer> getCaseCounts();

    void reset();

    void matchedTreeNode(int level, Node node);

    List<Node> getMatchedTreeNodes(int level);
}
