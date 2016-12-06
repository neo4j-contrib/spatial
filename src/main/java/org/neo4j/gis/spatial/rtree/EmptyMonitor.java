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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EmptyMonitor implements TreeMonitor
{
    @Override
    public void setHeight(int height)
    {
    }

    public int getHeight()
    {
        return -1;
    }

    @Override
    public void addNbrRebuilt(RTreeIndex rtree)
    {
    }

    @Override
    public int getNbrRebuilt()
    {
        return -1;
    }

    @Override
    public void addSplit(Node indexNode)
    {

    }

    @Override
    public void beforeMergeTree(Node indexNode, List<RTreeIndex.NodeWithEnvelope> right) {

    }

    @Override
    public void afterMergeTree(Node indexNode) {

    }

    @Override
    public int getNbrSplit()
    {
        return -1;
    }

    @Override
    public void addCase(String key) {

    }

    @Override
    public Map<String, Integer> getCaseCounts() {
        return null;
    }

    @Override
    public void reset()
    {

    }

    @Override
    public void matchedTreeNode(int level, Node node) {

    }

    @Override
    public List<Node> getMatchedTreeNodes(int level) {
        return new ArrayList();
    }
}
