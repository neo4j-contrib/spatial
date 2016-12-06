/**
 * Copyright (c) 2002-2013 "Neo Technology," Network Engine for Objects in Lund
 * AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j.
 * <p>
 * Neo4j is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 * <p>
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.gis.spatial.rtree;

import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RTreeMonitor implements TreeMonitor {
    private int nbrSplit;
    private int height;
    private int nbrRebuilt;
    private HashMap<String, Integer> cases = new HashMap<>();
    private ArrayList<ArrayList<Node>> matchedTreeNodes = new ArrayList<>();

    public RTreeMonitor() {
        reset();
    }

    @Override
    public void setHeight(int height) {
        this.height = height;
    }

    public int getHeight() {
        return height;
    }

    @Override
    public void addNbrRebuilt(RTreeIndex rtree) {
        nbrRebuilt++;
    }

    @Override
    public int getNbrRebuilt() {
        return nbrRebuilt;
    }

    @Override
    public void addSplit(Node indexNode) {
        nbrSplit++;
    }

    @Override
    public void beforeMergeTree(Node indexNode, List<RTreeIndex.NodeWithEnvelope> right) {

    }

    @Override
    public void afterMergeTree(Node indexNode) {

    }

    @Override
    public int getNbrSplit() {
        return nbrSplit;
    }

    @Override
    public void addCase(String key) {
        Integer n = cases.get(key);
        if (n != null) {
            n++;
        } else {
            n = 1;
        }
        cases.put(key, n);
    }

    @Override
    public Map<String, Integer> getCaseCounts() {
        return cases;
    }

    @Override
    public void reset() {
        cases.clear();
        height = 0;
        nbrRebuilt = 0;
        nbrSplit = 0;
        matchedTreeNodes.clear();
    }

    @Override
    public void matchedTreeNode(int level, Node node) {
        ensureMatchedTreeNodeLevel(level);
        matchedTreeNodes.get(level).add(node);
    }

    private void ensureMatchedTreeNodeLevel(int level) {
        while (matchedTreeNodes.size() <= level) {
            matchedTreeNodes.add(new ArrayList<Node>());
        }
    }

    @Override
    public List<Node> getMatchedTreeNodes(int level) {
        ensureMatchedTreeNodeLevel(level);
        return matchedTreeNodes.get(level).stream().collect(Collectors.toList());
    }
}
