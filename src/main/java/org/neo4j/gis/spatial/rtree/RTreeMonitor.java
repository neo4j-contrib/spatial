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

import java.util.HashMap;
import java.util.Map;

public class RTreeMonitor implements TreeMonitor {
    private int nbrSplit;
    private int height;
    private int nbrRebuilt;
    private HashMap<String, Integer> cases = new HashMap<>();

    public RTreeMonitor() {
        reset();
    }

    @Override
    public void addHight() {
        height++;
    }

    public int getHeight() {
        return height;
    }

    @Override
    public void addNbrRebuilt() {
        nbrRebuilt++;
    }

    @Override
    public int getNbrRebuilt() {
        return nbrRebuilt;
    }

    @Override
    public void addSplit() {
        nbrSplit++;
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
    }
}
