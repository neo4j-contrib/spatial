/*
 * Copyright (c) 2010-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.gis.spatial.index.hilbert;

import org.neo4j.gis.spatial.rtree.Envelope;

import java.util.HashMap;
import java.util.LinkedHashMap;

public class HilbertSpaceFillingCurve2D extends HilbertSpaceFillingCurve {

    /**
     * Description of the space filling curve structure
     */
    static class CurveRule2D extends CurveRule {
        private CurveRule[] children = null;

        private CurveRule2D(int... npointValues) {
            super(2, npointValues);
            assert npointValues[0] == 0 || npointValues[0] == 3;
        }

        public char direction(int end) {
            int start = npointValues[0];
            end -= start;
            switch (end) {
                case 1:
                    return 'U'; // move up      00->01
                case 2:
                    return 'R'; // move right   00->10
                case -2:
                    return 'L'; // move left    11->01
                case -1:
                    return 'D'; // move down    11->10
                default:
                    return '-';
            }
        }

        public String name() {
            return String.valueOf(direction(npointValues[1]));
        }

        private void setChildren(CurveRule... children) {
            this.children = children;
        }

        @Override
        public CurveRule childAt(int npoint) {
            return children[npoint];
        }
    }

    private static HashMap<String, CurveRule2D> curves = new LinkedHashMap<>();

    private static CurveRule2D addCurveRule(int... npointValues) {
        CurveRule2D curve = new CurveRule2D(npointValues);
        String name = curve.name();
        if (!curves.containsKey(name)) {
            curves.put(name, curve);
        }
        return curve;
    }

    private static void setChildren(String parent, String... children) {
        CurveRule2D curve = curves.get(parent);
        CurveRule2D[] childCurves = new CurveRule2D[children.length];
        for (int i = 0; i < children.length; i++) {
            childCurves[i] = curves.get(children[i]);
        }
        curve.setChildren(childCurves);
    }

    private static final CurveRule2D curveUp;

    static {
        addCurveRule(0, 1, 3, 2);
        addCurveRule(0, 2, 3, 1);
        addCurveRule(3, 1, 0, 2);
        addCurveRule(3, 2, 0, 1);
        setChildren("U", "R", "U", "U", "L");
        setChildren("R", "U", "R", "R", "D");
        setChildren("D", "L", "D", "D", "R");
        setChildren("L", "D", "L", "L", "U");
        curveUp = curves.get("U");
    }

    public HilbertSpaceFillingCurve2D(Envelope range) {
        this(range, MAX_LEVEL);
    }

    public HilbertSpaceFillingCurve2D(Envelope range, int maxLevel) {
        super(range, maxLevel);
        assert range.getDimension() == 2;
    }

    @Override
    protected CurveRule rootCurve() {
        return curveUp;
    }
}
