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

import java.util.ArrayList;
import java.util.List;

public class HilbertSpaceFillingCurve {

    /**
     * Description of the space filling curve structure
     */
    static class CurveRule {
        private int[] npointValues;
        private CurveRule[] children = null;

        private CurveRule(int... npointValues) {
            this.npointValues = npointValues;
        }

        private int indexForNPoint(int npoint) {
            for (int index = 0; index < npointValues.length; index++) {
                if (npointValues[index] == npoint) {
                    return index;
                }
            }
            return -1;
        }

        public void setChildren(CurveRule... children) {
            this.children = children;
        }

        public CurveRule childAt(int npoint) {
            return children[npoint];
        }
    }

    public static final CurveRule curveUp = new CurveRule(0, 1, 3, 2);
    public static final CurveRule curveRight = new CurveRule(0, 2, 3, 1);
    public static final CurveRule curveLeft = new CurveRule(3, 1, 0, 2);
    public static final CurveRule curveDown = new CurveRule(3, 2, 0, 1);

    static {
        curveUp.setChildren(curveRight, curveUp, curveUp, curveLeft);
        curveRight.setChildren(curveUp, curveRight, curveRight, curveDown);
        curveDown.setChildren(curveLeft, curveDown, curveDown, curveRight);
        curveLeft.setChildren(curveDown, curveLeft, curveLeft, curveUp);
    }

    public static final int MAX_LEVEL = 27;

    private final Envelope range;
    private final int maxLevel;
    private final long width;

    private double[] scalingFactor;

    public HilbertSpaceFillingCurve(Envelope range) {
        this(range, MAX_LEVEL);
    }

    public HilbertSpaceFillingCurve(Envelope range, int maxLevel) {
        this.range = range;
        this.maxLevel = maxLevel;
        if (maxLevel < 1) {
            throw new IllegalArgumentException("Hilbert index needs at least one level");
        }
        if (range.getDimension() != 2) {
            throw new IllegalArgumentException("Hilbert index does not yet support more than 2 dimensions");
        }
        this.width = (long) Math.pow(2, maxLevel);
        this.scalingFactor = new double[range.getDimension()];
        for (int dim = 0; dim < range.getDimension(); dim++) {
            scalingFactor[dim] = this.width / range.getWidth(dim);
        }
    }

    public int getMaxLevel() {
        return maxLevel;
    }

    public long getWidth() {
        return this.width;
    }

    public long getValueWidth() {
        return (long) Math.pow(2, maxLevel * range.getDimension());
    }

    public double getTileWidth(int dimension, int level) {
        return range.getWidth(dimension) / Math.pow(2, level);
    }

    /**
     * Given a coordinate in multiple dimensions, calculate its 1D value for maxLevel
     */
    public Long longValueFor(double x, double y) {
        return longValueFor(x, y, maxLevel);
    }

    /**
     * Given a coordinate in multiple dimensions, calculate its 1D value for given level
     */
    public Long longValueFor(double x, double y, int level) {
        assertValidLevel(level);
        long longX = getLongCoord(x, 0);
        long longY = getLongCoord(y, 1);
        long newValue = 0;
        long mask = 1L << (maxLevel - 1);
        int dimensions = range.getDimension();

        // First level is a single curveUp
        CurveRule currentCurve = curveUp;

        for (int i = 1; i <= maxLevel; i++) {
            if (i <= level) {
                int bitIndex = maxLevel - i;
                int bitX = (int) ((longX & mask) >> bitIndex);
                int bitY = (int) ((longY & mask) >> bitIndex);
                int npoint = bitX << 1 | bitY;
                int derivedIndex = currentCurve.indexForNPoint(npoint);
                newValue = (newValue << 2) | derivedIndex;
                mask = mask >> 1;
                currentCurve = currentCurve.childAt(derivedIndex);
            } else {
                newValue = newValue << dimensions;
            }
        }
        return newValue;
    }

    /**
     * Given a 1D value, find the center coordinate of the tile of the corresponding coordinate (2D, maxLevel)
     */
    public double[] centerPointFor(long value) {
        return centerPointFor(value, maxLevel);
    }

    /**
     * Given a 1D value, find the center coordinate of the tile of the corresponding coordinate (2D, given level)
     */
    public double[] centerPointFor(long value, int level) {
        long[] coordinate = coordinateFor(value, level);
        return new double[]{
                getDoubleCoord(coordinate[0], 0, level),
                getDoubleCoord(coordinate[1], 1, level)
        };
    }

    /**
     * Given a 1D value, find the tile it corresponds to
     */
    public long[] coordinateFor(long value, int level) {
        assertValidLevel(level);
        long mask = 3L << (maxLevel - 1) * range.getDimension();
        long[] coordinate = new long[range.getDimension()];

        // First level is a single curveUp
        CurveRule currentCurve = curveUp;

        for (int i = 1; i <= maxLevel; i++) {
            if (i <= level) {
                int bitIndex = maxLevel - i;
                int derivedIndex = (int) ((value & mask) >> bitIndex * 2);
                long npoint = currentCurve.npointValues[derivedIndex];
                long bitX = (npoint & 2) >> 1;
                long bitY = npoint & 1;
                coordinate[0] = (coordinate[0] << 1) | bitX;
                coordinate[1] = (coordinate[1] << 1) | bitY;
                mask = mask >> 2;
                currentCurve = currentCurve.childAt(derivedIndex);
            } else {
                coordinate[0] = coordinate[0] << 1;
                coordinate[1] = coordinate[1] << 1;
            }
        }
        return coordinate;
    }

    /**
     * Class for ranges of tiles
     */
    public static class LongRange {
        public final long min;
        public long max;

        public LongRange(long value) {
            this(value, value);
        }

        public LongRange(long min, long max) {
            this.min = min;
            this.max = max;
        }

        public void expandToMax(long other) {
            this.max = other;
        }

        public boolean equals(Object other) {
            return (other instanceof LongRange) && this.equals((LongRange) other);
        }

        public boolean equals(LongRange other) {
            return this.min == other.min && this.max == other.max;
        }

        public String toString() {
            return new StringBuilder().append("LongRange(").append(min).append(",").append(max).append(")").toString();
        }
    }

    private void addTilesIntersectingEnvelopeAt(SearchEnvelope search, SearchEnvelope currentExtent, CurveRule curve, long left, long right, ArrayList<LongRange> results) {
        if (right - left == 1) {
            long[] coord = coordinateFor(left, maxLevel);
            if (search.contains(coord)) {
                LongRange current = (results.size() > 0) ? results.get(results.size() - 1) : null;
                if (current != null && current.max == left - 1) {
                    current.expandToMax(left);
                } else {
                    current = new LongRange(left);
                    results.add(current);
                }
            }
        } else if (search.intersects(currentExtent)) {
            long width = (right - left) / 4;
            for (int i = 0; i < 4; i++) {
                int npoint = curve.npointValues[i];
                int bitX = (npoint & 2) >> 1;
                int bitY = npoint & 1;
                SearchEnvelope quadrant = currentExtent.quadrant(bitX, bitY);
                addTilesIntersectingEnvelopeAt(search, quadrant, curve.children[i], left + i * width, left + (i + 1) * width, results);
            }
        }
    }

    // TODO: This class could be generalized to n-dimensions using code in Envelope.java
    private class SearchEnvelope {
        long minX;
        long maxX;
        long minY;
        long maxY;

        private SearchEnvelope(Envelope referenceEnvelope) {
            this.minX = getLongCoord(referenceEnvelope.getMin(0), 0);
            this.maxX = getLongCoord(referenceEnvelope.getMax(0), 0);
            this.minY = getLongCoord(referenceEnvelope.getMin(1), 1);
            this.maxY = getLongCoord(referenceEnvelope.getMax(1), 1);
        }

        private SearchEnvelope(long minX, long maxX, long minY, long maxY) {
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
        }

        private SearchEnvelope quadrant(int x, int y) {
            long width = (maxX - minX) / 2;
            long height = (maxY - minY) / 2;
            return new SearchEnvelope(this.minX + x * width, this.minX + (x + 1) * width, this.minY + y * height, this.minY + (y + 1) * height);
        }

        private boolean contains(long[] coord) {
            return coord[0] >= minX && coord[0] <= maxX && coord[1] >= minY && coord[1] <= maxY;
        }

        private boolean intersects(SearchEnvelope other) {
            return this.maxX >= other.minX && this.minX <= other.maxX && this.maxY >= other.minY && this.minY <= other.maxY;
        }
    }

    /**
     * Given an envelope, find a collection of LongRange of tiles intersecting it on maxLevel and merge adjacent ones
     */
    public List<LongRange> getTilesIntersectingEnvelope(Envelope referenceEnvelope) {
        SearchEnvelope search = new SearchEnvelope(referenceEnvelope);
        ArrayList<LongRange> results = new ArrayList<>();

        addTilesIntersectingEnvelopeAt(search,
                new SearchEnvelope(0, this.getWidth(), 0, this.getWidth()),
                curveUp, 0, this.getValueWidth(), results);
        return results;
    }

    /**
     * Given a coordinate, find a long value describing the tile it is located in (1D)
     */
    private long getLongCoord(double value, int dimension) {
        if (value >= range.getMax(dimension)) {
            return width - 1;
        } else if (value < range.getMin(dimension)) {
            return 0;
        } else {
            return (long) ((value - range.getMin(dimension)) * scalingFactor[dimension]);
        }
    }

    /**
     * Given a long value describing a tile, find the center coordinate of the tile (1D)
     */
    private double getDoubleCoord(long value, int dimension, int level) {
        double coordinate = ((double) value) / scalingFactor[dimension] + range.getMin(dimension) + getTileWidth(dimension, level) / 2.0;
        return Math.min(range.getMax(dimension), Math.max(range.getMin(dimension), coordinate));
    }

    /**
     * Assert that a given level is valid
     */
    private void assertValidLevel(int level) {
        if (level > maxLevel) {
            throw new IllegalArgumentException("Level " + level + " greater than max-level " + maxLevel);
        }
    }

}
