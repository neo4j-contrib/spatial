package org.neo4j.gis.spatial.index.hilbert;

import org.neo4j.gis.spatial.rtree.Envelope;

public class HilbertSpaceFillingCurve {

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

    public long getMaxValue() {
        return (long) Math.pow(2, maxLevel * range.getDimension()) - 1;
    }

    public double getTileWidth(int dimension) {
        return 1.0/scalingFactor[dimension];
    }

    public Long longValueFor(double x, double y) {
        return longValueFor(x, y, maxLevel);
    }

    private long getLongCoord(double value, int dimension) {
        if (value >= range.getMax(dimension)) {
            return width - 1;
        } else if (value < range.getMin(dimension)) {
            return 0;
        } else {
            return (long) ((value - range.getMin(dimension)) * scalingFactor[dimension]);
        }
    }

    public Long longValueFor(double x, double y, int level) {
        if (level > maxLevel) {
            throw new IllegalArgumentException("Level " + level + " greater than max-level " + maxLevel);
        }
        long longX = getLongCoord(x, 0);
        long longY = getLongCoord(y, 1);
        long newValue = 0;
        long mask = 1 << (maxLevel - 1);
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
}
