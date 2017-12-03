package org.neo4j.gis.spatial.index.hilbert;

import org.junit.Test;
import org.neo4j.gis.spatial.rtree.Envelope;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.neo4j.gis.spatial.index.hilbert.HilbertSpaceFillingCurve3D.BinaryCoordinateRotationUtils3D.rotateNPointLeft;
import static org.neo4j.gis.spatial.index.hilbert.HilbertSpaceFillingCurve3D.BinaryCoordinateRotationUtils3D.rotateNPointRight;

public class HilbertSpaceFillingCurveTest {

    @Test
    public void shouldCreateSimpleHilberCurve2DOfOneLevel() {
        Envelope envelope = new Envelope(-8, 8, -8, 8);
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D(envelope, 1);
        assertAtLevel(curve, envelope);
        assertRange("Bottom-left should evaluate to zero", curve, getTileEnvelope(envelope, 2, 0, 0), 0L);
        assertRange("Top-left should evaluate to one", curve, getTileEnvelope(envelope, 2, 0, 1), 1L);
        assertRange("Top-right should evaluate to two", curve, getTileEnvelope(envelope, 2, 1, 1), 2L);
        assertRange("Bottom-right should evaluate to three", curve, getTileEnvelope(envelope, 2, 1, 0), 3L);
    }

    @Test
    public void shouldRotate3DNPointsLeft() {
        assertThat(rotateNPointLeft(0b000), equalTo(0b000));
        assertThat(rotateNPointLeft(0b001), equalTo(0b010));
        assertThat(rotateNPointLeft(0b010), equalTo(0b100));
        assertThat(rotateNPointLeft(0b100), equalTo(0b001));
        assertThat(rotateNPointLeft(0b011), equalTo(0b110));
        assertThat(rotateNPointLeft(0b110), equalTo(0b101));
        assertThat(rotateNPointLeft(0b101), equalTo(0b011));
        assertThat(rotateNPointLeft(0b111), equalTo(0b111));
    }

    @Test
    public void shouldRotate3DNPointsRight() {
        assertThat(rotateNPointRight(0b000), equalTo(0b000));
        assertThat(rotateNPointRight(0b001), equalTo(0b100));
        assertThat(rotateNPointRight(0b100), equalTo(0b010));
        assertThat(rotateNPointRight(0b010), equalTo(0b001));
        assertThat(rotateNPointRight(0b011), equalTo(0b101));
        assertThat(rotateNPointRight(0b101), equalTo(0b110));
        assertThat(rotateNPointRight(0b110), equalTo(0b011));
        assertThat(rotateNPointRight(0b111), equalTo(0b111));
    }

    @Test
    public void shouldCreateSimpleHilbertCurve3DOfOneLevel() {
        Envelope envelope = new Envelope(new double[]{-8, -8, -8}, new double[]{8, 8, 8});
        HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D(envelope, 1);
        assertAtLevel(curve, envelope);
        assertRange("Bottom-left-back should evaluate to zero", curve, getTileEnvelope(envelope, 2, 0, 0, 0), 0L);
        assertRange("Top-left-back should evaluate to one", curve, getTileEnvelope(envelope, 2, 0, 1, 0), 1L);
        assertRange("Top-left-front should evaluate to two", curve, getTileEnvelope(envelope, 2, 0, 1, 1), 2L);
        assertRange("Bottom-left-front should evaluate to three", curve, getTileEnvelope(envelope, 2, 0, 0, 1), 3L);
        assertRange("Bottom-right-front should evaluate to four", curve, getTileEnvelope(envelope, 2, 1, 0, 1), 4L);
        assertRange("Top-right-front should evaluate to five", curve, getTileEnvelope(envelope, 2, 1, 1, 1), 5L);
        assertRange("Top-right-back should evaluate to six", curve, getTileEnvelope(envelope, 2, 1, 1, 0), 6L);
        assertRange("Bottom-right-back should evaluate to seven", curve, getTileEnvelope(envelope, 2, 1, 0, 0), 7L);
    }

    @Test
    public void shouldCreateSimpleHilberCurve2DOfTwoLevels() {
        Envelope envelope = new Envelope(-8, 8, -8, 8);
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D(envelope, 2);
        assertAtLevel(curve, envelope);
        assertRange("'00' should evaluate to 0", curve, getTileEnvelope(envelope, 4, 0, 0), 0L);
        assertRange("'10' should evaluate to 1", curve, getTileEnvelope(envelope, 4, 1, 0), 1L);
        assertRange("'11' should evaluate to 2", curve, getTileEnvelope(envelope, 4, 1, 1), 2L);
        assertRange("'01' should evaluate to 3", curve, getTileEnvelope(envelope, 4, 0, 1), 3L);
        assertRange("'02' should evaluate to 4", curve, getTileEnvelope(envelope, 4, 0, 2), 4L);
        assertRange("'03' should evaluate to 5", curve, getTileEnvelope(envelope, 4, 0, 3), 5L);
        assertRange("'13' should evaluate to 6", curve, getTileEnvelope(envelope, 4, 1, 3), 6L);
        assertRange("'12' should evaluate to 7", curve, getTileEnvelope(envelope, 4, 1, 2), 7L);
        assertRange("'22' should evaluate to 8", curve, getTileEnvelope(envelope, 4, 2, 2), 8L);
        assertRange("'23' should evaluate to 9", curve, getTileEnvelope(envelope, 4, 2, 3), 9L);
        assertRange("'33' should evaluate to 10", curve, getTileEnvelope(envelope, 4, 3, 3), 10L);
        assertRange("'32' should evaluate to 11", curve, getTileEnvelope(envelope, 4, 3, 2), 11L);
        assertRange("'31' should evaluate to 12", curve, getTileEnvelope(envelope, 4, 3, 1), 12L);
        assertRange("'21' should evaluate to 13", curve, getTileEnvelope(envelope, 4, 2, 1), 13L);
        assertRange("'20' should evaluate to 14", curve, getTileEnvelope(envelope, 4, 2, 0), 14L);
        assertRange("'30' should evaluate to 15", curve, getTileEnvelope(envelope, 4, 3, 0), 15L);
    }

    @Test
    public void shouldCreateSimpleHilberCurve2DOfThreeLevels() {
        assert2DAtLevel(new Envelope(-8, 8, -8, 8), 3);
    }

    @Test
    public void shouldCreateSimpleHilberCurve2DOfFourLevels() {
        assert2DAtLevel(new Envelope(-8, 8, -8, 8), 4);
    }

    @Test
    public void shouldCreateSimpleHilberCurve2DOfFiveLevels() {
        assert2DAtLevel(new Envelope(-8, 8, -8, 8), 5);
    }

    @Test
    public void shouldCreateSimpleHilberCurve2DOfTwentyFourLevels() {
        assert2DAtLevel(new Envelope(-8, 8, -8, 8), 24);
    }

    @Test
    public void shouldCreateSimpleHilberCurve2DOfDefaultLevels() {
        Envelope envelope = new Envelope(-8, 8, -8, 8);
        assertAtLevel(new HilbertSpaceFillingCurve2D(envelope), envelope);
    }

    @Test
    public void shouldCreateHilbertCurveWithRectangularEnvelope() {
        assert2DAtLevel(new Envelope(-8, 8, -20, 20), 3);
    }

    @Test
    public void shouldCreateHilbertCurveWithNonCenteredEnvelope() {
        assert2DAtLevel(new Envelope(2, 7, 2, 7), 3);
    }

    @Test
    public void shouldCreateHilbertCurveOfThreeLevelsFromExampleInThePaper() {
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D(new Envelope(0, 8, 0, 8), 3);
        assertThat("Example should evaluate to 101110", curve.derivedValueFor(new double[]{6, 4}), equalTo(46L));
    }

    @Test
    public void shouldWorkWithNormalGPSCoordinates() {
        Envelope envelope = new Envelope(-180, 180, -90, 90);
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D(envelope);
        assertAtLevel(curve, envelope);
    }

    @Test
    public void shouldGetSearchTilesForLevelOne() {
        Envelope envelope = new Envelope(-8, 8, -8, 8);
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D(envelope, 1);
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-6, -5, -6, -5)), new HilbertSpaceFillingCurve2D.LongRange(0, 0));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(0, 6, -6, -5)), new HilbertSpaceFillingCurve2D.LongRange(3, 3));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-6, 4, -5, -2)), new HilbertSpaceFillingCurve2D.LongRange(0, 0), new HilbertSpaceFillingCurve2D.LongRange(3, 3));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-2, -1, -6, 5)), new HilbertSpaceFillingCurve2D.LongRange(0, 1));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-2, 1, -6, 5)), new HilbertSpaceFillingCurve2D.LongRange(0, 3));
    }

    @Test
    public void shouldGetSearchTilesForLevelTwo() {
        Envelope envelope = new Envelope(-8, 8, -8, 8);
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D(envelope, 2);
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-6, -5, -6, -5)), new HilbertSpaceFillingCurve2D.LongRange(0, 0));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(0, 6, -6, -5)), new HilbertSpaceFillingCurve2D.LongRange(14, 15));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-6, 4, -5, -2)), new HilbertSpaceFillingCurve2D.LongRange(0, 3), new HilbertSpaceFillingCurve2D.LongRange(12, 15));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-2, -1, -6, 5)), new HilbertSpaceFillingCurve2D.LongRange(1, 2), new HilbertSpaceFillingCurve2D.LongRange(6, 7));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-2, 1, -6, 5)), new HilbertSpaceFillingCurve2D.LongRange(1, 2), new HilbertSpaceFillingCurve2D.LongRange(6, 9), new HilbertSpaceFillingCurve2D.LongRange(13, 14));
    }

    @Test
    public void shouldGetSearchTilesForLevelThree() {
        Envelope envelope = new Envelope(-8, 8, -8, 8);
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D(envelope, 3);
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-8, -7, -8, -7)), new HilbertSpaceFillingCurve2D.LongRange(0, 0));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(0, 1, 0, 1)), new HilbertSpaceFillingCurve2D.LongRange(32, 32));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(7, 8, -8, -1)), new HilbertSpaceFillingCurve2D.LongRange(48, 49), new HilbertSpaceFillingCurve2D.LongRange(62, 63));
    }

    @Test
    public void shouldGetSearchTilesForManyLevels() {
        Envelope envelope = new Envelope(-8, 8, -8, 8);
        for (int level = 1; level < HilbertSpaceFillingCurve2D.MAX_LEVEL; level++) {
            HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D(envelope, level);
            System.out.print("Testing hilbert query at level " + level);
            double halfTile = curve.getTileWidth(0, level) / 2.0;
            long start = System.currentTimeMillis();
            assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-8, -8 + halfTile, -8, -8 + halfTile)), new HilbertSpaceFillingCurve2D.LongRange(0, 0));
            assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(8 - halfTile, 8, -8, -8 + halfTile)), new HilbertSpaceFillingCurve2D.LongRange(curve.getValueWidth() - 1, curve.getValueWidth() - 1));
            assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(0, halfTile, 0, halfTile)), new HilbertSpaceFillingCurve2D.LongRange(curve.getValueWidth() / 2, curve.getValueWidth() / 2));
            System.out.println(", took " + (System.currentTimeMillis() - start) + "ms");
        }
    }

    private void assertTiles(List<HilbertSpaceFillingCurve2D.LongRange> results, HilbertSpaceFillingCurve2D.LongRange... expected) {
        assertThat("Result should have same size as expected", results.size(), equalTo(expected.length));
        for (int i = 0; i < results.size(); i++) {
            assertThat("Result at " + i + " should be the same", results.get(i), equalTo(expected[i]));
        }
    }

    private Envelope getTileEnvelope(Envelope envelope, int divisor, int... index) {
        double[] widths = envelope.getWidths(divisor);
        double[] min = Arrays.copyOf(envelope.getMin(), envelope.getDimension());
        double[] max = Arrays.copyOf(envelope.getMin(), envelope.getDimension());
        for (int i = 0; i < min.length; i++) {
            min[i] += index[i] * widths[i];
            max[i] += (index[i] + 1) * widths[i];
        }
        return new Envelope(min, max);
    }

    private void assertRange(String message, HilbertSpaceFillingCurve2D curve, Envelope range, long value) {
        for (double x = range.getMinX(); x < range.getMaxX(); x += 1.0) {
            for (double y = range.getMinY(); y < range.getMaxY(); y += 1.0) {
                assertCurveAt(message, curve, value, new double[]{x, y});
            }
        }
    }

    private void assertRange(String message, HilbertSpaceFillingCurve3D curve, Envelope range, long value) {
        for (double x = range.getMin(0); x < range.getMax(0); x += 1.0) {
            for (double y = range.getMin(1); y < range.getMax(1); y += 1.0) {
                for (double z = range.getMin(2); z < range.getMax(2); z += 1.0) {
                    assertCurveAt(message, curve, value, new double[]{x, y, z});
                }
            }
        }
    }

    private void assertCurveAt(String message, HilbertSpaceFillingCurve curve, long value, double... coord) {
        double[] halfTileWidths = new double[coord.length];
        for (int i = 0; i < coord.length; i++) {
            halfTileWidths[i] = curve.getTileWidth(i, curve.getMaxLevel()) / 2.0;
        }
        long result = curve.derivedValueFor(coord);
        double[] coordinate = curve.centerPointFor(result);
        assertThat(message + ": " + Arrays.toString(coord), result, equalTo(value));
        for (int i = 0; i < coord.length; i++) {
            assertThat(message + ": " + Arrays.toString(coord), Math.abs(coordinate[0] - coord[0]), lessThanOrEqualTo(halfTileWidths[0]));
        }
    }

    private void assert2DAtLevel(Envelope envelope, int level) {
        assertAtLevel(new HilbertSpaceFillingCurve2D(envelope, level), envelope);
    }

    private void assertAtLevel(HilbertSpaceFillingCurve2D curve, Envelope envelope) {
        int level = curve.getMaxLevel();
        long width = (long) Math.pow(2, level);
        long valueWidth = width * width;
        double justInsideMaxX = envelope.getMaxX() - curve.getTileWidth(0, level) / 2.0;
        double justInsideMaxY = envelope.getMaxY() - curve.getTileWidth(1, level) / 2.0;
        double midX = (envelope.getMinX() + envelope.getMaxX()) / 2.0;
        double midY = (envelope.getMinY() + envelope.getMaxY()) / 2.0;

        long topRight = 0L;
        long topRightFactor = 2L;
        StringBuilder topRightDescription = new StringBuilder();
        for (int l = 0; l < level; l++) {
            topRight += topRightFactor;
            if (topRightDescription.length() == 0) {
                topRightDescription.append(topRightFactor);
            } else {
                topRightDescription.append(" + ").append(topRightFactor);
            }
            topRightFactor *= 4;
        }

        assertThat("Level " + level + " should have width of " + width, curve.getWidth(), equalTo(width));
        assertThat("Level " + level + " should have max value of " + valueWidth, curve.getValueWidth(), equalTo(valueWidth));

        assertCurveAt("Bottom-left should evaluate to zero", curve, 0, envelope.getMinX(), envelope.getMinY());
        assertCurveAt("Just inside right edge on the bottom should evaluate to max-value", curve, curve.getValueWidth() - 1, justInsideMaxX, envelope.getMinY());
        assertCurveAt("Just inside top-right corner should evaluate to " + topRightDescription, curve, topRight, justInsideMaxX, justInsideMaxY);
        assertCurveAt("Right on top-right corner should evaluate to " + topRightDescription, curve, topRight, envelope.getMaxX(), envelope.getMaxY());
        assertCurveAt("Bottom-right should evaluate to max-value", curve, curve.getValueWidth() - 1, envelope.getMaxX(), envelope.getMinY());
        assertCurveAt("Middle value should evaluate to (max-value+1) / 2", curve, curve.getValueWidth() / 2, midX, midY);
    }

    private void assert3DAtLevel(Envelope envelope, int level) {
        assertAtLevel(new HilbertSpaceFillingCurve3D(envelope, level), envelope);
    }

    private void assertAtLevel(HilbertSpaceFillingCurve3D curve, Envelope envelope) {
        int level = curve.getMaxLevel();
        int dimension = curve.rootCurve().dimension;
        long width = (long) Math.pow(2, level);
        long valueWidth = (long) Math.pow(width, dimension);
        double midY = (envelope.getMax(1) + envelope.getMin(1)) / 2.0;
        double[] justInsideMax = new double[dimension];
        double[] locationOfHalfCurve = new double[]{
                (envelope.getMin(0) + envelope.getMax(0)) / 2.0,            // mid-way on x
                envelope.getMin(1) + curve.getTileWidth(1, level) / 2.0,    // near bottom of y
                envelope.getMax(1) - curve.getTileWidth(2, level) / 2.0     // near front of z
        };
        for (int i = 0; i < dimension; i++) {
            justInsideMax[i] = envelope.getMax(i) - curve.getTileWidth(i, level) / 2.0;
        }

        long frontRightMid = valueWidth / 2 + valueWidth / 8;
        String fromRightMidDescription = new StringBuilder().append(valueWidth).append("/2 + ").append(valueWidth).append("/8").toString();

        assertThat("Level " + level + " should have width of " + width, curve.getWidth(), equalTo(width));
        assertThat("Level " + level + " should have max value of " + valueWidth, curve.getValueWidth(), equalTo(valueWidth));

        assertCurveAt("Bottom-left should evaluate to zero", curve, 0, envelope.getMin());
        assertCurveAt("Just inside right edge on the bottom back should evaluate to max-value", curve, curve.getValueWidth() - 1, replaceOne(envelope.getMin(), justInsideMax[0], 0));
        assertCurveAt("Just above front-right-mid edge should evaluate to " + fromRightMidDescription, curve, frontRightMid, replaceOne(justInsideMax, midY + curve.getTileWidth(1, level) / 2.0, 1));
        assertCurveAt("Right on top-right-front corner should evaluate to " + fromRightMidDescription, curve, frontRightMid, replaceOne(envelope.getMax(), midY, 1));
        assertCurveAt("Bottom-right-back should evaluate to max-value", curve, curve.getValueWidth() - 1, replaceOne(envelope.getMin(), envelope.getMax(0), 0));
        assertCurveAt("Middle value should evaluate to (max-value+1) / 2", curve, curve.getValueWidth() / 2, locationOfHalfCurve);
    }

    private double[] replaceOne(double[] values, double value, int index) {
        double[] newValues = Arrays.copyOf(values, values.length);
        newValues[index] = value;
        return newValues;
    }
}
