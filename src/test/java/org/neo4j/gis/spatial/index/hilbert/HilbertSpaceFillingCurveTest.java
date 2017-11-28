package org.neo4j.gis.spatial.index.hilbert;

import org.junit.Test;
import org.neo4j.gis.spatial.rtree.Envelope;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class HilbertSpaceFillingCurveTest {

    @Test
    public void shouldCreateSimpleHilberCurveOfOneLevel() {
        Envelope envelope = new Envelope(-8, 8, -8, 8);
        HilbertSpaceFillingCurve curve = new HilbertSpaceFillingCurve(envelope, 1);
        assertAtLevel(curve, envelope);
        //assertRange("Bottom-left should evaluate to zero", curve, getTileEnvelope(envelope, 0, 0, 2), 0L);
        assertRange("Top-left should evaluate to one", curve, getTileEnvelope(envelope, 0, 1, 2), 1L);
        assertRange("Top-right should evaluate to two", curve, getTileEnvelope(envelope, 1, 1, 2), 2L);
        assertRange("Bottom-right should evaluate to three", curve, getTileEnvelope(envelope, 1, 0, 2), 3L);
    }

    @Test
    public void shouldCreateSimpleHilberCurveOfTwoLevels() {
        Envelope envelope = new Envelope(-8, 8, -8, 8);
        HilbertSpaceFillingCurve curve = new HilbertSpaceFillingCurve(envelope, 2);
        assertAtLevel(curve, envelope);
        assertRange("'00' should evaluate to 0", curve, getTileEnvelope(envelope, 0, 0, 4), 0L);
        assertRange("'10' should evaluate to 1", curve, getTileEnvelope(envelope, 1, 0, 4), 1L);
        assertRange("'11' should evaluate to 2", curve, getTileEnvelope(envelope, 1, 1, 4), 2L);
        assertRange("'01' should evaluate to 3", curve, getTileEnvelope(envelope, 0, 1, 4), 3L);
        assertRange("'02' should evaluate to 4", curve, getTileEnvelope(envelope, 0, 2, 4), 4L);
        assertRange("'03' should evaluate to 5", curve, getTileEnvelope(envelope, 0, 3, 4), 5L);
        assertRange("'13' should evaluate to 6", curve, getTileEnvelope(envelope, 1, 3, 4), 6L);
        assertRange("'12' should evaluate to 7", curve, getTileEnvelope(envelope, 1, 2, 4), 7L);
        assertRange("'22' should evaluate to 8", curve, getTileEnvelope(envelope, 2, 2, 4), 8L);
        assertRange("'23' should evaluate to 9", curve, getTileEnvelope(envelope, 2, 3, 4), 9L);
        assertRange("'33' should evaluate to 10", curve, getTileEnvelope(envelope, 3, 3, 4), 10L);
        assertRange("'32' should evaluate to 11", curve, getTileEnvelope(envelope, 3, 2, 4), 11L);
        assertRange("'31' should evaluate to 12", curve, getTileEnvelope(envelope, 3, 1, 4), 12L);
        assertRange("'21' should evaluate to 13", curve, getTileEnvelope(envelope, 2, 1, 4), 13L);
        assertRange("'20' should evaluate to 14", curve, getTileEnvelope(envelope, 2, 0, 4), 14L);
        assertRange("'30' should evaluate to 15", curve, getTileEnvelope(envelope, 3, 0, 4), 15L);
    }

    @Test
    public void shouldCreateSimpleHilberCurveOfThreeLevels() {
        assertAtLevel(new Envelope(-8, 8, -8, 8), 3);
    }

    @Test
    public void shouldCreateSimpleHilberCurveOfFourLevels() {
        assertAtLevel(new Envelope(-8, 8, -8, 8), 4);
    }

    @Test
    public void shouldCreateSimpleHilberCurveOfFiveLevels() {
        assertAtLevel(new Envelope(-8, 8, -8, 8), 5);
    }

    @Test
    public void shouldCreateSimpleHilberCurveOfTwentyFourLevels() {
        assertAtLevel(new Envelope(-8, 8, -8, 8), 24);
    }

    @Test
    public void shouldCreateSimpleHilberCurveOfDefaultLevels() {
        Envelope envelope = new Envelope(-8, 8, -8, 8);
        assertAtLevel(new HilbertSpaceFillingCurve(envelope), envelope);
    }

    @Test
    public void shouldCreateHilbertCurveWithRectangularEnvelope() {
        assertAtLevel(new Envelope(-8, 8, -20,20 ), 3);
    }

    @Test
    public void shouldCreateHilbertCurveWithNonCenteredEnvelope() {
        assertAtLevel(new Envelope(2, 7, 2,7 ), 3);
    }

    @Test
    public void shouldCreateHilbertCurveOfThreeLevelsFromExampleInThePaper() {
        HilbertSpaceFillingCurve curve = new HilbertSpaceFillingCurve(new Envelope(0, 8, 0, 8), 3);
        assertThat("Example should evaluate to 101110", curve.longValueFor(6, 4), equalTo(46L));
    }

    @Test
    public void shouldWorkWithNormalGPSCoordinates() {
        Envelope envelope = new Envelope(-180, 180, -90, 90);
        HilbertSpaceFillingCurve curve = new HilbertSpaceFillingCurve(envelope);
        assertAtLevel(curve, envelope);
    }

    @Test
    public void shouldGetSearchTilesForLevelOne() {
        Envelope envelope = new Envelope(-8, 8, -8, 8);
        HilbertSpaceFillingCurve curve = new HilbertSpaceFillingCurve(envelope, 1);
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-6, -5, -6, -5)), new HilbertSpaceFillingCurve.LongRange(0, 0));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(0, 6, -6, -5)), new HilbertSpaceFillingCurve.LongRange(3, 3));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-6, 4, -5, -2)), new HilbertSpaceFillingCurve.LongRange(0, 0), new HilbertSpaceFillingCurve.LongRange(3, 3));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-2, -1, -6, 5)), new HilbertSpaceFillingCurve.LongRange(0, 1));
        assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(-2, 1, -6, 5)), new HilbertSpaceFillingCurve.LongRange(0, 3));
    }

    private void assertTiles(List<HilbertSpaceFillingCurve.LongRange> results, HilbertSpaceFillingCurve.LongRange... expected) {
        assertThat("Result should have same size as expected", results.size(), equalTo(expected.length));
        for (int i = 0; i < results.size(); i++) {
            assertThat("Result at " + i + " should be the same", results.get(i), equalTo(expected[i]));
        }
    }

    private Envelope getTileEnvelope(Envelope envelope, int xindex, int yindex, int divisor) {
        double width = envelope.getWidth(0) / divisor;
        double height = envelope.getWidth(1) / divisor;
        return new Envelope(
                envelope.getMinX() + xindex * width,
                envelope.getMinX() + (xindex + 1) * width,
                envelope.getMinY() + yindex * height,
                envelope.getMinY() + (yindex + 1) * height
        );
    }

    private void assertRange(String message, HilbertSpaceFillingCurve curve, Envelope range, long value) {
        for (double x = range.getMinX(); x < range.getMaxX(); x += 1.0) {
            for (double y = range.getMinY(); y < range.getMaxY(); y += 1.0) {
                assertCurveAt(message, curve, x, y, value);
            }
        }
    }

    private void assertCurveAt(String message, HilbertSpaceFillingCurve curve, double x, double y, long value) {
        double[] halfTileWidths = new double[]{
                curve.getTileWidth(0, curve.getMaxLevel()) / 2.0,
                curve.getTileWidth(1, curve.getMaxLevel()) / 2.0
        };
        long result = curve.longValueFor(x, y);
        double[] coordinate = curve.centerPointFor(result);
        assertThat(message + ": (" + x + "," + y + ")", result, equalTo(value));
        assertThat(message + ": (" + x + "," + y + ")", Math.abs(coordinate[0] - x), lessThanOrEqualTo(halfTileWidths[0]));
        assertThat(message + ": (" + x + "," + y + ")", Math.abs(coordinate[1] - y), lessThanOrEqualTo(halfTileWidths[1]));
    }

    private void assertAtLevel(Envelope envelope, int level) {
        assertAtLevel(new HilbertSpaceFillingCurve(envelope, level), envelope);
    }

    private void assertAtLevel(HilbertSpaceFillingCurve curve, Envelope envelope) {
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

        assertCurveAt("Bottom-left should evaluate to zero", curve, envelope.getMinX(), envelope.getMinY(), 0);
        assertCurveAt("Just inside right edge on the bottom should evaluate to max-value", curve, justInsideMaxX, envelope.getMinY(), curve.getValueWidth() - 1);
        assertCurveAt("Just inside top-right corner should evaluate to " + topRightDescription, curve, justInsideMaxX, justInsideMaxY, topRight);
        assertCurveAt("Right on top-right corner should evaluate to " + topRightDescription, curve, envelope.getMaxX(), envelope.getMaxY(), topRight);
        assertCurveAt("Bottom-right should evaluate to max-value", curve, envelope.getMaxX(), envelope.getMinY(), curve.getValueWidth() - 1);
        assertCurveAt("Middle value should evaluate to (max-value+1) / 2", curve, midX, midY, curve.getValueWidth() / 2);
    }

}
