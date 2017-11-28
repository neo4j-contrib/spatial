package org.neo4j.gis.spatial.index.hilbert;

import org.junit.Test;
import org.neo4j.gis.spatial.rtree.Envelope;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class HilbertSpaceFillingCurveTest {

    @Test
    public void shouldCreateSimpleHilberCurveOfOneLevel() {
        assertAtLevel(new Envelope(-8, 8, -8, 8), 1);
    }

    @Test
    public void shouldCreateSimpleHilberCurveOfTwoLevels() {
        assertAtLevel(new Envelope(-8, 8, -8, 8), 2);
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
                assertThat(message + ": (" + x + "," + y + ")", curve.longValueFor(x, y), equalTo(value));
            }
        }
    }

    private void assertAtLevel(Envelope envelope, int level) {
        assertAtLevel(new HilbertSpaceFillingCurve(envelope, level), envelope);
    }

    private void assertAtLevel(HilbertSpaceFillingCurve curve, Envelope envelope) {
        int level = curve.getMaxLevel();
        long width = (long) Math.pow(2, level);
        long maxValue = width * width - 1;
        double justInsideMaxX = envelope.getMaxX() - curve.getTileWidth(0)/2.0;
        double justInsideMaxY = envelope.getMaxY() - curve.getTileWidth(1)/2.0;
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
        assertThat("Level " + level + " should have max value of " + maxValue, curve.getMaxValue(), equalTo(maxValue));

        assertThat("Bottom-left should evaluate to zero", curve.longValueFor(envelope.getMinX(), envelope.getMinY()), equalTo(0L));
        assertThat("Just inside right edge on the bottom should evaluate to max-value", curve.longValueFor(justInsideMaxX, envelope.getMinY()), equalTo(curve.getMaxValue()));
        assertThat("Just inside top-right corner should evaluate to " + topRightDescription, curve.longValueFor(justInsideMaxX, justInsideMaxY), equalTo(topRight));
        assertThat("Right on top-right corner should evaluate to " + topRightDescription, curve.longValueFor(envelope.getMaxX(), envelope.getMaxY()), equalTo(topRight));
        assertThat("Bottom-right should evaluate to max-value", curve.longValueFor(envelope.getMaxX(), envelope.getMinY()), equalTo(curve.getMaxValue()));
        assertThat("Middle value should evaluate to (max-value+1) / 2", curve.longValueFor(midX, midY), equalTo((curve.getMaxValue() + 1) / 2));
    }

}
