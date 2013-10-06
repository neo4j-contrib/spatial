package org.neo4j.gis.spatial.pipes.impl;

import java.util.NoSuchElementException;

/**
 * The RangeFilterPipe will only allow a sequential subset of its incoming objects to be emitted to its output.
 * This pipe can be provided -1 for both its high and low range to denote a wildcard for high and/or low.
 * Note that -1 for both high and low is equivalent to the IdentityPipe.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RangeFilterPipe<S> extends AbstractPipe<S, S> implements FilterPipe<S> {

    private final long low;
    private final long high;
    private int counter = -1;

    public RangeFilterPipe(final long low, final long high) {
        this.low = low;
        this.high = high;
        if (this.low != -1 && this.high != -1 && this.low > this.high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + "]");
        }
    }

    protected S processNextStart() {
        while (true) {
            final S s = this.starts.next();
            this.counter++;
            if ((this.low == -1 || this.counter >= this.low) && (this.high == -1 || this.counter <= this.high)) {
                return s;
            }
            if (this.high != -1 && this.counter > this.high) {
                throw new NoSuchElementException();
            }
        }
    }

    public String toString() {
        return String.format("%s (%d, %d)",getClass().getSimpleName(),low,high);
    }

    public void reset() {
        this.counter = -1;
        super.reset();
    }
}
