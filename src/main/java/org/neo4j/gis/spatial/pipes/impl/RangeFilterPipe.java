/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

package org.neo4j.gis.spatial.pipes.impl;

import java.util.NoSuchElementException;

/**
 * The RangeFilterPipe will only allow a sequential subset of its incoming objects to be emitted to its output.
 * This pipe can be provided -1 for both its high and low range to denote a wildcard for high and/or low.
 * Note that -1 for both high and low is equivalent to the IdentityPipe.
 *
 * @author <a href="http://markorodriguez.com" >Marko A. Rodriguez</a>
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

	@Override
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

	@Override
	public String toString() {
		return String.format("%s (%d, %d)", getClass().getSimpleName(), low, high);
	}

	@Override
	public void reset() {
		this.counter = -1;
		super.reset();
	}
}
