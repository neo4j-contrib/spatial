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
package org.neo4j.spatial.api;

import org.neo4j.gis.spatial.index.Envelope;

/**
 * Operations on {@link Envelope} that Neo4j's record does not provide.
 */
public final class Envelopes {

	private Envelopes() {
	}

	/**
	 * @return a zero-size envelope at the given point
	 */
	public static Envelope ofPoint(double[] p) {
		return new Envelope(p, p);
	}

	public static double[] centre(Envelope e) {
		double[] centre = new double[e.getDimension()];
		for (int i = 0; i < centre.length; i++) {
			centre[i] = centre(e, i);
		}
		return centre;
	}

	public static double centre(Envelope e, int dimension) {
		return (e.getMin(dimension) + e.getMax(dimension)) / 2.0;
	}

	/**
	 * @return a new envelope covering both; neither input is modified
	 */
	public static Envelope bbox(Envelope a, Envelope b) {
		Envelope result = new Envelope(a);
		result.expandToInclude(b);
		return result;
	}

	/**
	 * @return area of the combined bounding box not covered by either envelope (negative if they overlap)
	 */
	public static double separation(Envelope a, Envelope b) {
		return bbox(a, b).getArea() - a.getArea() - b.getArea();
	}

	public static double separation(Envelope a, Envelope b, int dimension) {
		return bbox(a, b).getWidth(dimension) - a.getWidth(dimension) - b.getWidth(dimension);
	}
}
