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
package org.neo4j.gis.spatial.rtree;

public class Envelope extends org.neo4j.gis.spatial.index.Envelope {

	/**
	 * Copy constructor
	 */
	public Envelope(org.neo4j.gis.spatial.index.Envelope e) {
		super(e.getMin(), e.getMax());
	}

	/**
	 * General constructor for the n-dimensional case
	 */
	public Envelope(double[] min, double[] max) {
		super(min, max);
	}

	/**
	 * General constructor for the n-dimensional case starting with a single point
	 */
	public Envelope(double[] p) {
		super(p.clone(), p.clone());
	}

	/**
	 * Special constructor for the 2D case
	 */
	public Envelope(double xmin, double xmax, double ymin, double ymax) {
		super(xmin, xmax, ymin, ymax);
	}

	/**
	 * Note that this doesn't exclude the envelope boundary.
	 * See JTS Envelope.
	 */
	public boolean contains(Envelope other) {
		//TODO: We can remove this method and covers method if we determine why super.covers does not do boolean shortcut
		return covers(other);
	}

	public boolean covers(Envelope other) {
		boolean ans = getDimension() == other.getDimension();
		for (int i = 0; i < min.length; i++) {
			//TODO: Why does the parent class not use this shortcut?
			if (!ans) {
				return ans;
			}
			ans = other.min[i] >= min[i] && other.max[i] <= max[i];
		}
		return ans;
	}

	public void scaleBy(double factor) {
		for (int i = 0; i < min.length; i++) {
			scaleBy(factor, i);
		}
	}

	private void scaleBy(double factor, int dimension) {
		max[dimension] = min[dimension] + (max[dimension] - min[dimension]) * factor;
	}

	public void shiftBy(double offset) {
		for (int i = 0; i < min.length; i++) {
			shiftBy(offset, i);
		}
	}

	public void shiftBy(double offset, int dimension) {
		min[dimension] += offset;
		max[dimension] += offset;
	}

	public double[] centre() {
		double[] center = new double[min.length];
		for (int i = 0; i < min.length; i++) {
			center[i] = centre(i);
		}
		return center;
	}

	public double centre(int dimension) {
		return (min[dimension] + max[dimension]) / 2.0;
	}

	public void expandToInclude(double[] p) {
		for (int i = 0; i < Math.min(p.length, min.length); i++) {
			if (p[i] < min[i]) {
				min[i] = p[i];
			}
			if (p[i] > max[i]) {
				max[i] = p[i];
			}
		}
	}

	public double separation(Envelope other) {
		Envelope combined = new Envelope(this);
		combined.expandToInclude(other);
		return combined.getArea() - this.getArea() - other.getArea();
	}

	public double separation(Envelope other, int dimension) {
		Envelope combined = new Envelope(this);
		combined.expandToInclude(other);
		return combined.getWidth(dimension) - this.getWidth(dimension) - other.getWidth(dimension);
	}

	public Envelope intersection(Envelope other) {
		return new Envelope(super.intersection(other));
	}

	public Envelope bbox(Envelope other) {
		if (getDimension() == other.getDimension()) {
			Envelope result = new Envelope(this);
			result.expandToInclude(other);
			return result;
		}
		throw new IllegalArgumentException(
				"Cannot calculate bounding box of Envelopes with different dimensions: " + this.getDimension()
						+ " != " + other.getDimension());
	}
}
