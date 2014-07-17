/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.rtree;


public class Envelope {

	/**
	 * Copy constructor
	 */
	public Envelope(Envelope e) {
		this(e.min, e.max);
	}

	/**
	 * General constructor for the n-dimensional case
	 * @param min
	 * @param max
	 */
	public Envelope(double[] min, double[] max) {
		this.min = min.clone();
		this.max = max.clone();
		if (!isValid()) {
			throw new RuntimeException("Invalid envelope created " + toString());
		}
	}
	
	/**
	 * General constructor for the n-dimensional case starting with a single point
	 * @param min
	 * @param max
	 */
	public Envelope(double[] p) {
		this.min = p.clone();
		this.max = p.clone();
	}
	
	/**
	 * Special constructor for the 2D case
	 * @param xmin
	 * @param xmax
	 * @param ymin
	 * @param ymax
	 */
	public Envelope(double xmin, double xmax, double ymin, double ymax) {
		this(new double[] { xmin, ymin }, new double[] { xmax, ymax });
	}
	
	/**
	 * Creates an empty envelope with unknown dimension. Be sure to add something to this before attempting 
	 * to access the contents. 
	 */
	public Envelope() {
	}
	
	
	// Public methods

	public double getMin(int dimension) {
		return min[dimension];
	}

	public double getMax(int dimension) {
		return max[dimension];
	}

	public double getMinX() {
		return getMin(0);
	}

	public double getMaxX() {
		return getMax(0);
	}	
	
	public double getMinY() {
		return getMin(1);
	}
		
	public double getMaxY() {
		return getMax(1);
	}
	
	public int getDimension() {
		return isValid() ? min.length : 0;
	}
	
	/**
     * Note that this doesn't exclude the envelope boundary.
     * See JTS Envelope.
	 */
	public boolean contains(Envelope other) {
		return covers(other);
	}

	public boolean covers(Envelope other) {
		boolean ans = isValid() && other.isValid() && getDimension() == other.getDimension();
		for (int i = 0; i < min.length; i++) {
			if (!ans)
				return ans;
			ans = ans && other.min[i] >= min[i] && other.max[i] <= max[i];
		}
		return ans;
	}

	public boolean disjoint(Envelope other) {
		if (isValid() && other.isValid() && getDimension() == other.getDimension()) {
			return !intersects(other);
		}
		return false;
	}	

	public boolean intersects(Envelope other) {
		if (isValid() && other.isValid() && getDimension() == other.getDimension()) {
			boolean result = false;
			for (int i = 0; i < min.length; i++) {
				result = result || !(other.min[i] > max[i] || other.max[i] < min[i]);
			}
			return result;
		} else {
			return false;
		}
	}	
	
	public void expandToInclude(Envelope other) {
		if (!isValid()) {
			min = other.min.clone();
			max = other.max.clone();
		} else {
			for (int i = 0; i < min.length; i++) {
				if (other.min[i] < min[i])
					min[i] = other.min[i];
				if (other.max[i] > max[i])
					max[i] = other.max[i];
			}
		}
	}

	public void expandBy(double[] padding) {
		for(int i=0;i<min.length;i++) {
			min[i] -= padding[i];
			max[i] += padding[i];
		}
	}

	public double[] centre() {
		if (!isValid()) {
			return null;
		}
		double[] center = new double[min.length];
		for (int i = 0; i < min.length; i++) {
			center[i] = (min[i] + max[i]) / 2.0;
		}
		return center;
	}

	/**
	 * Return the distance between the two envelopes on one dimension. This can return negative values if the envelopes intersect on this dimension.
	 * @param other
	 * @param dimension
	 * @return distance between envelopes
	 */
	public double distance(Envelope other, int dimension) {
		if (min[dimension] < other.min[dimension]) {
			return other.min[dimension] - max[dimension];
		} else {
			return min[dimension] - other.max[dimension];
		}
	}

	/**
	 * Find the pythagorean distance between two envelopes
	 * @param other
	 * @return
	 */
	public double distance(Envelope other) {
		if (intersects(other)) {
			return 0;
		}

		double distance = 0.0;
		for (int i = 0; i < min.length; i++) {
			double dist = distance(other, i);
			if (dist > 0) {
				distance += dist * dist;
			}
		}
		return Math.sqrt(distance);
	}

	public void expandToInclude(double[] p) {
		if (!isValid()) {
			min = p.clone();
			max = p.clone();
		} else {
			for (int i = 0; i < Math.min(p.length, min.length); i++) {
				if (p[i] < min[i])
					min[i] = p[i];
				if (p[i] > max[i])
					max[i] = p[i];
			}
		}
	}

	public void expandToInclude(double x, double y) {
		expandToInclude(new double[] { x, y });
	}

	/**
	 * @return getWidth(1) for special 2D case with the second dimension being y (height)
	 */
	public double getHeight() {
		return getWidth(1);
	}

	/**
	 * @return getWidth(0) for special 2D case with the first dimension being x (width)
	 */
	public double getWidth() {
		return getWidth(0);
	}

	/**
	 * Return the width of the envelope at the specified dimension
	 * @param dimension
	 * @return with of that dimension, ie. max[d] - min[d]
	 */
	public double getWidth(int dimension) {
		return isValid() ? max[dimension] - min[dimension] : 0;
	}

	public double getArea() {
		double area = 1.0;
		for (int i = 0; i < min.length; i++) {
			area *= (max[i] - min[i]);
		}
		return area;
	}
	
	public boolean isValid() {
		boolean ans = min != null && max != null && min.length == max.length;
		if (!ans)
			return ans;
		for (int i = 0; i < min.length; i++) {
			ans = ans && min[i] <= max[i];
			if (!ans)
				return ans;
		}
		return ans;
	}	
	
	/**
	 * Move this Envelope by the specified offsets
	 * @param offset array of offsets
	 */
	public void translate(double[] offset) {
		for (int i = 0; i < Math.min(offset.length, min.length); i++) {
			min[i] += offset[i];
			max[i] += offset[i];
		}
	}

	public String toString() {
		return "Envelope: min=" + makeString(min) + ", max=" + makeString(max);
	}
	
	
	// Private methods
	
	private static String makeString(double[] vals) {
		StringBuffer sb = new StringBuffer();
		if (vals == null) {
			sb.append("null");
		} else {
			for (int i = 0; i < vals.length; i++) {
				if (sb.length() > 0)
					sb.append(",");
				else
					sb.append("(");
				sb.append(vals[i]);
			}
			if (sb.length() > 0)
				sb.append(")");
		}
		return sb.toString();
	}	
	
	
	// Attributes
	
	private double[] min;
	private double[] max;
}