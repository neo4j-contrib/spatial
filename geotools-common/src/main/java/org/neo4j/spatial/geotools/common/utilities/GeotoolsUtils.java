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

package org.neo4j.spatial.geotools.common.utilities;

import org.geotools.geometry.jts.ReferencedEnvelope;

public class GeotoolsUtils {

	/**
	 * Adjust the size and position of a ReferencedEnvelope using fractions of
	 * the current size. For example:
	 *
	 * <pre>
	 * bounds = adjustBounds(bounds, 0.3, new double[] { -0.1, 0.1 });
	 * </pre>
	 * <p>
	 * This will zoom in to show 30% of the height and width, and will also
	 * move the visible window 10% to the left and 10% up.
	 *
	 * @param bounds       current envelope
	 * @param zoomFactor   fraction of size to zoom in by
	 * @param offsetFactor fraction of size to offset visible window by
	 * @return adjusted envelope
	 */
	public static ReferencedEnvelope adjustBounds(ReferencedEnvelope bounds,
			double zoomFactor, double[] offsetFactor) {
		if (offsetFactor == null || offsetFactor.length < bounds.getDimension()) {
			offsetFactor = new double[bounds.getDimension()];
		}
		ReferencedEnvelope scaled = new ReferencedEnvelope(bounds);
		if (Math.abs(zoomFactor - 1.0) > 0.01) {
			double[] min = scaled.getLowerCorner().getCoordinate();
			double[] max = scaled.getUpperCorner().getCoordinate();
			for (int i = 0; i < scaled.getDimension(); i++) {
				double span = scaled.getSpan(i);
				double delta = (span - span * zoomFactor) / 2.0;
				double shift = span * offsetFactor[i];
				min[i] += shift + delta;
				max[i] += shift - delta;
			}
			scaled = new ReferencedEnvelope(min[0], max[0], min[1], max[1],
					scaled.getCoordinateReferenceSystem());
		}
		return scaled;
	}
}

