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
package org.neo4j.gis.spatial.pipes.processing;

import org.locationtech.jts.densify.Densifier;
import org.neo4j.gis.spatial.pipes.AbstractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;

/**
 * Densify geometries by inserting extra vertices along the line segments in the geometry.
 * The densified geometry contains no line segment which is longer than the given distance tolerance.
 * Item geometry is replaced by pipe output unless an alternative property name is given in the constructor.
 */
public class Densify extends AbstractGeoPipe {

	private final double distanceTolerance;

	/**
	 * @param distanceTolerance maximum distance between vertices
	 */
	public Densify(double distanceTolerance) {
		this.distanceTolerance = distanceTolerance;
	}

	/**
	 * @param distanceTolerance  maximum distance between vertices
	 * @param resultPropertyName property name to use for geometry output
	 */
	public Densify(double distanceTolerance, String resultPropertyName) {
		super(resultPropertyName);
		this.distanceTolerance = distanceTolerance;
	}

	@Override
	protected GeoPipeFlow process(GeoPipeFlow flow) {
		setGeometry(flow, Densifier.densify(flow.getGeometry(), distanceTolerance));
		return flow;
	}

}
