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

import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.pipes.AbstractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;

/**
 * Computes a geometry representing the points making up item geometry that do not make up the given geometry.
 * Item geometry is replaced by pipe output unless an alternative property name is given in the constructor.
 */
public class Difference extends AbstractGeoPipe {

	private final Geometry other;

	public Difference(Geometry other) {
		this.other = other;
	}

	/**
	 * @param other              geometry
	 * @param resultPropertyName property name to use for geometry output
	 */
	public Difference(Geometry other, String resultPropertyName) {
		super(resultPropertyName);
		this.other = other;
	}

	@Override
	protected GeoPipeFlow process(GeoPipeFlow flow) {
		setGeometry(flow, flow.getGeometry().difference(other));
		return flow;
	}
}
