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
package org.neo4j.gis.spatial.pipes.filtering;

import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.pipes.AbstractFilterGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;


/**
 * Find geometries equal to the given geometry.<br>
 * <br>
 * This filter tests for topological equality which is equivalent to drawing the two Geometry objects
 * and seeing if all of their component edges overlap. It is the most robust kind of comparison but also
 * the most computationally expensive.<br>
 * <br>
 * See <a href="http://docs.geotools.org/latest/userguide/library/jts/equals.html">GeoTools documentation</a>.
 */
public class FilterEqualTopo extends AbstractFilterGeoPipe {

	private final Geometry other;

	public FilterEqualTopo(Geometry other) {
		this.other = other;
	}

	@Override
	protected boolean validate(GeoPipeFlow flow) {
		return other.equalsTopo(flow.getGeometry());
	}
}
