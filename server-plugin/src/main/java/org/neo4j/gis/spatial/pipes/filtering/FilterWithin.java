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

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.pipes.AbstractFilterGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;


/**
 * Find geometries that are within the given geometry.
 */
public class FilterWithin extends AbstractFilterGeoPipe {

	private final Geometry other;
	private final Envelope otherEnvelope;

	public FilterWithin(Geometry other) {
		this.other = other;
		this.otherEnvelope = other.getEnvelopeInternal();
	}

	@Override
	protected boolean validate(GeoPipeFlow flow) {
		// check if every point of this geometry is a point of the other geometry,
		// and the interiors of the two geometries have at least one point in common
		return otherEnvelope.contains(flow.getEnvelope())
				&& flow.getGeometry().within(other);
	}
}
