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
import org.locationtech.jts.geom.GeometryFactory;
import org.neo4j.gis.spatial.pipes.AbstractFilterGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;


/**
 * Find geometries that intersects the given rectangle.
 */
public class FilterIntersectWindow extends AbstractFilterGeoPipe {

	private final Envelope envelope;
	private final Geometry envelopeGeom;

	public FilterIntersectWindow(GeometryFactory geomFactory, double xmin, double ymin, double xmax, double ymax) {
		this(geomFactory, new Envelope(xmin, xmax, ymin, ymax));
	}

	public FilterIntersectWindow(GeometryFactory geomFactory, Envelope envelope) {
		this.envelope = envelope;
		this.envelopeGeom = geomFactory.toGeometry(envelope);
	}

	@Override
	protected boolean validate(GeoPipeFlow flow) {
		return envelope.intersects(flow.getEnvelope())
				&& envelopeGeom.intersects(flow.getGeometry());
	}
}
