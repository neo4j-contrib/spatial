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

import org.locationtech.jts.geom.GeometryFactory;
import org.neo4j.gis.spatial.pipes.AbstractExtractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;

/**
 * Extracts every point from a geometry.
 */
public class ExtractPoints extends AbstractExtractGeoPipe {

	private final GeometryFactory geomFactory;

	public ExtractPoints(GeometryFactory geomFactory) {
		this.geomFactory = geomFactory;
	}

	@Override
	protected void extract(GeoPipeFlow pipeFlow) {
		int numPoints = pipeFlow.getGeometry().getCoordinates().length;
		for (int i = 0; i < numPoints; i++) {
			GeoPipeFlow newPoint = pipeFlow.makeClone("point" + i);
			newPoint.setGeometry(geomFactory.createPoint(pipeFlow.getGeometry().getCoordinates()[i]));
			extracts.add(newPoint);
		}
	}
}
