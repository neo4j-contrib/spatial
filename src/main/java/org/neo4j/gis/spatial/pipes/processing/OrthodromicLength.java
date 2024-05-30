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

import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.referencing.GeodeticCalculator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.pipes.AbstractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;


/**
 * Calculates geometry length for each item in the pipeline.
 * This pipe assume Layer contains geometries with Latitude / Longitude coordinates in degrees.
 */
public class OrthodromicLength extends AbstractGeoPipe {

	protected final CoordinateReferenceSystem crs;

	public OrthodromicLength(CoordinateReferenceSystem crs) {
		this.crs = crs;
	}

	/**
	 * @param resultPropertyName property name to use for geometry output
	 */
	public OrthodromicLength(CoordinateReferenceSystem crs, String resultPropertyName) {
		super(resultPropertyName);
		this.crs = crs;
	}

	@Override
	protected GeoPipeFlow process(GeoPipeFlow flow) {
		setProperty(flow, calculateLength(flow.getGeometry(), crs));
		return flow;
	}

	protected static double calculateLength(Geometry geometry, CoordinateReferenceSystem crs) {
		GeodeticCalculator geodeticCalculator = new GeodeticCalculator(crs);

		Coordinate[] coords = geometry.getCoordinates();

		double totalLength = 0;

		// accumulate the orthodromic distance for every point relation of the given geometry.
		for (int i = 0; i < (coords.length - 1); i++) {
			Coordinate c1 = coords[i];
			Coordinate c2 = coords[i + 1];
			geodeticCalculator.setStartingGeographicPoint(c1.x, c1.y);
			geodeticCalculator.setDestinationGeographicPoint(c2.x, c2.y);
			totalLength += geodeticCalculator.getOrthodromicDistance();
		}

		return totalLength;
	}

}
