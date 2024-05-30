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
package org.neo4j.gis.spatial.encoders.neo4j;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;

public class Neo4jGeometry implements org.neo4j.graphdb.spatial.Geometry {

	protected final String geometryType;
	protected final CRS crs;
	protected final List<Coordinate> coordinates;

	public Neo4jGeometry(String geometryType, List<org.neo4j.graphdb.spatial.Coordinate> coordinates, CRS crs) {
		this.geometryType = geometryType;
		this.coordinates = coordinates;
		this.crs = crs;
	}

	@Override
	public String getGeometryType() {
		return this.geometryType;
	}

	@Override
	public List<org.neo4j.graphdb.spatial.Coordinate> getCoordinates() {
		return this.coordinates;
	}

	@Override
	public CRS getCRS() {
		return this.crs;
	}

	public static String coordinateString(List<org.neo4j.graphdb.spatial.Coordinate> coordinates) {
		return coordinates.stream()
				.map(c -> Arrays.stream(c.getCoordinate()).mapToObj(Double::toString).collect(Collectors.joining(", ")))
				.collect(Collectors.joining(", "));
	}

	@Override
	public String toString() {
		return geometryType + "(" + coordinateString(coordinates) + ")[" + crs + "]";
	}
}
