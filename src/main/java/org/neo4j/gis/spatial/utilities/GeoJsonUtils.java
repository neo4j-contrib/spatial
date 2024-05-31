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

package org.neo4j.gis.spatial.utilities;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;


public class GeoJsonUtils {

	private GeoJsonUtils() {
	}

	public static Map<String, Object> toGeoJsonStructure(Geometry geometry) {
		if (geometry instanceof GeometryCollection geometryCollection && "GeometryCollection".equals(
				geometry.getGeometryType())) {
			return Map.of(
					"type", geometry.getGeometryType(),
					"geometries", IntStream.range(0, geometryCollection.getNumGeometries())
							.mapToObj(geometryCollection::getGeometryN)
							.map(GeoJsonUtils::toGeoJsonStructure)
							.toList()
			);
		}
		return Map.of(
				"type", geometry.getGeometryType(),
				"coordinates", getCoordinates(geometry)
		);
	}

	private static List<?> getCoordinates(Geometry geometry) {
		if (geometry instanceof Point point) {
			return getPoint(point.getCoordinate());
		}
		if (geometry instanceof LineString lineString) {
			return Arrays.stream(lineString.getCoordinates()).map(GeoJsonUtils::getPoint).toList();
		}
		if (geometry instanceof Polygon polygon) {
			return Stream.concat(
							Stream.of(polygon.getExteriorRing()),
							IntStream.range(0, polygon.getNumInteriorRing())
									.mapToObj(polygon::getInteriorRingN)
					)
					.map(GeoJsonUtils::getCoordinates)
					.toList();
		}
		if (geometry instanceof GeometryCollection geometryCollection) {
			return IntStream.range(0, geometryCollection.getNumGeometries())
					.mapToObj(geometryCollection::getGeometryN)
					.map(GeoJsonUtils::getCoordinates)
					.toList();
		}
		throw new IllegalArgumentException("Unsupported geometry type: " + geometry.getGeometryType());
	}

	private static List<Object> getPoint(Coordinate coordinate) {
		if (Double.isNaN(coordinate.getZ())) {
			return List.of(coordinate.getX(), coordinate.getY());
		}
		return List.of(
				coordinate.getX(),
				coordinate.getY(),
				coordinate.getZ());
	}
}
