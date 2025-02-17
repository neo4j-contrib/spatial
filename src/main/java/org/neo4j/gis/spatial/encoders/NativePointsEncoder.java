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
package org.neo4j.gis.spatial.encoders;

import java.util.Arrays;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jCRS;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jPoint;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Transaction;

/**
 * Simple encoder that stores line strings as an array of points.
 */
public class NativePointsEncoder extends AbstractGeometryEncoder implements Configurable {

	private String property = "geometry";
	private Neo4jCRS crs = Neo4jCRS.findCRS("WGS-84");

	@Override
	protected void encodeGeometryShape(Transaction tx, Geometry geometry, Entity container) {
		var gtype = SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass());
		if (geometry instanceof LineString lineString) {
			var neo4jPoints = Arrays.stream(lineString.getCoordinates())
					.map(coordinate -> new Neo4jPoint(coordinate, crs))
					.toArray(org.neo4j.graphdb.spatial.Point[]::new);
			container.setProperty(PROP_TYPE, gtype);
			container.setProperty(property, neo4jPoints);
		} else {
			throw new IllegalArgumentException(
					"Can only store point-arrays as linestring: " + SpatialDatabaseService.convertGeometryTypeToName(
							gtype));
		}

	}

	@Override
	public Geometry decodeGeometry(Entity container) {
		var points = ((org.neo4j.graphdb.spatial.Point[]) container.getProperty(property));
		var factory = getGeometryFactory();
		var coordinates = Arrays.stream(points)
				.map(point -> {
					if (point.getCRS().getCode() != crs.getCode()) {
						throw new IllegalStateException(
								"Trying to decode geometry with wrong CRS: layer configured to crs=" + crs.getCode()
										+ ", but geometry has crs=" + point.getCRS().getCode());
					}
					double[] coordinate = point.getCoordinate().getCoordinate();
					if (crs.dimensions() == 3) {
						return new Coordinate(coordinate[0], coordinate[1], coordinate[2]);
					} else {
						return new Coordinate(coordinate[0], coordinate[1]);
					}
				})
				.toArray(Coordinate[]::new);
		return factory.createLineString(coordinates);
	}

	@Override
	public String getConfiguration() {
		return property + ":" + bboxProperty + ": " + crs.getCode();
	}

	@Override
	public void setConfiguration(String configuration) {
		if (configuration != null && !configuration.trim().isEmpty()) {
			String[] fields = configuration.split(":");
			if (fields.length > 0) {
				property = fields[0];
			}
			if (fields.length > 1) {
				bboxProperty = fields[1];
			}
			if (fields.length > 2) {
				crs = Neo4jCRS.findCRS(fields[2]);
			}
		}
	}

	@Override
	public String getSignature() {
		return "NativePointEncoder(geometry='" + property + "', bbox='" + bboxProperty + "', crs=" + crs.getCode()
				+ ")";
	}
}
