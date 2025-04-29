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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jCRS;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jPoint;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Transaction;

/**
 * Simple encoder that stores point geometries as one Neo4j Point property.
 */
public class NativePointEncoder extends AbstractGeometryEncoder implements Configurable {

	private static final String DEFAULT_GEOM = "location";
	private String locationProperty = DEFAULT_GEOM;
	private Neo4jCRS crs = Neo4jCRS.findCRS("WGS-84");

	@Override
	protected void encodeGeometryShape(Transaction tx, Geometry geometry, Entity container) {
		int gtype = SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass());
		if (gtype == GTYPE_POINT) {
			container.setProperty(PROP_TYPE, gtype);
			Neo4jPoint neo4jPoint = new Neo4jPoint((Point) geometry, crs);
			container.setProperty(locationProperty, neo4jPoint);
		} else {
			throw new IllegalArgumentException("Cannot store non-Point types as Native Neo4j properties: "
					+ SpatialDatabaseService.convertGeometryTypeToName(gtype));
		}

	}

	@Override
	public Geometry decodeGeometry(Entity container) {
		org.neo4j.graphdb.spatial.Point point = ((org.neo4j.graphdb.spatial.Point) container.getProperty(
				locationProperty));
		if (point.getCRS().getCode() != crs.getCode()) {
			throw new IllegalStateException("Trying to decode geometry with wrong CRS: layer configured to crs=" + crs
					+ ", but geometry has crs=" + point.getCRS().getCode());
		}
		double[] coordinate = point.getCoordinate().getCoordinate();
		if (crs.dimensions() == 3) {
			return getGeometryFactory().createPoint(new Coordinate(coordinate[0], coordinate[1], coordinate[2]));
		}
		return getGeometryFactory().createPoint(new Coordinate(coordinate[0], coordinate[1]));
	}

	@Override
	public String getConfiguration() {
		return locationProperty + ":" + bboxProperty + ": " + crs.getCode();
	}

	@Override
	public void setConfiguration(String configuration) {
		if (configuration != null && !configuration.trim().isEmpty()) {
			String[] fields = configuration.split(":");
			if (fields.length > 0) {
				locationProperty = fields[0];
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
		return "NativePointEncoder(geometry='" + locationProperty + "', bbox='" + bboxProperty + "', crs="
				+ crs.getCode() + ")";
	}
}
