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
import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Transaction;

/**
 * Simple encoder that stores point geometries as two x/y properties.
 */
public class SimplePointEncoder extends AbstractGeometryEncoder implements Configurable {

	public static final String DEFAULT_X = "longitude";
	public static final String DEFAULT_Y = "latitude";
	protected String xProperty = DEFAULT_X;
	protected String yProperty = DEFAULT_Y;

	@Override
	protected void encodeGeometryShape(Transaction tx, Geometry geometry, Entity container) {
		container.setProperty(
				PROP_TYPE,
				SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass()));
		Coordinate[] coords = geometry.getCoordinates();
		container.setProperty(xProperty, coords[0].x);
		container.setProperty(yProperty, coords[0].y);
	}

	@Override
	public Geometry decodeGeometry(Entity container) {
		double x = ((Number) container.getProperty(xProperty)).doubleValue();
		double y = ((Number) container.getProperty(yProperty)).doubleValue();
		Coordinate coordinate = new Coordinate(x, y);
		return getGeometryFactory().createPoint(coordinate);
	}

	@Override
	public String getConfiguration() {
		return xProperty + ":" + yProperty + ":" + bboxProperty;
	}

	@Override
	public void setConfiguration(String configuration) {
		if (configuration != null && !configuration.trim().isEmpty()) {
			String[] fields = configuration.split(":");
			if (fields.length > 0) {
				xProperty = fields[0];
			}
			if (fields.length > 1) {
				yProperty = fields[1];
			}
			if (fields.length > 2) {
				bboxProperty = fields[2];
			}
		}
	}

	@Override
	public String getSignature() {
		return "SimplePointEncoder(x='" + xProperty + "', y='" + yProperty + "', bbox='" + bboxProperty + "')";
	}
}
