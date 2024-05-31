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
 * Simple encoder that stores geometries as an array of float values.
 * Only supports LineString geometries.
 */
// TODO: Consider generalizing this code and making a float[] type geometry store available in the library
// TODO: Consider switching from Float to Double according to Davide Savazzi
public class SimplePropertyEncoder extends AbstractGeometryEncoder {

	@Override
	protected void encodeGeometryShape(Transaction tx, Geometry geometry, Entity container) {
		container.setProperty(PROP_TYPE, SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass()));
		Coordinate[] coords = geometry.getCoordinates();
		float[] data = new float[coords.length * 2];
		for (int i = 0; i < coords.length; i++) {
			data[i * 2] = (float) coords[i].x;
			data[i * 2 + 1] = (float) coords[i].y;
		}

		container.setProperty("data", data);
	}

	@Override
	public Geometry decodeGeometry(Entity container) {
		float[] data = (float[]) container.getProperty("data");
		Coordinate[] coordinates = new Coordinate[data.length / 2];
		for (int i = 0; i < data.length / 2; i++) {
			coordinates[i] = new Coordinate(data[2 * i], data[2 * i + 1]);
		}
		return getGeometryFactory().createLineString(coordinates);
	}
}
