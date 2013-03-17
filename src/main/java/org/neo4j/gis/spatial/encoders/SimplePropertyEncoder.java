/**
 * Copyright (c) 2010-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.encoders;

import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.PropertyContainer;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Simple encoder that stores geometries as an array of float values.
 * Only supports LineString geometries.
 * 
 * @TODO: Consider generalizing this code and making a float[] type
 *        geometry store available in the library
 * @TODO: Consider switching from Float to Double according to Davide Savazzi
 * @author craig
 */
public class SimplePropertyEncoder extends AbstractGeometryEncoder {
	protected GeometryFactory geometryFactory;

	protected GeometryFactory getGeometryFactory() {
		if(geometryFactory==null) geometryFactory = new GeometryFactory();
		return geometryFactory;
	}

	@Override
	protected void encodeGeometryShape(Geometry geometry, PropertyContainer container) {
		container.setProperty("gtype", SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass()));
		Coordinate[] coords = geometry.getCoordinates();
		float[] data = new float[coords.length * 2];
		for (int i = 0; i < coords.length; i++) {
			data[i * 2 + 0] = (float) coords[i].x;
			data[i * 2 + 1] = (float) coords[i].y;
		}
		
		container.setProperty("data", data);
	}

	public Geometry decodeGeometry(PropertyContainer container) {
		float[] data = (float[]) container.getProperty("data");
		Coordinate[] coordinates = new Coordinate[data.length / 2];
		for (int i = 0; i < data.length / 2; i++) {
			coordinates[i] = new Coordinate(data[2 * i + 0], data[2 * i + 1]);
		}
		return getGeometryFactory().createLineString(coordinates);
	}
}