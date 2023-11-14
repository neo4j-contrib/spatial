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

package org.neo4j.gis.spatial.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.List;
import java.util.Map;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.spatial.AbstractApiTest;
import org.neo4j.graphdb.spatial.Geometry;

public class SpatialFunctionsTest extends AbstractApiTest {

	@Override
	protected void registerApiProceduresAndFunctions() throws KernelException {
		registerProceduresAndFunctions(SpatialFunctions.class);
	}

	@Test
	public void create_point_and_pass_as_param() {
		Geometry geom = (Geometry) executeObject("RETURN point({latitude: 5.0, longitude: 4.0}) as geometry",
				"geometry");
		double distance = (Double) executeObject(
				"WITH spatial.asGeometry($geom) AS geometry RETURN point.distance(geometry, point({latitude: 5.1, longitude: 4.0})) as distance",
				Map.of("geom", geom), "distance");
		MatcherAssert.assertThat("Expected the geographic distance of 11132km", distance, closeTo(11132.0, 1.0));
	}

	@Test
	public void create_point_geometry_return() {
		Object geometry = executeObject(
				"WITH point({latitude: 5.0, longitude: 4.0}) as geom RETURN spatial.asGeometry(geom) AS geometry",
				"geometry");
		assertInstanceOf(Geometry.class, geometry, "Should be Geometry type");
	}

	@Test
	public void create_point_geometry_and_distance() {
		double distance = (double) executeObject(
				"WITH point({latitude: 5.0, longitude: 4.0}) as geom WITH spatial.asGeometry(geom) AS geometry RETURN point.distance(geometry, point({latitude: 5.0, longitude: 4.0})) as distance",
				"distance");
		System.out.println(distance);
	}

	@Test
	public void literal_geometry_return() {
		Object geometry = executeObject(
				"WITH spatial.asGeometry({latitude: 5.0, longitude: 4.0}) AS geometry RETURN geometry", "geometry");
		assertInstanceOf(Geometry.class, geometry, "Should be Geometry type");
	}

	@Test
	public void wktToGeoJson() {
		String wkt = "MULTIPOLYGON(((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2)))";
		Object json = executeObject("return spatial.convert.wktToGeoJson($wkt) as json", Map.of("wkt", wkt), "json");
		assertThat(json, equalTo(Map.of(
				"type", "MultiPolygon",
				"coordinates", List.of( // MultiPolygon
						List.of( // Polygon
								List.of( // LineString
										List.of(15.3, 60.2),
										List.of(15.3, 60.4),
										List.of(15.7, 60.4),
										List.of(15.7, 60.2),
										List.of(15.3, 60.2)
								)
						)
				)
		)));
	}
}
