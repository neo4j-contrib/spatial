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
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.doc.domain.examples.ExampleCypher;
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
		docExample("spatial.asGeometry", "Creates a point geometry")
				.runCypher(
						"WITH point({latitude: 5.0, longitude: 4.0}) as geom RETURN spatial.asGeometry(geom) AS geometry",
						ExampleCypher::storeResult)
				.assertSingleResult("geometry", geometry -> {
					assertInstanceOf(Geometry.class, geometry, "Should be Geometry type");
				});
	}

	@Test
	public void create_point_geometry_ad_map() {
		docExample("spatial.asMap", "Creates a point geometry as map")
				.runCypher(
						"WITH point({latitude: 5.0, longitude: 4.0}) as geom RETURN spatial.asMap(geom) AS geometry",
						ExampleCypher::storeResult)
				.assertSingleResult("geometry", geometry -> {
					Assertions.assertThat(geometry)
							.asInstanceOf(InstanceOfAssertFactories.MAP)
							.containsExactly(
									Assertions.entry("type", "Point"),
									Assertions.entry("coordinate", new double[]{4.0d, 5.0d}));
				});
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
		docExample("spatial.asGeometry", "Creates a point geometry from a map")
				.runCypher("WITH spatial.asGeometry({latitude: 5.0, longitude: 4.0}) AS geometry RETURN geometry",
						ExampleCypher::storeResult)
				.assertSingleResult("geometry", geometry -> {
					assertInstanceOf(Geometry.class, geometry, "Should be Geometry type");
				});
	}

	/**
	 * Test for all WKT types
	 *
	 * @see <a href="https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry">Wikipedia WKT</a>
	 */
	@Nested
	class WktToGeoJson {

		@Test
		public void testPoint() {
			docExample("spatial.wktToGeoJson", "1. Converts a WKT POINT")
					.runCypher("RETURN spatial.wktToGeoJson('POINT (30 10)') as json", ExampleCypher::storeResult)
					.assertSingleResult("json", json -> {
						assertThat(json, equalTo(Map.of(
								"type", "Point",
								"coordinates", List.of(30., 10.)
						)));
					});
		}

		@Test
		public void testLineString() {
			docExample("spatial.wktToGeoJson", "2. Converts a WKT LINESTRING")
					.runCypher("RETURN spatial.wktToGeoJson('LINESTRING (30 10, 10 30, 40 40)') as json",
							ExampleCypher::storeResult)
					.assertSingleResult("json", json -> {
						assertThat(json, equalTo(Map.of(
								"type", "LineString",
								"coordinates", List.of(List.of(30., 10.), List.of(10., 30.), List.of(40., 40.))
						)));
					});
		}

		@Test
		public void testPolygon() {
			docExample("spatial.wktToGeoJson", "3. Converts a WKT POLYGON")
					.runCypher("RETURN spatial.wktToGeoJson('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') as json",
							ExampleCypher::storeResult)
					.assertSingleResult("json", json -> {
						assertThat(json, equalTo(Map.of(
								"type", "Polygon",
								"coordinates",
								List.of( // Polygon
										List.of( // LineString
												List.of(30., 10.),
												List.of(40., 40.),
												List.of(20., 40.),
												List.of(10., 20.),
												List.of(30., 10.)
										)
								)
						)));
					});
		}

		@Test
		public void testPolygonWithHole() {
			docExample("spatial.wktToGeoJson", "4. Converts a WKT POLYGON with a hole")
					.runCypher(
							"RETURN spatial.wktToGeoJson('POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))') as json",
							ExampleCypher::storeResult)
					.assertSingleResult("json", json -> {
						assertThat(json, equalTo(Map.of(
								"type", "Polygon",
								"coordinates",
								List.of( // Polygon
										List.of( // LineString
												List.of(35., 10.),
												List.of(45., 45.),
												List.of(15., 40.),
												List.of(10., 20.),
												List.of(35., 10.)
										),
										List.of( // hole
												List.of(20., 30.),
												List.of(35., 35.),
												List.of(30., 20.),
												List.of(20., 30.)
										)
								)
						)));
					});
		}

		@Test
		public void testMultiPoint() {
			docExample("spatial.wktToGeoJson", "5a. Converts a WKT MULTIPOINT")
					.runCypher("RETURN spatial.wktToGeoJson('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))') as json",
							ExampleCypher::storeResult)
					.assertSingleResult("json", json -> {
						assertThat(json, equalTo(Map.of(
								"type", "MultiPoint",
								"coordinates", List.of(
										List.of(10., 40.),
										List.of(40., 30.),
										List.of(20., 20.),
										List.of(30., 10.)
								))));
					});
		}

		@Test
		public void testMultiPoint2() {
			docExample("spatial.wktToGeoJson", "5b. Converts a WKT MULTIPOINT")
					.runCypher("RETURN spatial.wktToGeoJson('MULTIPOINT (10 40, 40 30, 20 20, 30 10)') as json",
							ExampleCypher::storeResult)
					.assertSingleResult("json", json -> {
						assertThat(json, equalTo(Map.of(
								"type", "MultiPoint",
								"coordinates", List.of(
										List.of(10., 40.),
										List.of(40., 30.),
										List.of(20., 20.),
										List.of(30., 10.)
								))));
					});
		}

		@Test
		public void testMultiLineString() {
			docExample("spatial.wktToGeoJson", "6. Converts a WKT MULTILINESTRING")
					.runCypher(
							"RETURN spatial.wktToGeoJson('MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))') as json",
							ExampleCypher::storeResult)
					.assertSingleResult("json", json -> {
						assertThat(json, equalTo(Map.of(
								"type", "MultiLineString",
								"coordinates", List.of(
										List.of( // LineString
												List.of(10., 10.),
												List.of(20., 20.),
												List.of(10., 40.)
										),
										List.of( // LineString
												List.of(40., 40.),
												List.of(30., 30.),
												List.of(40., 20.),
												List.of(30., 10.)
										)
								))));
					});
		}

		@Test
		public void testMultiPolygon() {
			docExample("spatial.wktToGeoJson", "7a. Converts a WKT MULTIPOLYGON")
					.runCypher(
							"RETURN spatial.wktToGeoJson('MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))') as json",
							ExampleCypher::storeResult)
					.assertSingleResult("json", json -> {
						assertThat(json, equalTo(Map.of(
								"type", "MultiPolygon",
								"coordinates", List.of( // MultiPolygon
										List.of( // Polygon
												List.of( // LineString
														List.of(30., 20.),
														List.of(45., 40.),
														List.of(10., 40.),
														List.of(30., 20.)
												)
										),
										List.of( // Polygon
												List.of( // LineString
														List.of(15., 5.),
														List.of(40., 10.),
														List.of(10., 20.),
														List.of(5., 10.),
														List.of(15., 5.)
												)
										)
								))));
					});
		}

		@Test
		public void testMultiPolygon2() {
			docExample("spatial.wktToGeoJson", "7b. Converts a WKT MULTIPOLYGON")
					.runCypher(
							"RETURN spatial.wktToGeoJson('MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))') as json",
							ExampleCypher::storeResult)
					.assertSingleResult("json", json -> {
						assertThat(json, equalTo(Map.of(
								"type", "MultiPolygon",
								"coordinates", List.of( // MultiPolygon
										List.of( // Polygon
												List.of( // LineString
														List.of(40., 40.),
														List.of(20., 45.),
														List.of(45., 30.),
														List.of(40., 40.)
												)
										),
										List.of( // Polygon
												List.of( // LineString
														List.of(20., 35.),
														List.of(10., 30.),
														List.of(10., 10.),
														List.of(30., 5.),
														List.of(45., 20.),
														List.of(20., 35.)
												),
												List.of( // hole
														List.of(30., 20.),
														List.of(20., 15.),
														List.of(20., 25.),
														List.of(30., 20.)
												)
										)
								))));
					});
		}

		@Test
		public void testGeometryCollection() {
			docExample("spatial.wktToGeoJson", "8. FConverts a WKT GEOMETRYCOLLECTION")
					.runCypher(
							"RETURN spatial.wktToGeoJson('GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))') as json",
							ExampleCypher::storeResult)
					.assertSingleResult("json", json -> {
						assertThat(json, equalTo(Map.of(
								"type", "GeometryCollection",
								"geometries", List.of(
										Map.of( // Point
												"type", "Point",
												"coordinates", List.of(40., 10.)
										),
										Map.of( // LineString
												"type", "LineString",
												"coordinates", List.of(
														List.of(10., 10.),
														List.of(20., 20.),
														List.of(10., 40.)
												)
										),
										Map.of( // Polygon
												"type", "Polygon",
												"coordinates", List.of(
														List.of( // LineString
																List.of(40., 40.),
																List.of(20., 45.),
																List.of(45., 30.),
																List.of(40., 40.)
														)
												)
										)
								)
						)));
					});
		}
	}

	@Test
	public void testPointToWkt() {
		docExample("spatial.neo4jGeometryToWkt", "Converting a point to WKT")
				.runCypher("RETURN spatial.neo4jGeometryToWkt(point({longitude: 1, latitude: 2})) as wkt",
						ExampleCypher::storeResult)
				.assertSingleResult("wkt", wkt -> {
					assertThat(wkt, equalTo("POINT ( 1 2 )"));
				});
	}

	@Test
	public void testPointArrayToWkt() {
		docExample("spatial.neo4jGeometryToWkt", "Converting a point array to WKT")
				.runCypher(
						"RETURN spatial.neo4jGeometryToWkt([point({longitude: 1, latitude: 2}), point({longitude: 3, latitude: 4}) ]) as wkt",
						ExampleCypher::storeResult)
				.assertSingleResult("wkt", wkt -> {
					assertThat(wkt, equalTo("LINESTRING (1 2, 3 4)"));
				});
	}
}
