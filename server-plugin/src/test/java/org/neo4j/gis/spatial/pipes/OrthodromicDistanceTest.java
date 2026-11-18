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
package org.neo4j.gis.spatial.pipes;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.number.IsCloseTo.closeTo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.gis.spatial.pipes.processing.OrthodromicDistance;

public class OrthodromicDistanceTest {

	@Test
	public void shouldCalculateDistanceBetweenIdenticalPoints() {
		Coordinate pointA = new Coordinate(1.0, 1.0);
		Coordinate pointB = new Coordinate(1.0, 1.0);
		assertThat("Should be zero", OrthodromicDistance.calculateDistance(pointA, pointB), equalTo(0.0));
	}

	@Test
	public void shouldCalculateDistanceBetweenSamePoint() {
		Coordinate pointA = new Coordinate(1.0, 1.0);
		assertThat("Should be zero", OrthodromicDistance.calculateDistance(pointA, pointA), closeTo(0.0, 0.000001));
	}

	@Test
	public void shouldCalculateDistanceBetweenClosePoints() {
		Coordinate pointA = new Coordinate(1.0001, 1.0001);
		Coordinate pointB = new Coordinate(1.0, 1.0);
		assertThat("Should be zero", OrthodromicDistance.calculateDistance(pointA, pointB),
				closeTo(0.015724, 0.000001));
	}

	@Test
	public void shouldCalculateDistanceBetweenIdenticalPointsWithManyDecimalPlaces() {
		Coordinate pointA = new Coordinate(0.0905302, 52.2029252);
		Coordinate pointB = new Coordinate(0.0905302, 52.2029252);
		assertThat("Should be zero", OrthodromicDistance.calculateDistance(pointA, pointB), closeTo(0.0, 0.000001));
		assertThat("Should be zero", OrthodromicDistance.calculateDistance(pointB, pointA), closeTo(0.0, 0.000001));
	}

	@Test
	public void shouldCalculateDistanceBetweenClosePointsDifferingOnlyBySignificantDigits() {
		Coordinate pointA = new Coordinate(0.0905302, 52.2029252);
		Coordinate pointB = new Coordinate(0.0905302, 52.202925);
		assertThat("Should be zero", OrthodromicDistance.calculateDistance(pointA, pointB), closeTo(0.0, 0.000001));
		assertThat("Should be zero", OrthodromicDistance.calculateDistance(pointB, pointA), closeTo(0.0, 0.000001));
	}

	@Test
	public void shouldCalculateDistanceBetweenDistantPoints() {
		Coordinate pointA = new Coordinate(-80.0, 80.0);
		Coordinate pointB = new Coordinate(80.0, -80.0);
		assertThat("Should be a big number", OrthodromicDistance.calculateDistance(pointA, pointB),
				closeTo(19630.8, 0.1));
	}

	@Test
	public void shouldCalculateDistanceBetweenOppositePointsOnTheWorld() {
		Coordinate pointA = new Coordinate(-90.0, 0.0);
		Coordinate pointB = new Coordinate(90.0, 0.0);
		double halfEarthCircumference = Math.PI * OrthodromicDistance.earthRadiusInKm;
		assertThat("Should be half the earths circumference", OrthodromicDistance.calculateDistance(pointA, pointB),
				closeTo(halfEarthCircumference, 0.1));
	}

	@Test
	public void shouldCalculateDistanceBetweenIdenticalPointAroundTheWorld() {
		Coordinate pointA = new Coordinate(-180.0, 0.0);
		Coordinate pointB = new Coordinate(180.0, 0.0);
		assertThat("Should be zero", OrthodromicDistance.calculateDistance(pointA, pointB), closeTo(0.0, 0.000001));
	}

	@Test
	public void shouldCalculateDistanceToPolygon() {
		GeometryFactory factory = new GeometryFactory();
		Point reference = factory.createPoint(new Coordinate(0, 0));
		Polygon polygon = factory.createPolygon(new Coordinate[]{
				new Coordinate(1, -1),
				new Coordinate(1, 1),
				new Coordinate(2, 1),
				new Coordinate(2, -1),
				new Coordinate(1, -1)
		});
		assertThat("Should be positive number",
				OrthodromicDistance.calculateDistanceToGeometry(reference.getCoordinate(), polygon), closeTo(111, 1));
	}

	@Test
	public void shouldCalculateDistanceToEncompassingPolygon() {
		GeometryFactory factory = new GeometryFactory();
		Point reference = factory.createPoint(new Coordinate(0, 0));
		Polygon polygon = factory.createPolygon(new Coordinate[]{
				new Coordinate(1, -1),
				new Coordinate(1, 1),
				new Coordinate(-1, 1),
				new Coordinate(-1, -1),
				new Coordinate(1, -1)
		});
		assertThat("Should be zero",
				OrthodromicDistance.calculateDistanceToGeometry(reference.getCoordinate(), polygon),
				closeTo(0, 0.00001));
	}

	@ParameterizedTest
	@CsvSource(delimiter = ';', value = {
			// reference lon; lat; geometry; great-circle distance in km to its nearest point
			"0; 60; LINESTRING (1 60, 0 62); 53.85",
			"-179.9; 0; LINESTRING (179.95 0, 179.99 0); 12.23",
			"-179.9; 0; POLYGON ((179.95 -1, 179.99 -1, 179.99 1, 179.95 1, 179.95 -1)); 12.23",
			"0; 60; MULTILINESTRING ((10 10, 11 10), (1 60, 0 62)); 53.85",
			"0; 60; MULTIPOINT ((10 10), (0 61)); 111.19",
			"0.5; 60.5; POLYGON ((0 60, 1 60, 1 61, 0 61, 0 60)); 0",
			// edges are straight in lon/lat, as in the index and spatial.intersects
			"45; 60.5; POLYGON ((0 60, 90 60, 90 70, 0 70, 0 60)); 0",
			"45; 62; POLYGON ((0 50, 90 50, 90 60, 0 60, 0 50)); 222.39",
			"45; 67.79; LINESTRING (0 60, 90 60); 866.21",
			// nearest point lies in another basin than the nearest sample
			"-53.57; -67.07; LINESTRING (-180 -69.23, 180 71.06); 4315.81",
			"0.5; 0.5; GEOMETRYCOLLECTION (POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1)), POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))); 0",
			"0; 0; MULTILINESTRING (EMPTY, (1 1, 2 2)); 157.25",
			"0; 0; MULTIPOINT (EMPTY, (1 1)); 157.25"
	})
	public void shouldCalculateGreatCircleDistanceToTheNearestPointOfAGeometry(double lon, double lat, String wkt,
			double km) throws ParseException {
		Geometry geometry = new WKTReader().read(wkt);
		assertThat(OrthodromicDistance.calculateDistanceToGeometry(new Coordinate(lon, lat), geometry),
				closeTo(km, 0.01));
	}

	@ParameterizedTest
	@ValueSource(strings = {"POINT EMPTY", "LINESTRING EMPTY", "POLYGON EMPTY", "GEOMETRYCOLLECTION EMPTY"})
	public void shouldNotMeasureADistanceToAnEmptyGeometry(String wkt) throws ParseException {
		Geometry geometry = new WKTReader().read(wkt);
		assertThat(OrthodromicDistance.calculateDistanceToGeometry(new Coordinate(0, 0), geometry),
				is(Double.POSITIVE_INFINITY));
	}
}
