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
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
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
	@CsvSource({
			// lon, lat, km, target lon, target lat (each target lies within km of the reference)
			"0, 60, 1000, 18.1, 61.24",
			"0, -60, 1000, 18.1, -61.24",
			"179.9, 0, 50, -179.9, 0",
			"-179.9, 0, 50, 179.9, 0",
			"0, 89.9, 50, 180, 89.9",
			"0, -89.9, 50, 180, -89.9",
			"0, 0, 25000, 180, 0"
	})
	public void searchWindowCoversEveryPointWithinTheDistance(double lon, double lat, double km, double targetLon,
			double targetLat) {
		Coordinate reference = new Coordinate(lon, lat);
		Coordinate target = new Coordinate(targetLon, targetLat);
		assertThat(OrthodromicDistance.calculateDistance(reference, target), lessThanOrEqualTo(km));
		Envelope window = OrthodromicDistance.suggestSearchWindow(reference, km);
		assertTrue(window.covers(target), window + " should cover " + target);
		assertTrue(new Envelope(-180, 180, -90, 90).covers(window), window + " should stay within the world");
	}
}
