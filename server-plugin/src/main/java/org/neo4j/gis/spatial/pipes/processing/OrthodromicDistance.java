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
package org.neo4j.gis.spatial.pipes.processing;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.util.LinearComponentExtracter;
import org.locationtech.jts.geom.util.PointExtracter;
import org.neo4j.gis.spatial.pipes.AbstractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;


/**
 * Calculates distance between the given geometry and item geometry for each item in the pipeline.
 * This pipe assume Layer contains geometries with Latitude / Longitude coordinates in degrees.
 * <p>
 * <a href="https://www.movable-type.co.uk/scripts/latlong-db.html">Algorithm reference</a>
 */
public class OrthodromicDistance extends AbstractGeoPipe {

	private final Coordinate reference;
	public static final double earthRadiusInKm = 6371;
	public static final String DISTANCE = "OrthodromicDistance";

	public OrthodromicDistance(Coordinate reference) {
		this(reference, OrthodromicDistance.DISTANCE);
	}

	/**
	 * @param resultPropertyName property name to use for geometry output
	 */
	public OrthodromicDistance(Coordinate reference, String resultPropertyName) {
		super(resultPropertyName);
		this.reference = reference;
	}

	@Override
	protected GeoPipeFlow process(GeoPipeFlow flow) {
		double distanceInKm = calculateDistanceToGeometry(reference, flow.getGeometry());
		setProperty(flow, distanceInKm);
		return flow;
	}

	/**
	 * @return the great-circle distance in km from the reference to the nearest point of the geometry, taking the
	 * edges between its vertices as great-circle arcs; 0 if an areal geometry contains the reference
	 */
	public static double calculateDistanceToGeometry(Coordinate reference, Geometry geometry) {
		if (geometry instanceof Point point) {
			return calculateDistance(reference, point.getCoordinate());
		}
		if (geometry.getDimension() == 2 && geometry.covers(geometry.getFactory().createPoint(reference))) {
			return 0;
		}
		double[] p = toUnitVector(reference);
		double minAngle = Math.PI;
		for (Object point : PointExtracter.getPoints(geometry)) {
			minAngle = Math.min(minAngle, angle(p, toUnitVector(((Point) point).getCoordinate())));
		}
		for (Object line : LinearComponentExtracter.getLines(geometry)) {
			Coordinate[] coordinates = ((LineString) line).getCoordinates();
			for (int i = 1; i < coordinates.length; i++) {
				minAngle = Math.min(minAngle,
						angleToArc(p, toUnitVector(coordinates[i - 1]), toUnitVector(coordinates[i])));
			}
		}
		return minAngle * earthRadiusInKm;
	}

	// angle between p and the shorter great-circle arc from a to b
	private static double angleToArc(double[] p, double[] a, double[] b) {
		double[] normal = cross(a, b);
		double normalLength = Math.sqrt(dot(normal, normal));
		if (normalLength > 1e-15) {
			// the arc's closest point to p is p projected onto the arc's great circle, if it lies between a and b
			double[] n = scale(normal, 1 / normalLength);
			if (dot(cross(a, p), n) >= 0 && dot(cross(p, b), n) >= 0) {
				return Math.abs(Math.asin(Math.max(-1, Math.min(1, dot(p, n)))));
			}
		}
		return Math.min(angle(p, a), angle(p, b));
	}

	private static double[] toUnitVector(Coordinate c) {
		double lon = Math.toRadians(c.x);
		double lat = Math.toRadians(c.y);
		return new double[]{Math.cos(lat) * Math.cos(lon), Math.cos(lat) * Math.sin(lon), Math.sin(lat)};
	}

	private static double angle(double[] a, double[] b) {
		double[] c = cross(a, b);
		return Math.atan2(Math.sqrt(dot(c, c)), dot(a, b));
	}

	private static double[] cross(double[] a, double[] b) {
		return new double[]{a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]};
	}

	private static double dot(double[] a, double[] b) {
		return a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
	}

	private static double[] scale(double[] a, double factor) {
		return new double[]{a[0] * factor, a[1] * factor, a[2] * factor};
	}

	public static Envelope suggestSearchWindow(Coordinate reference, double maxDistanceInKm) {
		double lat = reference.y;
		double lon = reference.x;

		double degrees = Math.toDegrees(maxDistanceInKm / earthRadiusInKm);

		// first-cut bounding box (in degrees)
		double maxLat = lat + degrees;
		double minLat = lat - degrees;

		degrees = Math.toDegrees(maxDistanceInKm / earthRadiusInKm / Math.cos(Math.toRadians(lat)));
		// compensate for degrees longitude getting smaller with increasing latitude
		double maxLon = lon + degrees;
		double minLon = lon - degrees;
		return new Envelope(minLon, maxLon, minLat, maxLat);
	}

	public static double calculateDistance(Coordinate reference, Coordinate point) {
		// TODO use org.geotools.referencing.GeodeticCalculator?
		// d = acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1)) * R
		return Math.acos(Math.min(Math.sin(Math.toRadians(reference.y)) * Math.sin(Math.toRadians(point.y))
				+ Math.cos(Math.toRadians(reference.y)) * Math.cos(Math.toRadians(point.y))
				* Math.cos(Math.toRadians(point.x) - Math.toRadians(reference.x)), 1.0))
				* earthRadiusInKm;
	}
}
