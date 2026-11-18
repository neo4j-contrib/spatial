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

import org.locationtech.jts.algorithm.locate.SimplePointInAreaLocator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateFilter;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Location;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.util.LinearComponentExtracter;
import org.locationtech.jts.geom.util.PointExtracter;
import org.locationtech.jts.operation.distance.DistanceOp;
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
	 * edges between its vertices as straight lines in lon/lat (as the index and the topology operations do); 0 if an
	 * areal geometry contains the reference; infinite if the geometry is empty
	 */
	public static double calculateDistanceToGeometry(Coordinate reference, Geometry geometry) {
		if (geometry.isEmpty()) {
			return Double.POSITIVE_INFINITY;
		}
		if (geometry instanceof Point point) {
			return calculateDistance(reference, point.getCoordinate());
		}
		// unlike covers(), needs no topology, so overlapping polygons in a collection don't throw
		if (geometry.getDimension() == 2
				&& SimplePointInAreaLocator.locate(reference, geometry) != Location.EXTERIOR) {
			return 0;
		}
		double best = upperBound(reference, geometry);
		for (Object point : PointExtracter.getPoints(geometry)) {
			if (!((Point) point).isEmpty()) {
				best = Math.min(best, haversine(reference, ((Point) point).getCoordinate()));
			}
		}
		for (Object line : LinearComponentExtracter.getLines(geometry)) {
			CoordinateSequence coordinates = ((LineString) line).getCoordinateSequence();
			if (coordinates.size() == 0) {
				continue;
			}
			Coordinate a = coordinates.getCoordinate(0);
			double distanceToA = haversine(reference, a);
			best = Math.min(best, distanceToA);
			for (int i = 1; i < coordinates.size(); i++) {
				Coordinate b = coordinates.getCoordinate(i);
				double distanceToB = haversine(reference, b);
				best = Math.min(best, distanceToB);
				// every point of the edge is within half its length of an endpoint
				double length = maxEdgeLength(a, b);
				if (Math.min(distanceToA, distanceToB) - length / 2 < best) {
					best = distanceToEdge(reference, a, b, length, best);
				}
				a = b;
				distanceToA = distanceToB;
			}
		}
		return best;
	}

	// exact distance to a near-optimal point, found in a plane scaled by cos(lat) at the reference; lets the edge loop
	// prune from the start
	private static double upperBound(Coordinate reference, Geometry geometry) {
		double scale = Math.cos(Math.toRadians(reference.y));
		Geometry scaled = geometry.copy();
		scaled.apply((CoordinateFilter) c -> c.x *= scale);
		scaled.geometryChanged();
		Coordinate nearest = DistanceOp.nearestPoints(scaled,
				geometry.getFactory().createPoint(new Coordinate(reference.x * scale, reference.y)))[0];
		return haversine(reference, new Coordinate(nearest.x / scale, nearest.y));
	}

	private static final double KM_PER_DEGREE = Math.toRadians(earthRadiusInKm);
	private static final double GOLDEN = (Math.sqrt(5) - 1) / 2;

	// upper bound of the length of the straight lon/lat edge: a degree of longitude is longest at the lowest latitude
	private static double maxEdgeLength(Coordinate a, Coordinate b) {
		double lowestAbsLat = Math.signum(a.y) == Math.signum(b.y) ? Math.min(Math.abs(a.y), Math.abs(b.y)) : 0;
		return KM_PER_DEGREE * Math.hypot(b.y - a.y, (b.x - a.x) * Math.cos(Math.toRadians(lowestAbsLat)));
	}

	// distance to the nearest point of the edge, if below best; samples first, since a long edge need not be unimodal
	private static double distanceToEdge(Coordinate reference, Coordinate a, Coordinate b, double length,
			double best) {
		int samples = (int) Math.min(64, 2 + length / 100);
		double[] distances = new double[samples + 1];
		for (int i = 0; i <= samples; i++) {
			distances[i] = distanceAlongEdge(reference, a, b, (double) i / samples);
			best = Math.min(best, distances[i]);
		}
		// refine every local minimum that can still beat best: the lowest sample need not lie in the deepest basin
		double halfStep = length / samples / 2;
		for (int i = 0; i <= samples; i++) {
			boolean localMinimum = (i == 0 || distances[i - 1] > distances[i])
					&& (i == samples || distances[i + 1] >= distances[i]);
			if (localMinimum && distances[i] - halfStep < best) {
				best = Math.min(best, goldenSection(reference, a, b, length,
						Math.max(0, i - 1) / (double) samples, Math.min(samples, i + 1) / (double) samples));
			}
		}
		return best;
	}

	private static double goldenSection(Coordinate reference, Coordinate a, Coordinate b, double length, double lo,
			double hi) {
		double x1 = hi - GOLDEN * (hi - lo);
		double x2 = lo + GOLDEN * (hi - lo);
		double f1 = distanceAlongEdge(reference, a, b, x1);
		double f2 = distanceAlongEdge(reference, a, b, x2);
		// 1 m along the edge is finer than the distance resolution anyone filters on
		while ((hi - lo) * length > 0.001) {
			if (f1 < f2) {
				hi = x2;
				x2 = x1;
				f2 = f1;
				x1 = hi - GOLDEN * (hi - lo);
				f1 = distanceAlongEdge(reference, a, b, x1);
			} else {
				lo = x1;
				x1 = x2;
				f1 = f2;
				x2 = lo + GOLDEN * (hi - lo);
				f2 = distanceAlongEdge(reference, a, b, x2);
			}
		}
		return Math.min(f1, f2);
	}

	private static double distanceAlongEdge(Coordinate reference, Coordinate a, Coordinate b, double t) {
		return haversine(reference, new Coordinate(a.x + (b.x - a.x) * t, a.y + (b.y - a.y) * t));
	}

	// unlike the acos formula in calculateDistance, accurate down to millimetres
	private static double haversine(Coordinate reference, Coordinate point) {
		double lat1 = Math.toRadians(reference.y);
		double lat2 = Math.toRadians(point.y);
		double sinDLat = Math.sin((lat2 - lat1) / 2);
		double sinDLon = Math.sin(Math.toRadians(point.x - reference.x) / 2);
		double h = sinDLat * sinDLat + Math.cos(lat1) * Math.cos(lat2) * sinDLon * sinDLon;
		return 2 * earthRadiusInKm * Math.asin(Math.min(1, Math.sqrt(h)));
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
