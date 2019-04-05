/**
 * Copyright (c) 2010-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.operation.distance.DistanceOp;
import org.neo4j.gis.spatial.pipes.AbstractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;


/**
 * Calculates distance between the given geometry and item geometry for each item in the pipeline.
 * This pipe assume Layer contains geometries with Latitude / Longitude coordinates in degrees.
 *
 * Algorithm reference: http://www.movable-type.co.uk/scripts/latlong-db.html
 */
public class OrthodromicDistance extends AbstractGeoPipe {

	private Coordinate reference;
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

    public static double calculateDistanceToGeometry(Coordinate reference, Geometry geometry) {
        if (geometry instanceof Point) {
            Point point = (Point) geometry;
            return calculateDistance(reference, point.getCoordinate());
        } else {
            Geometry referencePoint = geometry.getFactory().createPoint(reference);
            DistanceOp ops = new DistanceOp(referencePoint, geometry);
            Coordinate[] nearest = ops.nearestPoints();
            assert nearest.length == 2;
            return calculateDistance(nearest[0], nearest[1]);
        }
    }

	public static Envelope suggestSearchWindow(Coordinate reference, double maxDistanceInKm) {
		double lat = reference.y;
		double lon = reference.x;

		// first-cut bounding box (in degrees)
		double maxLat = lat + Math.toDegrees(maxDistanceInKm / earthRadiusInKm);
		double minLat = lat - Math.toDegrees(maxDistanceInKm / earthRadiusInKm);
		// compensate for degrees longitude getting smaller with increasing latitude
		double maxLon = lon + Math.toDegrees(maxDistanceInKm / earthRadiusInKm / Math.cos(Math.toRadians(lat)));
		double minLon = lon - Math.toDegrees(maxDistanceInKm / earthRadiusInKm / Math.cos(Math.toRadians(lat)));
		return new Envelope(minLon, maxLon, minLat, maxLat);
	}

	public static double calculateDistance(Coordinate reference, Coordinate point) {
		// TODO use org.geotools.referencing.GeodeticCalculator?

		// d = acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1)) * R
		double distanceInKm = Math.acos(Math.min(Math.sin(Math.toRadians(reference.y)) * Math.sin(Math.toRadians(point.y))
				+ Math.cos(Math.toRadians(reference.y)) * Math.cos(Math.toRadians(point.y))
				* Math.cos(Math.toRadians(point.x) - Math.toRadians(reference.x)),1.0))
				* earthRadiusInKm;
		return distanceInKm;
	}
}
