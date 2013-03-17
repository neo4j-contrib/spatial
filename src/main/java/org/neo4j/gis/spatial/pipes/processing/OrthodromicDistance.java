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
package org.neo4j.gis.spatial.pipes.processing;

import org.neo4j.gis.spatial.pipes.AbstractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;


/**
 * Calculates distance between the given geometry and item geometry for each item in the pipeline.
 * This pipe assume Layer contains geometries with Latitude / Longitude coordinates in degrees.
 * 
 * Algorithm reference: http://www.movable-type.co.uk/scripts/latlong-db.html
 */
public class OrthodromicDistance extends AbstractGeoPipe {

	private Coordinate reference;
	public static final double earthRadiusInKm = 6371;	
	
	public OrthodromicDistance(Coordinate reference) {
		this.reference = reference;
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
		// TODO check Geometry is a point? use Centroid?
		Coordinate point = flow.getGeometry().getCoordinate();

		// d = acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1)) * R
		double distanceInKm = calculateDistance(reference, point);
		
		setProperty(flow, distanceInKm);
		return flow;
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
		double distanceInKm = Math.acos(Math.sin(Math.toRadians(reference.y)) * Math.sin(Math.toRadians(point.y))
				+ Math.cos(Math.toRadians(reference.y)) * Math.cos(Math.toRadians(point.y))
				* Math.cos(Math.toRadians(point.x) - Math.toRadians(reference.x)))
				* earthRadiusInKm;
		return distanceInKm;
	}
}