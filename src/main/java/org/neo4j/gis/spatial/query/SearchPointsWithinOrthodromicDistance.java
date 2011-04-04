/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.gis.spatial.query;

import org.neo4j.gis.spatial.AbstractSearch;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

/**
 * This search assume Layer contains Points with Latitude / Longitude
 * coordinates in degrees. Algorithm reference:
 * http://www.movable-type.co.uk/scripts/latlong-db.html
 * 
 * @author Davide Savazzi
 */
public class SearchPointsWithinOrthodromicDistance extends AbstractSearch {

	public SearchPointsWithinOrthodromicDistance(Coordinate reference, double maxDistanceInKm, boolean saveDistanceOnGeometry) {
		this.reference = reference;
		this.maxDistanceInKm = maxDistanceInKm;
		this.saveDistanceOnGeometry = saveDistanceOnGeometry;

		double lat = reference.y;
		double lon = reference.x;

		// first-cut bounding box (in degrees)
		double maxLat = lat + Math.toDegrees(maxDistanceInKm / earthRadiusInKm);
		double minLat = lat - Math.toDegrees(maxDistanceInKm / earthRadiusInKm);
		// compensate for degrees longitude getting smaller with increasing
		// latitude
		double maxLon = lon + Math.toDegrees(maxDistanceInKm / earthRadiusInKm / Math.cos(Math.toRadians(lat)));
		double minLon = lon - Math.toDegrees(maxDistanceInKm / earthRadiusInKm / Math.cos(Math.toRadians(lat)));
		this.bbox = new Envelope(minLon, maxLon, minLat, maxLat);
	}

	public SearchPointsWithinOrthodromicDistance(Coordinate reference, Envelope bbox, boolean saveDistanceOnGeometry) {
		this.reference = reference;
		this.bbox = bbox;
		this.maxDistanceInKm = calculateDistance(bbox.centre(), new Coordinate(bbox.getMinX(),
				(bbox.getMinY() + bbox.getMaxY()) / 2));
		this.saveDistanceOnGeometry = saveDistanceOnGeometry;
	}

	public boolean needsToVisit(Envelope indexNodeEnvelope) {
		return indexNodeEnvelope.intersects(bbox);
	}

	public void onIndexReference(Node geomNode) {
		Geometry geometry = decode(geomNode);
		Coordinate point = geometry.getCoordinate();

		// d = acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 -
		// lon1)) * R
		double distanceInKm = calculateDistance(reference, point);

		if (distanceInKm < maxDistanceInKm) {
			if (saveDistanceOnGeometry) {
				add(geomNode, geometry, "distanceInKm", distanceInKm);
			} else {
				add(geomNode, geometry);
			}
		}
	}

	public static double calculateDistance(Coordinate reference, Coordinate point) {
		double distanceInKm = Math.acos(Math.sin(Math.toRadians(reference.y)) * Math.sin(Math.toRadians(point.y))
				+ Math.cos(Math.toRadians(reference.y)) * Math.cos(Math.toRadians(point.y))
				* Math.cos(Math.toRadians(point.x) - Math.toRadians(reference.x)))
				* earthRadiusInKm;
		return distanceInKm;
	}

	private Coordinate reference;
	private double maxDistanceInKm;
	private Envelope bbox;
	private boolean saveDistanceOnGeometry;

	private static final double earthRadiusInKm = 6371;
}