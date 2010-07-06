/*
 * Copyright (c) 2010
 *
 * This program is free software: you can redistribute it and/or modify
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

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;


/**
 * This search assume Layer contains Points with Latitude / Longitude coordinates in degrees.
 * Algorithm reference: http://www.movable-type.co.uk/scripts/latlong-db.html
 * 
 * @author Davide Savazzi
 */
public class SearchPointsWithinOrthodromicDistance extends AbstractSearch {

	public SearchPointsWithinOrthodromicDistance(Point refPoint, double maxDistanceInKm) {
		this.refPoint = refPoint;
		this.maxDistanceInKm = maxDistanceInKm;
		
		double lat = refPoint.getY();
		double lon = refPoint.getX();
		
		// first-cut bounding box (in degrees)
		double maxLat = lat + Math.toDegrees(maxDistanceInKm / earthRadiusInKm);
		double minLat = lat - Math.toDegrees(maxDistanceInKm / earthRadiusInKm);
		// compensate for degrees longitude getting smaller with increasing latitude
		double maxLon = lon + Math.toDegrees(maxDistanceInKm / earthRadiusInKm / Math.cos(Math.toRadians(lat)));
		double minLon = lon - Math.toDegrees(maxDistanceInKm / earthRadiusInKm / Math.cos(Math.toRadians(lat)));		
		bbox = new Envelope(minLon, maxLon, minLat, maxLat);
	}

	public boolean needsToVisit(Node indexNode) {
		return getEnvelope(indexNode).intersects(bbox);
	}
	
	public void onIndexReference(Node geomNode) {
		Geometry geometry = decode(geomNode);
		Point point = geometry.getInteriorPoint();
		
		// d = acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1)) * R
		double distanceInKm = Math.acos(
				Math.sin(Math.toRadians(refPoint.getY())) * Math.sin(Math.toRadians(point.getY())) + 
				Math.cos(Math.toRadians(refPoint.getY())) * Math.cos(Math.toRadians(point.getY())) * Math.cos(Math.toRadians(point.getX()) - Math.toRadians(refPoint.getX()))) * earthRadiusInKm;
		
		if (distanceInKm < maxDistanceInKm) {
			add(geomNode, geometry);
		}
	}

	
	private Point refPoint;
	private double maxDistanceInKm;
	private Envelope bbox;
	
	private static final double earthRadiusInKm = 6371;
}