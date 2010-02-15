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
package org.neo4j.gis.spatial;

import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;


/**
 * @author Davide Savazzi 
 */
public class GeometryUtils implements Constants {

	// Public methods
	
	public static Envelope getEnvelope(Node node) {
		double[] bbox = (double[]) node.getProperty(PROP_BBOX);
			
		// Envelope parameters: xmin, xmax, ymin, ymax)
		return new Envelope(bbox[0], bbox[2], bbox[1], bbox[3]);
	}	
	
	public static Envelope getEnvelope(Envelope e, Envelope e1) {
		Envelope result = new Envelope(e);
		result.expandToInclude(e1);
		return result;
	}	
	
	public static void encode(Geometry geom, Node geomNode) {
		geomNode.setProperty(PROP_TYPE, encodeGeometryType(geom.getGeometryType()));

		Envelope mbb = geom.getEnvelopeInternal();				
		geomNode.setProperty(PROP_BBOX, new double[] { mbb.getMinX(), mbb.getMinY(), mbb.getMaxX(), mbb.getMaxY() });					

		if (geom.getNumGeometries() == 1) {
			geomNode.setProperty(PROP_COORDINATES, encodeCoordinates(geom.getCoordinates()));			
		} else {
			// geometry collection
			int[] coordinatesLength = new int[geom.getNumGeometries()];
			for (int i = 0; i < geom.getNumGeometries(); i++) {
				Geometry childGeom = geom.getGeometryN(i);
				coordinatesLength[i] = childGeom.getCoordinates().length;
			}
			
			geomNode.setProperty(PROP_COORDINATES_LENGTH, coordinatesLength);
			geomNode.setProperty(PROP_COORDINATES, geom.getCoordinates());
		}
	}

	public static Geometry decode(Node geomNode, GeometryFactory geomFactory) {
		Integer type = (Integer) geomNode.getProperty(PROP_TYPE);
		Coordinate[] coordinates = decodeCoordinates((double[]) geomNode.getProperty(PROP_COORDINATES));

		boolean isGeometryCollection = geomNode.hasProperty(PROP_COORDINATES_LENGTH);
		if (isGeometryCollection) {
			int[] coordinatesLength = (int[]) geomNode.getProperty(PROP_COORDINATES_LENGTH);
			
			if (GTYPE_POINT == type) {
				return geomFactory.createMultiPoint(coordinates);
			} else if (GTYPE_LINESTRING == type) {
				LineString[] lineStrings = new LineString[coordinatesLength.length];
				for (int i = 0; i < lineStrings.length; i++) {
					lineStrings[i] = geomFactory.createLineString(extractCoordinates(coordinates, coordinatesLength, i));
				}
				return geomFactory.createMultiLineString(lineStrings);
			} else if (GTYPE_POLYGON == type) {
				Polygon[] polygons = new Polygon[coordinatesLength.length];
				for (int i = 0; i < polygons.length; i++) {
					// TODO Polygon holes not yet supported
					polygons[i] = geomFactory.createPolygon(geomFactory.createLinearRing(extractCoordinates(coordinates, coordinatesLength, i)), null);
				}
				return geomFactory.createMultiPolygon(polygons);
			} else {
				throw new UnsupportedOperationException("unknown type:" + type);
			}
		} else {
			return createGeometry(geomFactory, type, coordinates);
		}
	}
	
	
	// Private methods

	private static Coordinate[] extractCoordinates(Coordinate[] coordinates, int[] coordinatesLength, int index) {
		int start = 0;
		for (int i = 0; i < index; i++) {
			start += coordinatesLength[i];
		}
		
		Coordinate[] extracted = new Coordinate[coordinatesLength[index]];
		for (int i = start; i < (start + extracted.length); i++) {
			extracted[i - start] = coordinates[i];
		}
		return extracted;
	}
	
	private static Geometry createGeometry(GeometryFactory geomFactory, Integer type, Coordinate[] coordinates) {
		if (GTYPE_POINT == type) {
			return geomFactory.createPoint(coordinates[0]);
		} else if (GTYPE_LINESTRING == type) {
			return geomFactory.createLineString(coordinates);
		} else if (GTYPE_POLYGON == type) {
			// TODO Polygon holes not yet supported
			return geomFactory.createPolygon(geomFactory.createLinearRing(coordinates), null);
		} else {
			throw new UnsupportedOperationException("unknown type:" + type);
		}
	}
	
	private static Integer encodeGeometryType(String jtsGeometryType) {
		if ("Point".equals(jtsGeometryType) || "MultiPoint".equals(jtsGeometryType)) {
			return GTYPE_POINT;
		} else if ("LineString".equals(jtsGeometryType) || "MultiLineString".equals(jtsGeometryType)) {
			return GTYPE_LINESTRING;
		} else if ("Polygon".equals(jtsGeometryType) || "MultiPolygon".equals(jtsGeometryType)) {
			return GTYPE_POLYGON;
		} else {
			throw new UnsupportedOperationException("unknown type:" + jtsGeometryType);
		}
	}

	private static double[] encodeCoordinates(Coordinate[] rawCoordinates) {
		double[] coordinates = new double[rawCoordinates.length * 2];
		for (int i = 0; i < rawCoordinates.length; i++) {
			coordinates[i * 2] = rawCoordinates[i].x;
			coordinates[i * 2 + 1] = rawCoordinates[i].y;
		}
		return coordinates;
	}
	
	private static Coordinate[] decodeCoordinates(double[] coordinates) {
		Coordinate[] rawCoordinates = new Coordinate[coordinates.length / 2];
		for (int i = 0; i < rawCoordinates.length; i++) {
			rawCoordinates[i] = new Coordinate(coordinates[i * 2], coordinates[i * 2 + 1]);
		}
		return rawCoordinates;
	}
}