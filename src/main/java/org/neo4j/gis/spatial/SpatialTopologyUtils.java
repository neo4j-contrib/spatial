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
package org.neo4j.gis.spatial;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import org.geotools.geometry.jts.ReferencedEnvelope;
import org.neo4j.gis.spatial.query.SearchIntersect;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.linearref.LengthIndexedLine;
import com.vividsolutions.jts.linearref.LinearLocation;
import com.vividsolutions.jts.linearref.LocationIndexedLine;

/**
 * This class is a temporary location for collecting a number of spatial
 * utilities before we have decided on a more complete analysis structure. Do
 * not rely on this API remaining constant.
 * 
 * @author craig
 */
public class SpatialTopologyUtils {
	/**
	 * Inner class associating points and resulting geometry records to
	 * facilitate the result set returned.
	 * 
	 * @author craig
	 */
	public static class PointResult implements
			Map.Entry<Point, SpatialDatabaseRecord>, Comparable<PointResult> {
		private Point point;
		private SpatialDatabaseRecord record;
		private double distance;

		private PointResult(Point point, SpatialDatabaseRecord record,
				double distance) {
			this.point = point;
			this.record = record;
			this.distance = distance;
		}

		public Point getKey() {
			return point;
		}

		public SpatialDatabaseRecord getValue() {
			return record;
		}

		public double getDistance() {
			return distance;
		}

		public SpatialDatabaseRecord setValue(SpatialDatabaseRecord value) {
			return this.record = value;
		}

		public int compareTo(PointResult other) {
			if (this.distance == other.distance) {
				return 0;
			} else if (this.distance < other.distance) {
				return -1;
			} else {
				return 1;
			}
		}

		public String toString() {
			return "Point[" + point + "] distance[" + distance + "] record["
					+ record + "]";
		}
	}

	public static ArrayList<PointResult> findClosestEdges(Point point,
			Layer layer) {
		return findClosestEdges(point, layer, 0.0);
	}

	public static ArrayList<PointResult> findClosestEdges(Point point,
			Layer layer, double distance) {
		ReferencedEnvelope env = new ReferencedEnvelope(
				Utilities.fromNeo4jToJts(layer.getIndex().getBoundingBox()), 
				layer.getCoordinateReferenceSystem());
		if (distance <= 0.0)
			distance = env.getSpan(0) / 100.0;
		Envelope search = new Envelope(point.getCoordinate());
		search.expandBy(distance);
		GeometryFactory factory = layer.getGeometryFactory();
		return findClosestEdges(point, layer, factory.toGeometry(search));
	}

	public static ArrayList<PointResult> findClosestEdges(Point point,
			Layer layer, Geometry filter) {
		ArrayList<PointResult> results = new ArrayList<PointResult>();
		LayerSearch searchQuery = new SearchIntersect(filter);
		layer.getIndex().executeSearch(searchQuery);
		for (SpatialDatabaseRecord record : searchQuery.getExtendedResults()) {
			Geometry geom = record.getGeometry();
			if (geom instanceof LineString) {
				LocationIndexedLine line = new LocationIndexedLine(geom);
				LinearLocation here = line.project(point.getCoordinate());
				Coordinate snap = line.extractPoint(here);
				double distance = snap.distance(point.getCoordinate());
				results.add(new PointResult(layer.getGeometryFactory()
						.createPoint(snap), record, distance));
			}
		}
		Collections.sort(results);
		return results;
	}
	
	/**
	 * Create a Point located at the specified 'measure' distance along a
	 * Geometry. This is achieved through using the JTS
	 * LengthIndexedLine.extractPoint(measure) method for finding the
	 * coordinates at the specified measure along the geometry. It is equivalent
	 * to Oracle's SDO_LRS.LOCATE_PT.
	 * 
	 * @see http://download.oracle.com/docs/cd/B13789_01/appdev.101/b10826/sdo_lrs_ref.htm#i85478
	 * @see http://www.vividsolutions.com/jts/javadoc/com/vividsolutions/jts/linearref/LengthIndexedLine.html
	 * @param layer
	 *            Layer the geometry is contained by, and is used to access the
	 *            GeometryFactory for creating the Point
	 * @param geometry
	 *            Geometry to measure
	 * @param measure
	 *            the distance along the geometry
	 * @return Point at 'measure' distance along the geometry
	 */
	public static Point locatePoint(Layer layer, Geometry geometry, double measure) {
		return layer.getGeometryFactory().createPoint(locatePoint(geometry, measure));
	}

	/**
	 * Find the coordinate at the specified 'measure' distance along a
	 * Geometry. This is achieved through using the JTS
	 * LengthIndexedLine.extractPoint(measure) method for finding the
	 * coordinates at the specified measure along the geometry. It is equivalent
	 * to Oracle's SDO_LRS.LOCATE_PT.
	 * 
	 * @see http://download.oracle.com/docs/cd/B13789_01/appdev.101/b10826/sdo_lrs_ref.htm#i85478
	 * @see http://www.vividsolutions.com/jts/javadoc/com/vividsolutions/jts/linearref/LengthIndexedLine.html
	 * @param geometry
	 *            Geometry to measure
	 * @param measure
	 *            the distance along the geometry
	 * @return Coordinate at 'measure' distance along the geometry
	 */
	public static Coordinate locatePoint(Geometry geometry, double measure) {
		return new LengthIndexedLine(geometry).extractPoint(measure);
	}

	/**
	 * Create a Point located at the specified 'measure' distance along a
	 * Geometry, and offset to the left of the Geometry by the specified offset
	 * distance. This is achieved through using the JTS
	 * LengthIndexedLine.extractPoint(measure) method for finding the
	 * coordinates at the specified measure along the geometry. It is equivalent
	 * to Oracle's SDO_LRS.LOCATE_PT.
	 * 
	 * @see http://download.oracle.com/docs/cd/B13789_01/appdev.101/b10826/sdo_lrs_ref.htm#i85478
	 * @see http://www.vividsolutions.com/jts/javadoc/com/vividsolutions/jts/linearref/LengthIndexedLine.html
	 * @param layer
	 *            Layer the geometry is contained by, and is used to access the
	 *            GeometryFactory for creating the Point
	 * @param geometry
	 *            Geometry to measure
	 * @param measure
	 *            the distance along the geometry
	 * @param offset
	 *            the distance offset to the left (or right for negative numbers)
	 * @return Point at 'measure' distance along the geometry, and offset
	 */
	public static Point locatePoint(Layer layer, Geometry geometry, double measure, double offset) {
		return layer.getGeometryFactory().createPoint(locatePoint(geometry, measure, offset));
	}

	/**
	 * Find the coordinate located at the specified 'measure' distance along a
	 * Geometry, and offset to the left of the Geometry by the specified offset
	 * distance. This is achieved through using the JTS
	 * LengthIndexedLine.extractPoint(measure) method for finding the
	 * coordinates at the specified measure along the geometry. It is equivalent
	 * to Oracle's SDO_LRS.LOCATE_PT.
	 * 
	 * @see http://download.oracle.com/docs/cd/B13789_01/appdev.101/b10826/sdo_lrs_ref.htm#i85478
	 * @see http://www.vividsolutions.com/jts/javadoc/com/vividsolutions/jts/linearref/LengthIndexedLine.html
	 * @param layer
	 *            Layer the geometry is contained by, and is used to access the
	 *            GeometryFactory for creating the Point
	 * @param geometry
	 *            Geometry to measure
	 * @param measure
	 *            the distance along the geometry
	 * @param offset
	 *            the distance offset to the left (or right for negative numbers)
	 * @return Point at 'measure' distance along the geometry, and offset
	 */
	public static Coordinate locatePoint(Geometry geometry, double measure, double offset) {
		return new LengthIndexedLine(geometry).extractPoint(measure, offset);
	}

	/**
	 * Adjust the size and position of a ReferencedEnvelope using fractions of
	 * the current size. For example:
	 * 
	 * <pre>
	 * bounds = adjustBounds(bounds, 0.3, new double[] { -0.1, 0.1 });
	 * </pre>
	 * 
	 * This will zoom in to show 30% of the height and width, and will also
	 * move the visible window 10% to the left and 10% up.
	 * 
	 * @param bounds
	 *            current envelope
	 * @param zoomFactor
	 *            fraction of size to zoom in by
	 * @param offsetFactor
	 *            fraction of size to offset visible window by
	 * @return adjusted envelope
	 */
	public static ReferencedEnvelope adjustBounds(ReferencedEnvelope bounds,
			double zoomFactor, double[] offsetFactor) {
		if(offsetFactor==null || offsetFactor.length < bounds.getDimension()) {
			offsetFactor = new double[bounds.getDimension()];
		}
		ReferencedEnvelope scaled = new ReferencedEnvelope(bounds);
		if (Math.abs(zoomFactor - 1.0) > 0.01) {
			double[] min = scaled.getLowerCorner().getCoordinate();
			double[] max = scaled.getUpperCorner().getCoordinate();
			for (int i = 0; i < scaled.getDimension(); i++) {
				double span = scaled.getSpan(i);
				double delta = (span - span * zoomFactor) / 2.0;
				double shift = span * offsetFactor[i];
				System.out.println("Have offset["+i+"]: "+shift);
				min[i] += shift + delta;
				max[i] += shift - delta;
			}
			scaled = new ReferencedEnvelope(min[0], max[0], min[1], max[1],
					scaled.getCoordinateReferenceSystem());
		}
		return scaled;
	}

	public static Envelope adjustBounds(Envelope bounds, double zoomFactor, double[] offset) {
		if(offset==null || offset.length < 2) {
			offset = new double[]{0,0};
		}
		Envelope scaled = new Envelope(bounds);
		if (Math.abs(zoomFactor - 1.0) > 0.01) {
			double[] min = new double[] { scaled.getMinX(), scaled.getMinY() };
			double[] max = new double[] { scaled.getMaxX(), scaled.getMaxY() };
			for (int i = 0; i < 2; i++) {
				double shift = offset[i];
				System.out.println("Have offset["+i+"]: "+shift);
				double span = (i == 0) ? scaled.getWidth() : scaled.getHeight();
				double delta = (span - span * zoomFactor) / 2.0;
				min[i] += shift + delta;
				max[i] += shift - delta;
			}
			scaled = new Envelope(min[0], max[0], min[1], max[1]);
		}
		return scaled;
	}

	/**
	 * Create an Envelope that should approximately include the specified number
	 * of geometries, based on a simple linear calculation of the geometry
	 * density. If the layer has fewer geometries, then the layer bounds will be
	 * returned. If the limit is set to zero (or negative), a point Envelope
	 * will be returned.
	 * 
	 * @param layer
	 *            the layer whose geometry density is to be used to estimate the
	 *            size of the envelope
	 * @param point
	 *            the coordinate around which to build the envelope
	 * @param limit
	 *            the number of geometries to be included in the envelope
	 * @return an envelope designed to include the estimated number of
	 *         geometries
	 */
	public static Envelope createEnvelopeForGeometryDensityEstimate(Layer layer, Coordinate point, int limit) {
		if(limit < 1) {
			return new Envelope(point);
		}
		int count = layer.getIndex().count();
		if (count > limit) {
			return createEnvelopeForGeometryDensityEstimate(layer, point,(double) limit / (double) count);
		} else {
			return Utilities.fromNeo4jToJts(layer.getIndex().getBoundingBox());
		}
	}

	/**
	 * Create an Envelope that should approximately include the specified number
	 * of geometries, based on a simple linear calculation of the geometry
	 * density. If the layer has fewer geometries, then the layer bounds will be
	 * returned. If the limit is set to zero (or negative), a point Envelope
	 * will be returned.
	 * 
	 * @param layer
	 *            the layer whose geometry density is to be used to estimate the
	 *            size of the envelope
	 * @param point
	 *            the coordinate around which to build the envelope
	 * @param fraction
	 *            the fractional number of geometries to be included in the envelope
	 * @return an envelope designed to include the estimated number of
	 *         geometries
	 */
	public static Envelope createEnvelopeForGeometryDensityEstimate(Layer layer, Coordinate point, double fraction) {
		if(fraction < 0.0) {
			return new Envelope(point);
		}
		Envelope bbox = Utilities.fromNeo4jToJts(layer.getIndex().getBoundingBox());
		double width = bbox.getWidth() * fraction;
		double height = bbox.getWidth() * fraction;
		Envelope extent = new Envelope(point);
		extent.expandToInclude(point.x - width / 2.0, point.y - height / 2.0);
		extent.expandToInclude(point.x + width / 2.0, point.y + height / 2.0);
		return extent;
	}

}
