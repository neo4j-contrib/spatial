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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.neo4j.Neo4jFeatureBuilder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.collection.AbstractFeatureCollection;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialRecord;
import org.neo4j.gis.spatial.SpatialTopologyUtils;
import org.neo4j.gis.spatial.filter.SearchIntersectWindow;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.pipes.filtering.FilterCQL;
import org.neo4j.gis.spatial.pipes.filtering.FilterContain;
import org.neo4j.gis.spatial.pipes.filtering.FilterCover;
import org.neo4j.gis.spatial.pipes.filtering.FilterCoveredBy;
import org.neo4j.gis.spatial.pipes.filtering.FilterCross;
import org.neo4j.gis.spatial.pipes.filtering.FilterDisjoint;
import org.neo4j.gis.spatial.pipes.filtering.FilterEmpty;
import org.neo4j.gis.spatial.pipes.filtering.FilterEqualExact;
import org.neo4j.gis.spatial.pipes.filtering.FilterEqualNorm;
import org.neo4j.gis.spatial.pipes.filtering.FilterEqualTopo;
import org.neo4j.gis.spatial.pipes.filtering.FilterInRelation;
import org.neo4j.gis.spatial.pipes.filtering.FilterIntersect;
import org.neo4j.gis.spatial.pipes.filtering.FilterIntersectWindow;
import org.neo4j.gis.spatial.pipes.filtering.FilterInvalid;
import org.neo4j.gis.spatial.pipes.filtering.FilterOverlap;
import org.neo4j.gis.spatial.pipes.filtering.FilterProperty;
import org.neo4j.gis.spatial.pipes.filtering.FilterPropertyNotNull;
import org.neo4j.gis.spatial.pipes.filtering.FilterPropertyNull;
import org.neo4j.gis.spatial.pipes.filtering.FilterTouch;
import org.neo4j.gis.spatial.pipes.filtering.FilterValid;
import org.neo4j.gis.spatial.pipes.filtering.FilterWithin;
import org.neo4j.gis.spatial.pipes.impl.FilterPipe;
import org.neo4j.gis.spatial.pipes.impl.IdentityPipe;
import org.neo4j.gis.spatial.pipes.impl.Pipe;
import org.neo4j.gis.spatial.pipes.impl.Pipeline;
import org.neo4j.gis.spatial.pipes.impl.RangeFilterPipe;
import org.neo4j.gis.spatial.pipes.processing.ApplyAffineTransformation;
import org.neo4j.gis.spatial.pipes.processing.Area;
import org.neo4j.gis.spatial.pipes.processing.Boundary;
import org.neo4j.gis.spatial.pipes.processing.Buffer;
import org.neo4j.gis.spatial.pipes.processing.Centroid;
import org.neo4j.gis.spatial.pipes.processing.ConvexHull;
import org.neo4j.gis.spatial.pipes.processing.CopyDatabaseRecordProperties;
import org.neo4j.gis.spatial.pipes.processing.Densify;
import org.neo4j.gis.spatial.pipes.processing.DensityIslands;
import org.neo4j.gis.spatial.pipes.processing.Difference;
import org.neo4j.gis.spatial.pipes.processing.Dimension;
import org.neo4j.gis.spatial.pipes.processing.Distance;
import org.neo4j.gis.spatial.pipes.processing.EndPoint;
import org.neo4j.gis.spatial.pipes.processing.ExtractGeometries;
import org.neo4j.gis.spatial.pipes.processing.ExtractPoints;
import org.neo4j.gis.spatial.pipes.processing.GML;
import org.neo4j.gis.spatial.pipes.processing.GeoJSON;
import org.neo4j.gis.spatial.pipes.processing.GeometryType;
import org.neo4j.gis.spatial.pipes.processing.InteriorPoint;
import org.neo4j.gis.spatial.pipes.processing.IntersectAll;
import org.neo4j.gis.spatial.pipes.processing.Intersection;
import org.neo4j.gis.spatial.pipes.processing.KeyholeMarkupLanguage;
import org.neo4j.gis.spatial.pipes.processing.Length;
import org.neo4j.gis.spatial.pipes.processing.Max;
import org.neo4j.gis.spatial.pipes.processing.Min;
import org.neo4j.gis.spatial.pipes.processing.NumGeometries;
import org.neo4j.gis.spatial.pipes.processing.NumPoints;
import org.neo4j.gis.spatial.pipes.processing.OrthodromicDistance;
import org.neo4j.gis.spatial.pipes.processing.OrthodromicLength;
import org.neo4j.gis.spatial.pipes.processing.SimplifyPreservingTopology;
import org.neo4j.gis.spatial.pipes.processing.SimplifyWithDouglasPeucker;
import org.neo4j.gis.spatial.pipes.processing.Sort;
import org.neo4j.gis.spatial.pipes.processing.StartPoint;
import org.neo4j.gis.spatial.pipes.processing.SymDifference;
import org.neo4j.gis.spatial.pipes.processing.Union;
import org.neo4j.gis.spatial.pipes.processing.UnionAll;
import org.neo4j.gis.spatial.pipes.processing.WellKnownText;
import org.neo4j.gis.spatial.rtree.filter.SearchAll;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class GeoPipeline extends Pipeline<GeoPipeFlow, GeoPipeFlow> {

	protected final Layer layer;

	protected GeoPipeline(Layer layer) {
		this.layer = layer;
	}

	protected static IdentityPipe<GeoPipeFlow> createStartPipe(List<SpatialDatabaseRecord> records) {
		return createStartPipe(records.iterator());
	}

	protected static IdentityPipe<GeoPipeFlow> createStartPipe(final Iterator<SpatialDatabaseRecord> records) {
		final Iterator<GeoPipeFlow> start = new Iterator<>() {
			@Override
			public boolean hasNext() {
				return records.hasNext();
			}

			@Override
			public GeoPipeFlow next() {
				return new GeoPipeFlow(records.next());
			}

			@Override
			public void remove() {
				records.remove();
			}
		};
		return new IdentityPipe<>() {
			{
				super.setStarts(start);
			}
		};
	}

	/**
	 * Start a new pipeline with an iterator of SpatialDatabaseRecords
	 */
	public static GeoPipeline start(Layer layer, Iterator<SpatialDatabaseRecord> records) {
		GeoPipeline pipeline = new GeoPipeline(layer);
		return pipeline.add(createStartPipe(records));
	}

	/**
	 * Start a new pipeline with a list of SpatialDatabaseRecords
	 */
	public static GeoPipeline start(Layer layer, List<SpatialDatabaseRecord> records) {
		GeoPipeline pipeline = new GeoPipeline(layer);
		return pipeline.add(createStartPipe(records));
	}

	/**
	 * Start a new pipeline that will iterate through a SearchRecords
	 */
	public static GeoPipeline start(Layer layer, SearchRecords records) {
		GeoPipeline pipeline = new GeoPipeline(layer);
		return pipeline.add(createStartPipe(records));
	}

	/**
	 * Start a new pipeline that will iterate through a SearchFilter
	 */
	public static GeoPipeline start(final Transaction tx, Layer layer, SearchFilter searchFilter) {
		return start(layer, layer.getIndex().search(tx, searchFilter));
	}

	/**
	 * Start a new pipeline that will iterate through all items contained in a Layer
	 */
	public static GeoPipeline start(final Transaction tx, Layer layer) {
		return start(tx, layer, new SearchAll());
	}

	/**
	 * Extracts layer items that are found in the search window
	 */
	public static GeoPipeline startIntersectWindowSearch(final Transaction tx, Layer layer, Envelope searchWindow) {
		return start(layer, layer.getIndex().search(tx, new SearchIntersectWindow(layer, searchWindow)));
	}

	/**
	 * Extracts Layer items that contain the given geometry and start a pipeline.
	 */
	public static GeoPipeline startContainSearch(final Transaction tx, Layer layer, Geometry geometry) {
		return startIntersectWindowSearch(tx, layer, geometry.getEnvelopeInternal()).containFilter(geometry);
	}

	/**
	 * Extracts Layer items that cover the given geometry and start a pipeline.
	 */
	public static GeoPipeline startCoverSearch(final Transaction tx, Layer layer, Geometry geometry) {
		return startIntersectWindowSearch(tx, layer, geometry.getEnvelopeInternal()).coverFilter(geometry);
	}

	/**
	 * Extracts Layer items that are covered by the given geometry and start a pipeline.
	 */
	public static GeoPipeline startCoveredBySearch(final Transaction tx, Layer layer, Geometry geometry) {
		return startIntersectWindowSearch(tx, layer, geometry.getEnvelopeInternal()).coveredByFilter(geometry);
	}

	/**
	 * Extracts Layer items that cross by the given geometry and start a pipeline.
	 */
	public static GeoPipeline startCrossSearch(final Transaction tx, Layer layer, Geometry geometry) {
		return startIntersectWindowSearch(tx, layer, geometry.getEnvelopeInternal()).crossFilter(geometry);
	}

	/**
	 * Extracts Layer items that are equal to the given geometry and start a pipeline.
	 */
	public static GeoPipeline startEqualExactSearch(final Transaction tx, Layer layer, Geometry geometry,
			double tolerance) {
		return startIntersectWindowSearch(tx, layer, geometry.getEnvelopeInternal())
				.equalExactFilter(geometry, tolerance);
	}

	/**
	 * Extracts Layer items that intersect the given geometry and start a pipeline.
	 */
	public static GeoPipeline startIntersectSearch(final Transaction tx, Layer layer, Geometry geometry) {
		return startIntersectWindowSearch(tx, layer, geometry.getEnvelopeInternal())
				.intersectionFilter(geometry);
	}

	/**
	 * Extracts Layer items that overlap the given geometry and start a pipeline.
	 */
	public static GeoPipeline startOverlapSearch(final Transaction tx, Layer layer, Geometry geometry) {
		return startIntersectWindowSearch(tx, layer, geometry.getEnvelopeInternal()).overlapFilter(geometry);
	}

	/**
	 * Extracts Layer items that touch the given geometry and start a pipeline.
	 */
	public static GeoPipeline startTouchSearch(final Transaction tx, Layer layer, Geometry geometry) {
		return startIntersectWindowSearch(tx, layer, geometry.getEnvelopeInternal()).touchFilter(geometry);
	}

	/**
	 * Extracts Layer items that are within the given geometry and start a pipeline.
	 */
	public static GeoPipeline startWithinSearch(final Transaction tx, Layer layer, Geometry geometry) {
		return startIntersectWindowSearch(tx, layer, geometry.getEnvelopeInternal()).withinFilter(geometry);
	}

	/**
	 * Calculates the distance between Layer items nearest to the given point and the given point.
	 * The search window created is based on Layer items density, and it could lead to no results.
	 *
	 * @param layer               the layer to search
	 * @param point               with latitude, longitude coordinates
	 * @param numberOfItemsToFind tries to find this number of items for comparison
	 * @return geoPipeline
	 */
	public static GeoPipeline startNearestNeighborLatLonSearch(final Transaction tx, Layer layer, Coordinate point,
			int numberOfItemsToFind) {
		Envelope searchWindow = SpatialTopologyUtils.createEnvelopeForGeometryDensityEstimate(tx, layer, point,
				numberOfItemsToFind);
		return startNearestNeighborLatLonSearch(tx, layer, point, searchWindow);
	}

	/**
	 * Calculates the distance between Layer items inside the given search window and the given point.
	 *
	 * @param layer        the layer to search
	 * @param point        with latitude, longitude coordinates
	 * @param searchWindow the search window
	 * @return geoPipeline
	 */
	public static GeoPipeline startNearestNeighborLatLonSearch(final Transaction tx, Layer layer, Coordinate point,
			Envelope searchWindow) {
		return start(tx, layer, new SearchIntersectWindow(layer, searchWindow)).calculateOrthodromicDistance(point);
	}

	/**
	 * Extracts Layer items with a distance from the given point that is less than or equal the given distance.
	 *
	 * @param layer           the layer to search
	 * @param point           with latitude, longitude coordinates
	 * @param maxDistanceInKm the maximum distance in kilometers
	 * @return geoPipeline
	 */
	public static GeoPipeline startNearestNeighborLatLonSearch(final Transaction tx, Layer layer, Coordinate point,
			double maxDistanceInKm) {
		Envelope searchWindow = OrthodromicDistance.suggestSearchWindow(point, maxDistanceInKm);
		GeoPipeline pipeline = start(tx, layer,
				new SearchIntersectWindow(layer, searchWindow)).calculateOrthodromicDistance(point);
		return pipeline.propertyFilter(OrthodromicDistance.DISTANCE, maxDistanceInKm,
				FilterPipe.Filter.LESS_THAN_EQUAL);
	}

	/**
	 * Calculates the distance between Layer items nearest to the given point and the given point.
	 * The search window created is based on Layer items density, and it could lead to no results.
	 *
	 * @param layer               the layer to search
	 * @param point               with latitude, longitude coordinates
	 * @param numberOfItemsToFind tries to find this number of items for comparison
	 * @return geoPipeline
	 */
	public static GeoPipeline startNearestNeighborSearch(final Transaction tx, Layer layer, Coordinate point,
			int numberOfItemsToFind) {
		Envelope searchWindow = SpatialTopologyUtils.createEnvelopeForGeometryDensityEstimate(tx, layer, point,
				numberOfItemsToFind);
		return startNearestNeighborSearch(tx, layer, point, searchWindow);
	}

	/**
	 * Calculates the distance between Layer items inside the given search window and the given point.
	 *
	 * @param layer        the layer to search
	 * @param point        with latitude, longitude coordinates
	 * @param searchWindow the search window
	 * @return geoPipeline
	 */
	public static GeoPipeline startNearestNeighborSearch(final Transaction tx, Layer layer, Coordinate point,
			Envelope searchWindow) {
		return start(tx, layer, new SearchIntersectWindow(layer, searchWindow)).calculateDistance(
				layer.getGeometryFactory().createPoint(point));
	}

	/**
	 * Extracts Layer items with a distance from the given point that is less than or equal the given distance.
	 *
	 * @param layer       the layer to search
	 * @param point       with latitude, longitude coordinates
	 * @param maxDistance the maximum distance in kilometers
	 * @return geoPipeline
	 */
	public static GeoPipeline startNearestNeighborSearch(final Transaction tx, Layer layer, Coordinate point,
			double maxDistance) {
		Envelope extent = new Envelope(point.x - maxDistance, point.x + maxDistance,
				point.y - maxDistance, point.y + maxDistance);

		return start(tx, layer, new SearchIntersectWindow(layer, extent))
				.calculateDistance(layer.getGeometryFactory().createPoint(point))
				.propertyFilter("Distance", maxDistance, FilterPipe.Filter.LESS_THAN_EQUAL);
	}

	/**
	 * Adds a pipe at the end of this pipeline
	 *
	 * @param geoPipe the pipe to add
	 * @return geoPipeline
	 */
	public GeoPipeline addPipe(AbstractGeoPipe geoPipe) {
		return add(geoPipe);
	}

	/**
	 * @see CopyDatabaseRecordProperties
	 */
	public GeoPipeline copyDatabaseRecordProperties(Transaction tx) {
		return addPipe(new CopyDatabaseRecordProperties(tx));
	}

	/**
	 * @see CopyDatabaseRecordProperties
	 */
	public GeoPipeline copyDatabaseRecordProperties(Transaction tx, String[] keys) {
		return addPipe(new CopyDatabaseRecordProperties(tx, keys));
	}

	/**
	 * @see CopyDatabaseRecordProperties
	 */
	public GeoPipeline copyDatabaseRecordProperties(Transaction tx, String key) {
		return addPipe(new CopyDatabaseRecordProperties(tx, key));
	}

	/**
	 * @see Min
	 */
	public GeoPipeline getMin(String property) {
		return addPipe(new Min(property));
	}

	/**
	 * @see Max
	 */
	public GeoPipeline getMax(String property) {
		return addPipe(new Max(property));
	}

	/**
	 * @see Sort
	 */
	public GeoPipeline sort(String property) {
		return addPipe(new Sort(property, true));
	}

	/**
	 * @see Sort
	 */
	public GeoPipeline sort(String property, boolean asc) {
		return addPipe(new Sort(property, asc));
	}

	/**
	 * @see Sort
	 */
	public GeoPipeline sort(String property, Comparator<Object> comparator) {
		return addPipe(new Sort(property, comparator));
	}

	/**
	 * @see Boundary
	 */
	public GeoPipeline toBoundary() {
		return addPipe(new Boundary());
	}

	/**
	 * @see Buffer
	 */
	public GeoPipeline toBuffer(double distance) {
		return addPipe(new Buffer(distance));
	}

	/**
	 * @see Centroid
	 */
	public GeoPipeline toCentroid() {
		return addPipe(new Centroid());
	}

	/**
	 * @see ConvexHull
	 */
	public GeoPipeline toConvexHull() {
		return addPipe(new ConvexHull());
	}

	/**
	 * @see org.neo4j.gis.spatial.pipes.processing.Envelope
	 */
	public GeoPipeline toEnvelope() {
		return addPipe(new org.neo4j.gis.spatial.pipes.processing.Envelope());
	}

	/**
	 * @see InteriorPoint
	 */
	public GeoPipeline toInteriorPoint() {
		return addPipe(new InteriorPoint());
	}

	/**
	 * @see StartPoint
	 */
	public GeoPipeline toStartPoint() {
		return addPipe(new StartPoint(layer.getGeometryFactory()));
	}

	/**
	 * @see EndPoint
	 */
	public GeoPipeline toEndPoint() {
		return addPipe(new EndPoint(layer.getGeometryFactory()));
	}

	/**
	 * @see NumPoints
	 */
	public GeoPipeline countPoints() {
		return addPipe(new NumPoints());
	}

	/**
	 * @see Union
	 */
	public GeoPipeline union() {
		return addPipe(new Union());
	}

	/**
	 * @see Union
	 */
	public GeoPipeline union(Geometry geometry) {
		return addPipe(new Union(geometry));
	}

	/**
	 * @see UnionAll
	 */
	public GeoPipeline unionAll() {
		return addPipe(new UnionAll());
	}

	/**
	 * @see Intersection
	 */
	public GeoPipeline intersect(Geometry geometry) {
		return addPipe(new Intersection(geometry));
	}

	/**
	 * @see IntersectAll
	 */
	public GeoPipeline intersectAll() {
		return addPipe(new IntersectAll());
	}

	/**
	 * @see Difference
	 */
	public GeoPipeline difference(Geometry geometry) {
		return addPipe(new Difference(geometry));
	}

	/**
	 * @see SymDifference
	 */
	public GeoPipeline symDifference(Geometry geometry) {
		return addPipe(new SymDifference(geometry));
	}

	/**
	 * @see SimplifyWithDouglasPeucker
	 */
	public GeoPipeline simplifyWithDouglasPeucker(double distanceTolerance) {
		return addPipe(new SimplifyWithDouglasPeucker(distanceTolerance));
	}

	/**
	 * @see SimplifyPreservingTopology
	 */
	public GeoPipeline simplifyPreservingTopology(double distanceTolerance) {
		return addPipe(new SimplifyPreservingTopology(distanceTolerance));
	}

	/**
	 * @see ApplyAffineTransformation
	 */
	public GeoPipeline applyAffineTransform(AffineTransformation t) {
		return addPipe(new ApplyAffineTransformation(t));
	}

	/**
	 * @see Densify
	 */
	public GeoPipeline densify(double distanceTolerance) {
		return addPipe(new Densify(distanceTolerance));
	}

	/**
	 * @see Area
	 */
	public GeoPipeline calculateArea() {
		return addPipe(new Area());
	}

	/**
	 * @see Length
	 */
	public GeoPipeline calculateLength() {
		return addPipe(new Length());
	}

	/**
	 * @see OrthodromicLength
	 */
	public GeoPipeline calculateOrthodromicLength(Transaction tx) {
		return addPipe(new OrthodromicLength(layer.getCoordinateReferenceSystem(tx)));
	}

	/**
	 * @see Distance
	 */
	public GeoPipeline calculateDistance(Geometry reference) {
		return addPipe(new Distance(reference));
	}

	/**
	 * @see OrthodromicDistance
	 */
	public GeoPipeline calculateOrthodromicDistance(Coordinate reference) {
		return addPipe(new OrthodromicDistance(reference));
	}

	/**
	 * @see Dimension
	 */
	public GeoPipeline getDimension() {
		return addPipe(new Dimension());
	}

	/**
	 * @see GeometryType
	 */
	public GeoPipeline getGeometryType() {
		return addPipe(new GeometryType());
	}

	/**
	 * @see NumGeometries
	 */
	public GeoPipeline getNumGeometries() {
		return addPipe(new NumGeometries());
	}

	/**
	 * @see GeoJSON
	 */
	public GeoPipeline createJson() {
		return addPipe(new GeoJSON());
	}

	/**
	 * @see WellKnownText
	 */
	public GeoPipeline createWellKnownText() {
		return addPipe(new WellKnownText());
	}

	/**
	 * @see KeyholeMarkupLanguage
	 */
	public GeoPipeline createKML() {
		return addPipe(new KeyholeMarkupLanguage());
	}

	/**
	 * @see GML
	 */
	public GeoPipeline createGML() {
		return addPipe(new GML());
	}

	/**
	 * @see FilterProperty
	 */
	public GeoPipeline propertyFilter(String key, Object value) {
		return addPipe(new FilterProperty(key, value));
	}

	/**
	 * @see FilterProperty
	 */
	public GeoPipeline propertyFilter(String key, Object value, FilterPipe.Filter comparison) {
		return addPipe(new FilterProperty(key, value, comparison));
	}

	/**
	 * @see FilterPropertyNotNull
	 */
	public GeoPipeline propertyNotNullFilter(String key) {
		return addPipe(new FilterPropertyNotNull(key));
	}

	/**
	 * @see FilterPropertyNull
	 */
	public GeoPipeline propertyNullFilter(String key) {
		return addPipe(new FilterPropertyNull(key));
	}

	/**
	 * @see FilterCQL
	 */
	public GeoPipeline cqlFilter(Transaction tx, String cql) throws CQLException {
		return addPipe(new FilterCQL(tx, layer, cql));
	}

	/**
	 * @see FilterIntersect
	 */
	public GeoPipeline intersectionFilter(Geometry geometry) {
		return addPipe(new FilterIntersect(geometry));
	}

	/**
	 * @see FilterIntersectWindow
	 */
	public GeoPipeline windowIntersectionFilter(double xmin, double ymin, double xmax, double ymax) {
		return addPipe(new FilterIntersectWindow(layer.getGeometryFactory(), xmin, ymin, xmax, ymax));
	}

	/**
	 * @see FilterIntersectWindow
	 */
	public GeoPipeline windowIntersectionFilter(Envelope envelope) {
		return addPipe(new FilterIntersectWindow(layer.getGeometryFactory(), envelope));
	}

	/**
	 * @see FilterContain
	 */
	public GeoPipeline containFilter(Geometry geometry) {
		return addPipe(new FilterContain(geometry));
	}

	/**
	 * @see FilterCover
	 */
	public GeoPipeline coverFilter(Geometry geometry) {
		return addPipe(new FilterCover(geometry));
	}

	/**
	 * @see FilterCoveredBy
	 */
	public GeoPipeline coveredByFilter(Geometry geometry) {
		return addPipe(new FilterCoveredBy(geometry));
	}

	/**
	 * @see FilterCross
	 */
	public GeoPipeline crossFilter(Geometry geometry) {
		return addPipe(new FilterCross(geometry));
	}

	/**
	 * @see FilterDisjoint
	 */
	public GeoPipeline disjointFilter(Geometry geometry) {
		return addPipe(new FilterDisjoint(geometry));
	}

	/**
	 * @see FilterEmpty
	 */
	public GeoPipeline emptyFilter() {
		return addPipe(new FilterEmpty());
	}

	/**
	 * @see FilterEqualExact
	 */
	public GeoPipeline equalExactFilter(Geometry geometry, double tolerance) {
		return addPipe(new FilterEqualExact(geometry, tolerance));
	}

	/**
	 * @see FilterEqualNorm
	 */
	public GeoPipeline equalNormFilter(Geometry geometry, double tolerance) {
		return addPipe(new FilterEqualNorm(geometry, tolerance));
	}

	/**
	 * @see FilterEqualTopo
	 */
	public GeoPipeline equalTopoFilter(Geometry geometry) {
		return addPipe(new FilterEqualTopo(geometry));
	}

	/**
	 * @see FilterInRelation
	 */
	public GeoPipeline relationFilter(Geometry geometry, String intersectionPattern) {
		return addPipe(new FilterInRelation(geometry, intersectionPattern));
	}

	/**
	 * @see FilterValid
	 */
	public GeoPipeline validFilter() {
		return addPipe(new FilterValid());
	}

	/**
	 * @see FilterInvalid
	 */
	public GeoPipeline invalidFilter() {
		return addPipe(new FilterInvalid());
	}

	/**
	 * @see FilterOverlap
	 */
	public GeoPipeline overlapFilter(Geometry geometry) {
		return addPipe(new FilterOverlap(geometry));
	}

	/**
	 * @see FilterTouch
	 */
	public GeoPipeline touchFilter(Geometry geometry) {
		return addPipe(new FilterTouch(geometry));
	}

	/**
	 * @see FilterWithin
	 */
	public GeoPipeline withinFilter(Geometry geometry) {
		return addPipe(new FilterWithin(geometry));
	}

	/**
	 * @see DensityIslands
	 */
	public GeoPipeline groupByDensityIslands(double density) {
		return addPipe(new DensityIslands(density));
	}

	/**
	 * @see ExtractPoints
	 */
	public GeoPipeline extractPoints() {
		return addPipe(new ExtractPoints(layer.getGeometryFactory()));
	}

	/**
	 * @see ExtractGeometries
	 */
	public GeoPipeline extractGeometries() {
		return addPipe(new ExtractGeometries());
	}

	public FeatureCollection<SimpleFeatureType, SimpleFeature> toStreamingFeatureCollection(final Transaction tx,
			final Envelope bounds) {
		return toStreamingFeatureCollection(tx, Neo4jFeatureBuilder.getTypeFromLayer(tx, layer), bounds);
	}

	public FeatureCollection<SimpleFeatureType, SimpleFeature> toStreamingFeatureCollection(final Transaction tx,
			SimpleFeatureType featureType, final Envelope bounds) {
		final Neo4jFeatureBuilder featureBuilder = Neo4jFeatureBuilder.fromLayer(tx, layer);
		return new AbstractFeatureCollection(featureType) {
			@Override
			public int size() {
				return Integer.MAX_VALUE;
			}

			@Override
			protected Iterator<SimpleFeature> openIterator() {
				return new Iterator<>() {
					@Override
					public boolean hasNext() {
						return GeoPipeline.this.hasNext();
					}

					@Override
					public SimpleFeature next() {
						return featureBuilder.buildFeature(tx, GeoPipeline.this.next().getRecord());
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}

			@Override
			public ReferencedEnvelope getBounds() {
				return new ReferencedEnvelope(bounds, layer.getCoordinateReferenceSystem(tx));
			}
		};
	}

	public FeatureCollection<SimpleFeatureType, SimpleFeature> toFeatureCollection(final Transaction tx) {
		return toFeatureCollection(tx, Neo4jFeatureBuilder.getTypeFromLayer(tx, layer));
	}

	public FeatureCollection<SimpleFeatureType, SimpleFeature> toFeatureCollection(final Transaction tx,
			SimpleFeatureType featureType) {
		final List<GeoPipeFlow> records = toList();

		Envelope bounds = null;
		for (SpatialRecord record : records) {
			if (bounds == null) {
				bounds = record.getGeometry().getEnvelopeInternal();
			} else {
				bounds.expandToInclude(record.getGeometry().getEnvelopeInternal());
			}
		}

		final Iterator<GeoPipeFlow> recordsIterator = records.iterator();
		final ReferencedEnvelope refBounds = new ReferencedEnvelope(bounds, layer.getCoordinateReferenceSystem(tx));

		final Neo4jFeatureBuilder featureBuilder = new Neo4jFeatureBuilder(featureType,
				Arrays.asList(layer.getExtraPropertyNames(tx)));
		return new AbstractFeatureCollection(featureType) {
			@Override
			public int size() {
				return records.size();
			}

			@Override
			protected Iterator<SimpleFeature> openIterator() {
				return new Iterator<>() {

					@Override
					public boolean hasNext() {
						return recordsIterator.hasNext();
					}

					@Override
					public SimpleFeature next() {
						return featureBuilder.buildFeature(tx, recordsIterator.next());
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}

			@Override
			public ReferencedEnvelope getBounds() {
				return refBounds;
			}
		};
	}

	/**
	 * Iterates through the pipeline content and creates a list of all the SpatialDatabaseRecord found.
	 * This will empty the pipeline.
	 * <p>
	 * Warning: this method should not be used with pipes that extract many items from a single item
	 * or with pipes that group many items into fewer items.
	 * <p>
	 * Warning: GeoPipeline doesn't modify SpatialDatabaseRecords thus the geometries contained aren't those
	 * transformed by the pipeline but the original ones.
	 */
	public List<SpatialDatabaseRecord> toSpatialDatabaseRecordList() {

		List<SpatialDatabaseRecord> result = new ArrayList<>();

		while (true) {
			try {
				result.add(next().getRecord());
			} catch (NoSuchElementException e) {
				break;
			}
		}
		return result;
	}

	/**
	 * Iterates through the pipeline content and creates a list of all the Nodes found.
	 * This will empty the pipeline.
	 * <p>
	 * Warning: this method should *not* be used with pipes that extract many items from a single item
	 * or with pipes that group many items into fewer items.
	 */
	public List<Node> toNodeList() {
		List<Node> result = new ArrayList<>();
		while (true) {
			try {
				result.add(next().getRecord().getGeomNode());
			} catch (NoSuchElementException e) {
				break;
			}
		}
		return result;

	}

	public <T> GeoPipeline add(final Pipe<?, T> pipe) {
		this.addPipe(pipe);
		return this;
	}

	public GeoPipeline range(final int low, final int high) {
		return this.add(new RangeFilterPipe<GeoPipeFlow>(low, high));
	}
}
