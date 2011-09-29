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
package org.neo4j.gis.spatial.pipes;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.geotools.filter.text.cql2.CQLException;
import org.neo4j.collections.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
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
import org.neo4j.gis.spatial.pipes.filtering.FilterEqual;
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
import org.neo4j.graphdb.Node;

import com.tinkerpop.pipes.filter.FilterPipe;
import com.tinkerpop.pipes.util.FluentPipeline;
import com.tinkerpop.pipes.util.StartPipe;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.util.AffineTransformation;


public class GeoPipeline extends FluentPipeline<GeoPipeFlow, GeoPipeFlow> {

	protected Layer layer;
	
	protected GeoPipeline(Layer layer) {
		this.layer = layer;
	}

	protected static StartPipe<GeoPipeFlow> createStartPipe(List<SpatialDatabaseRecord> records) {
		return createStartPipe(records.iterator());
	}	
	
	protected static StartPipe<GeoPipeFlow> createStartPipe(final Iterator<SpatialDatabaseRecord> records) {
		return new StartPipe<GeoPipeFlow>(new Iterator<GeoPipeFlow>() {
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
    	});
	}
	
	public static GeoPipeline start(Layer layer, List<SpatialDatabaseRecord> records) {
		GeoPipeline pipeline = new GeoPipeline(layer);
    	return (GeoPipeline) pipeline.add(createStartPipe(records));		
	}
	
    public static GeoPipeline start(Layer layer, SearchRecords records) {
    	GeoPipeline pipeline = new GeoPipeline(layer);
    	return (GeoPipeline) pipeline.add(createStartPipe(records));
    }
    
    public static GeoPipeline start(Layer layer, SearchFilter searchFilter) {
    	return start(layer, layer.getIndex().search(searchFilter));
    }

    public static GeoPipeline startIntersectWindowSearch(Layer layer, Envelope searchWindow) {
    	return start(layer, layer.getIndex().search(new SearchIntersectWindow(layer, searchWindow)));
    }
    
    public static GeoPipeline startContainSearch(Layer layer, Geometry geometry) {
    	return startIntersectWindowSearch(layer, geometry.getEnvelopeInternal())
    		.containFilter(geometry);
    }

    public static GeoPipeline startCoverSearch(Layer layer, Geometry geometry) {
    	return startIntersectWindowSearch(layer, geometry.getEnvelopeInternal())
    		.coverFilter(geometry);
    }    

    public static GeoPipeline startCoveredBySearch(Layer layer, Geometry geometry) {
    	return startIntersectWindowSearch(layer, geometry.getEnvelopeInternal())
    		.coveredByFilter(geometry);
    }    

    public static GeoPipeline startCrossSearch(Layer layer, Geometry geometry) {
    	return startIntersectWindowSearch(layer, geometry.getEnvelopeInternal())
    		.crossFilter(geometry);
    }    

    public static GeoPipeline startEqualSearch(Layer layer, Geometry geometry) {
    	return startIntersectWindowSearch(layer, geometry.getEnvelopeInternal())
    		.equalFilter(geometry);
    }    

    public static GeoPipeline startIntersectSearch(Layer layer, Geometry geometry) {
    	return startIntersectWindowSearch(layer, geometry.getEnvelopeInternal())
    		.intersectionFilter(geometry);
    }    

    public static GeoPipeline startOverlapSearch(Layer layer, Geometry geometry) {
    	return startIntersectWindowSearch(layer, geometry.getEnvelopeInternal())
    		.overlapFilter(geometry);
    }    

    public static GeoPipeline startTouchSearch(Layer layer, Geometry geometry) {
    	return startIntersectWindowSearch(layer, geometry.getEnvelopeInternal())
    		.touchFilter(geometry);
    }    
    
    public static GeoPipeline startWithinSearch(Layer layer, Geometry geometry) {
    	return startIntersectWindowSearch(layer, geometry.getEnvelopeInternal())
    		.withinFilter(geometry);
    }
    
	public static GeoPipeline startNearestNeighborLatLonSearch(Layer layer, Coordinate point, int limit) {
		Envelope searchWindow = SpatialTopologyUtils.createEnvelopeForGeometryDensityEstimate(layer, point, limit);
		return startNearestNeighborLatLonSearch(layer, point, searchWindow);
	}
    
	public static GeoPipeline startNearestNeighborLatLonSearch(Layer layer, Coordinate point, Envelope searchWindow) {
		return start(layer, new SearchIntersectWindow(layer, searchWindow))
			.calculateOrthodromicDistance(point);
	}

	public static GeoPipeline startNearestNeighborLatLonSearch(Layer layer, Coordinate point, double maxDistanceInKm) {
		Envelope extent = OrthodromicDistance.suggestSearchWindow(point, maxDistanceInKm);
		return start(layer, new SearchIntersectWindow(layer, extent))
			.calculateOrthodromicDistance(point)
			.propertyFilter("OrthodromicDistance", maxDistanceInKm, FilterPipe.Filter.LESS_THAN_EQUAL);
	}

	public static GeoPipeline startNearestNeighborSearch(Layer layer, Coordinate point, int limit) {	
		Envelope searchWindow = SpatialTopologyUtils.createEnvelopeForGeometryDensityEstimate(layer, point, limit);
		return startNearestNeighborSearch(layer, point, searchWindow);
	}
	
	public static GeoPipeline startNearestNeighborSearch(Layer layer, Coordinate point, Envelope searchWindow) {
		return start(layer, new SearchIntersectWindow(layer, searchWindow))
			.calculateDistance(layer.getGeometryFactory().createPoint(point));
	}
		
	public static GeoPipeline startNearestNeighborSearch(Layer layer, Coordinate point, double maxDistance) {
		Envelope extent = new Envelope(point.x - maxDistance, point.x + maxDistance, 
				point.y - maxDistance, point.y + maxDistance);
		
		return start(layer, new SearchIntersectWindow(layer, extent))
			.calculateDistance(layer.getGeometryFactory().createPoint(point))
			.propertyFilter("Distance", maxDistance, FilterPipe.Filter.LESS_THAN_EQUAL);	
	}
	
    public GeoPipeline addPipe(AbstractGeoPipe geoPipe) {
    	return (GeoPipeline) add(geoPipe);
    }

    public GeoPipeline copyDatabaseRecordProperties() {
    	return addPipe(new CopyDatabaseRecordProperties());
    }
    
    public GeoPipeline getMin(String property) {
    	return addPipe(new Min(property));
    }

    public GeoPipeline getMax(String property) {
    	return addPipe(new Max(property));
    }
    
    public GeoPipeline sort(String property) {
    	return addPipe(new Sort(property, true));
    }
    
    public GeoPipeline sort(String property, boolean asc) {
    	return addPipe(new Sort(property, asc));
    }
    
    public GeoPipeline sort(String property, Comparator<Object> comparator) {
    	return addPipe(new Sort(property, comparator));
    }    
    
    public GeoPipeline toBoundary() {
    	return addPipe(new Boundary());
    }
    
    public GeoPipeline toBuffer(double distance) {
    	return addPipe(new Buffer(distance));
    }
    
    public GeoPipeline toCentroid() {
    	return addPipe(new Centroid());
    }

    public GeoPipeline toConvexHull() {
    	return addPipe(new ConvexHull());
    }
    
    public GeoPipeline toEnvelope() {
    	return addPipe(new org.neo4j.gis.spatial.pipes.processing.Envelope());
    }
        
    public GeoPipeline toInteriorPoint() {
    	return addPipe(new InteriorPoint());
    }
    
    public GeoPipeline toStartPoint() {
    	return addPipe(new StartPoint(layer.getGeometryFactory()));
    }
    
    public GeoPipeline toEndPoint() {
    	return addPipe(new EndPoint(layer.getGeometryFactory()));
    }
    
    public GeoPipeline countPoints() {
    	return addPipe(new NumPoints());
    }
    
    public GeoPipeline union() {
    	return addPipe(new Union());
    }

    public GeoPipeline union(Geometry geometry) {
    	return addPipe(new Union(geometry));
    }
    
    public GeoPipeline unionAll() {
    	return addPipe(new UnionAll());
    }
    
    public GeoPipeline intersect(Geometry geometry) {
    	return addPipe(new Intersection(geometry));
    }
    
    public GeoPipeline intersectAll() {
    	return addPipe(new IntersectAll());
    }
    
    public GeoPipeline difference(Geometry geometry) {
    	return addPipe(new Difference(geometry));
    }
    
    public GeoPipeline symDifference(Geometry geometry) {
    	return addPipe(new SymDifference(geometry));
    }
    
    public GeoPipeline simplifyWithDouglasPeucker(double distanceTolerance) {
    	return addPipe(new SimplifyWithDouglasPeucker(distanceTolerance));
    }
    
    public GeoPipeline simplifyPreservingTopology(double distanceTolerance) {
    	return addPipe(new SimplifyPreservingTopology(distanceTolerance));
    }
    
    public GeoPipeline applyAffineTransform(AffineTransformation t) {
    	return addPipe(new ApplyAffineTransformation(t));
    }
    
    public GeoPipeline densify(double distanceTolerance) {
    	return addPipe(new Densify(distanceTolerance));
    }
    
    public GeoPipeline calculateArea() {
    	return addPipe(new Area());
    }
    
    public GeoPipeline calculateLength() {
    	return addPipe(new Length());
    }
    
    public GeoPipeline calculateOrthodromicLength() {
    	return addPipe(new OrthodromicLength(layer.getCoordinateReferenceSystem()));
    }    
    
    public GeoPipeline calculateDistance(Geometry reference) {
    	return addPipe(new Distance(reference));
    }

    public GeoPipeline calculateOrthodromicDistance(Coordinate reference) {
    	return addPipe(new OrthodromicDistance(reference));
    } 
    
    public GeoPipeline getDimension() {
    	return addPipe(new Dimension());
    }
    
    public GeoPipeline getGeometryType() {
    	return addPipe(new GeometryType());
    }
    
    public GeoPipeline getNumGeometries() {
    	return addPipe(new NumGeometries());
    }
        
    public GeoPipeline createJson() {
    	return addPipe(new GeoJSON());
    }
    
    public GeoPipeline createWellKnownText() {
    	return addPipe(new WellKnownText());
    }
    
    public GeoPipeline createKML() {
    	return addPipe(new KeyholeMarkupLanguage());
    }
    
    public GeoPipeline createGML() {
    	return addPipe(new GML());
    }
    
    public GeoPipeline propertyFilter(String key, Object value) {
    	return addPipe(new FilterProperty(key, value));
    }    
    
    public GeoPipeline propertyFilter(String key, Object value, FilterPipe.Filter comparison) {
    	return addPipe(new FilterProperty(key, value, comparison));    	
    }

    public GeoPipeline propertyNotNullFilter(String key) {
    	return addPipe(new FilterPropertyNotNull(key));    	
    }
    
    public GeoPipeline propertyNullFilter(String key) {
    	return addPipe(new FilterPropertyNull(key));    	
    }    
    
    public GeoPipeline cqlFilter(String cql) throws CQLException {
    	return addPipe(new FilterCQL(layer, cql));
    }
    
    public GeoPipeline intersectionFilter(Geometry geometry) {
    	return addPipe(new FilterIntersect(geometry));
    }
    
    public GeoPipeline windowIntersectionFilter(double xmin, double ymin, double xmax, double ymax) {
    	return addPipe(new FilterIntersectWindow(layer.getGeometryFactory(), xmin, ymin, xmax, ymax));
    }
    
    public GeoPipeline containFilter(Geometry geometry) {
    	return addPipe(new FilterContain(geometry));
    }
    
    public GeoPipeline coverFilter(Geometry geometry) {
    	return addPipe(new FilterCover(geometry));
    }
    
    public GeoPipeline coveredByFilter(Geometry geometry) {
    	return addPipe(new FilterCoveredBy(geometry));
    }    
    
    public GeoPipeline crossFilter(Geometry geometry) {
    	return addPipe(new FilterCross(geometry));
    }        

    public GeoPipeline disjointFilter(Geometry geometry) {
    	return addPipe(new FilterDisjoint(geometry));
    }        
    
    public GeoPipeline emptyFilter() {
    	return addPipe(new FilterEmpty());
    }            

    public GeoPipeline equalFilter(Geometry geometry) {
    	return addPipe(new FilterEqual(geometry));
    }        
    
    public GeoPipeline relationFilter(Geometry geometry, String intersectionPattern) {
    	return addPipe(new FilterInRelation(geometry, intersectionPattern));
    }        

    public GeoPipeline validFilter() {
    	return addPipe(new FilterValid());
    }            

    public GeoPipeline invalidFilter() {
    	return addPipe(new FilterInvalid());
    }            
    
    public GeoPipeline overlapFilter(Geometry geometry) {
    	return addPipe(new FilterOverlap(geometry));
    }

    public GeoPipeline touchFilter(Geometry geometry) {
    	return addPipe(new FilterTouch(geometry));
    }
    
    public GeoPipeline withinFilter(Geometry geometry) {
    	return addPipe(new FilterWithin(geometry));
    }    
    
    public GeoPipeline groupByDensityIslands(double density) {
    	return addPipe(new DensityIslands(density));
    }
        
    public GeoPipeline extractPoints() {
    	return addPipe(new ExtractPoints(layer.getGeometryFactory()));
    }    
    
    public GeoPipeline extractGeometries() {
    	return addPipe(new ExtractGeometries());
    }
    
    /**
     * Warning: this method should *not* be used with pipes that extract many items from a single item 
     * or with pipes that group many items into fewer items.
     */
    public List<SpatialDatabaseRecord> toSpatialDatabaseRecordList() {
    	List<SpatialDatabaseRecord> result = new ArrayList<SpatialDatabaseRecord>();
    	try {
	    	while (true) {
	    		result.add(next().getRecord());
	    	}
    	} catch (NoSuchElementException e) {}
    	return result;
    }
    
    /**
     * Warning: this method should *not* be used with pipes that extract many items from a single item 
     * or with pipes that group many items into fewer items.
     */    
    public List<Node> toNodeList() {
    	List<Node> result = new ArrayList<Node>();
    	try {
	    	while (true) {
	    		result.add(next().getRecord().getGeomNode());	    		
	    	}
    	} catch (NoSuchElementException e) {}    	
    	return result;

    }
}