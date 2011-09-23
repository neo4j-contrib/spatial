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

import java.util.Iterator;

import org.geotools.filter.text.cql2.CQLException;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.pipes.filtering.FilterAttributes;
import org.neo4j.gis.spatial.pipes.filtering.FilterCQL;
import org.neo4j.gis.spatial.pipes.filtering.FilterIntersect;
import org.neo4j.gis.spatial.pipes.filtering.FilterIntersectWindow;
import org.neo4j.gis.spatial.pipes.processing.ApplyAffineTransformation;
import org.neo4j.gis.spatial.pipes.processing.Area;
import org.neo4j.gis.spatial.pipes.processing.Boundary;
import org.neo4j.gis.spatial.pipes.processing.Buffer;
import org.neo4j.gis.spatial.pipes.processing.Centroid;
import org.neo4j.gis.spatial.pipes.processing.ConvexHull;
import org.neo4j.gis.spatial.pipes.processing.Densify;
import org.neo4j.gis.spatial.pipes.processing.DensityIslands;
import org.neo4j.gis.spatial.pipes.processing.Difference;
import org.neo4j.gis.spatial.pipes.processing.Dimension;
import org.neo4j.gis.spatial.pipes.processing.Distance;
import org.neo4j.gis.spatial.pipes.processing.EndPoint;
import org.neo4j.gis.spatial.pipes.processing.Envelope;
import org.neo4j.gis.spatial.pipes.processing.ExtractPoints;
import org.neo4j.gis.spatial.pipes.processing.GML;
import org.neo4j.gis.spatial.pipes.processing.GeoJSON;
import org.neo4j.gis.spatial.pipes.processing.GeometryType;
import org.neo4j.gis.spatial.pipes.processing.InteriorPoint;
import org.neo4j.gis.spatial.pipes.processing.Intersection;
import org.neo4j.gis.spatial.pipes.processing.KeyholeMarkupLanguage;
import org.neo4j.gis.spatial.pipes.processing.Length;
import org.neo4j.gis.spatial.pipes.processing.LengthInMeters;
import org.neo4j.gis.spatial.pipes.processing.LengthInMiles;
import org.neo4j.gis.spatial.pipes.processing.NumGeometries;
import org.neo4j.gis.spatial.pipes.processing.NumPoints;
import org.neo4j.gis.spatial.pipes.processing.SimplifyPreservingTopology;
import org.neo4j.gis.spatial.pipes.processing.SimplifyWithDouglasPeucker;
import org.neo4j.gis.spatial.pipes.processing.StartPoint;
import org.neo4j.gis.spatial.pipes.processing.SymDifference;
import org.neo4j.gis.spatial.pipes.processing.Union;
import org.neo4j.gis.spatial.pipes.processing.WellKnownText;

import com.tinkerpop.pipes.filter.FilterPipe;
import com.tinkerpop.pipes.util.FluentPipeline;
import com.tinkerpop.pipes.util.StartPipe;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.util.AffineTransformation;


public class GeoPipeline extends FluentPipeline<GeoPipeFlow, GeoPipeFlow> {

	protected Layer layer;
	
	protected GeoPipeline(Layer layer) {
		this.layer = layer;
	}
	
	protected static StartPipe<GeoPipeFlow> createStartPipe(final SearchRecords records) {
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
	
    public static GeoPipeline start(Layer layer, SearchRecords records) {
    	GeoPipeline pipeline = new GeoPipeline(layer);
    	return (GeoPipeline) pipeline.add(createStartPipe(records));
    }
    
    public GeoPipeline addPipe(AbstractGeoPipe geoPipe) {
    	return (GeoPipeline) add(geoPipe);
    }

    public GeoPipeline toBoundary() {
    	return addPipe(new Boundary());
    }
    
    public GeoPipeline buffer(double distance) {
    	return addPipe(new Buffer(distance));
    }
    
    public GeoPipeline toCentroid() {
    	return addPipe(new Centroid());
    }

    public GeoPipeline toConvexHull() {
    	return addPipe(new ConvexHull());
    }
    
    public GeoPipeline toEnvelope() {
    	return addPipe(new Envelope());
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
    
    public GeoPipeline intersect(Geometry geometry) {
    	return addPipe(new Intersection(geometry));
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
    
    public GeoPipeline getDimension() {
    	return addPipe(new Dimension());
    }
    
    public GeoPipeline getGeometryType() {
    	return addPipe(new GeometryType());
    }
    
    public GeoPipeline getNumGeometries() {
    	return addPipe(new NumGeometries());
    }
    
    public GeoPipeline calculateLengthInMeters() {
    	return addPipe(new LengthInMeters(layer.getCoordinateReferenceSystem()));
    }
    
    public GeoPipeline calculateLengthInMiles() {
    	return addPipe(new LengthInMiles(layer.getCoordinateReferenceSystem()));
    }    
    
    public GeoPipeline calculateDistance(Geometry reference) {
    	return addPipe(new Distance(reference));
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
    
    public GeoPipeline filterByAttribute(String key, Object value) {
    	return addPipe(new FilterAttributes(key, value));
    }    
    
    public GeoPipeline filterByAttribute(String key, String value, FilterPipe.Filter comparison) {
    	return addPipe(new FilterAttributes(key, value, comparison));    	
    }

    public GeoPipeline filterByCQL(String cql) throws CQLException {
    	return addPipe(new FilterCQL(layer, cql));
    }
    
    public GeoPipeline filterByIntersection(Geometry geometry) {
    	return addPipe(new FilterIntersect(geometry));
    }
    
    public GeoPipeline filterByWindowIntersection(double xmin, double ymin, double xmax, double ymax) {
    	return addPipe(new FilterIntersectWindow(layer.getGeometryFactory(), xmin, ymin, xmax, ymax));
    }
    
    public GeoPipeline groupByDensityIslands(double density) {
    	return addPipe(new DensityIslands(density));
    }
        
    public GeoPipeline extractPoints() {
    	return addPipe(new ExtractPoints(layer.getGeometryFactory()));
    }    
    
	public int countResults() {
    	int count = 0;
    	for (@SuppressWarnings("unused") GeoPipeFlow flow : this) {
    		count++;
    	}
    	return count;
    }
}