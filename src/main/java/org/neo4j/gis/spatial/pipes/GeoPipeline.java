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
import org.neo4j.gis.spatial.pipes.processing.Boundary;
import org.neo4j.gis.spatial.pipes.processing.Buffer;
import org.neo4j.gis.spatial.pipes.processing.Centroid;
import org.neo4j.gis.spatial.pipes.processing.ConvexHull;
import org.neo4j.gis.spatial.pipes.processing.DensityIslands;
import org.neo4j.gis.spatial.pipes.processing.Envelope;
import org.neo4j.gis.spatial.pipes.processing.ExtractPoints;
import org.neo4j.gis.spatial.pipes.processing.InteriorPoint;
import org.neo4j.gis.spatial.pipes.processing.NumPoints;
import org.neo4j.gis.spatial.pipes.processing.Union;

import com.tinkerpop.pipes.filter.FilterPipe;
import com.tinkerpop.pipes.util.FluentPipeline;
import com.tinkerpop.pipes.util.StartPipe;
import com.vividsolutions.jts.geom.Geometry;


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
    
    public GeoPipeline groupByDensityIslands(double density) {
    	return addPipe(new DensityIslands(density));
    }
    
    public GeoPipeline toEnvelope() {
    	return addPipe(new Envelope());
    }
    
    public GeoPipeline extractPoints() {
    	return addPipe(new ExtractPoints(layer.getGeometryFactory()));
    }
    
    public GeoPipeline toInteriorPoint() {
    	return addPipe(new InteriorPoint());
    }
    
    public GeoPipeline countPoints() {
    	return addPipe(new NumPoints());
    }
    
    public GeoPipeline toUnion() {
    	return addPipe(new Union());
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
    
	public int countResults() {
    	int count = 0;
    	for (@SuppressWarnings("unused") GeoPipeFlow flow : this) {
    		count++;
    	}
    	return count;
    }
}