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

import org.neo4j.collections.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.filter.SearchCQL;
import org.neo4j.gis.spatial.filter.SearchIntersectWindow;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.graphdb.Node;

import com.tinkerpop.pipes.filter.FilterPipe;
import com.tinkerpop.pipes.util.PipeHelper;
import com.vividsolutions.jts.geom.Envelope;

/**
 * This class can combine multiple filters together for generating a stream of
 * SpatialDatabaseRecords for use in the Geoprocessing pipeline.
 * 
 * @author craig
 */
public class GeoFilter implements SearchFilter {
	protected final Layer layer;
	private ArrayList<SearchFilter> filters;
	private SearchRecords results;

	/**
	 * An abstract implementation of a filter that cannot make use of the RTree
	 * index, and only filters on the geometries
	 */
	public abstract class GeometryFilter implements SearchFilter {

		@Override
		public boolean needsToVisit(org.neo4j.collections.rtree.Envelope envelope) {
			return true;
		}

	}

	public GeoFilter(Layer layer) {
		this.layer = layer;
		this.filters = new ArrayList<SearchFilter>();
	}

	public GeoFilter add(SearchFilter filter) {
		filters.add(filter);
		return this;
	}

	public GeoFilter all() {
		return this.add(new GeometryFilter() {
			@Override
			public boolean geometryMatches(Node geomNode) {
				return true;
			}
		});
	}

	public GeoFilter attributes(final String key, final String value, final FilterPipe.Filter filter) {
		return this.add(new GeometryFilter() {

			@Override
			public boolean geometryMatches(Node geomNode) {
				return geomNode.hasProperty(key) && PipeHelper.compareObjects(filter, geomNode.getProperty(key), value);
			}
		});
	}

	public GeoFilter bbox(double minLon, double minLat, double maxLon, double maxLat) {
		return this.add(new SearchIntersectWindow(layer, new Envelope(minLon, maxLon, minLat, maxLat)));
	}

	public GeoFilter cql(String cql) {
		return this.add(new SearchCQL(layer, cql));
	}

	public GeoProcessingPipeline<SpatialDatabaseRecord, SpatialDatabaseRecord> process() {
		return (GeoProcessingPipeline<SpatialDatabaseRecord, SpatialDatabaseRecord>) this.layer.process().start(getResults());
	}

	public SearchRecords getResults() {
		if (results == null) {
			results = layer.getIndex().search(this);
		}
		return results;
	}

	@Override
	public boolean needsToVisit(org.neo4j.collections.rtree.Envelope envelope) {
		for (SearchFilter filter : filters) {
			if (!filter.needsToVisit(envelope))
				return false;
		}
		return true;
	}

	@Override
	public boolean geometryMatches(Node geomNode) {
		for (SearchFilter filter : filters) {
			if (!filter.geometryMatches(geomNode))
				return false;
		}
		return true;
	}

}
