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

import java.util.HashMap;
import java.util.Iterator;

import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.Search;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;

import com.tinkerpop.pipes.AbstractPipe;

public class FilterAttributes<S, E> extends
		AbstractPipe<SpatialDatabaseRecord, SpatialDatabaseRecord> {

	private final Layer layer;
	private Search search;
	private Iterator<SpatialDatabaseRecord> results;

	public FilterAttributes(final Layer layer,
			final HashMap<String, String> attributes) {
		this.layer = layer;
		this.search = new SearchAttributes(attributes);
	}

	public SpatialDatabaseRecord processNextStart() {
		if (this.results == null) {
			layer.getIndex().executeSearch(search);
			this.results = search.getResults().iterator();
		}
		return this.results.next();
	}

}
