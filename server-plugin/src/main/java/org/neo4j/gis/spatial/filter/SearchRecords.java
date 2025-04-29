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
package org.neo4j.gis.spatial.filter;

import java.util.Iterator;
import javax.annotation.Nonnull;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.rtree.filter.SearchResults;
import org.neo4j.graphdb.Node;

public class SearchRecords implements Iterable<SpatialDatabaseRecord>, Iterator<SpatialDatabaseRecord> {

	private final SearchResults results;
	private final Iterator<Node> nodeIterator;
	private final Layer layer;

	public SearchRecords(Layer layer, SearchResults results) {
		this.layer = layer;
		this.results = results;
		nodeIterator = results.iterator();
	}

	@Override
	@Nonnull
	public Iterator<SpatialDatabaseRecord> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		return nodeIterator.hasNext();
	}

	@Override
	public SpatialDatabaseRecord next() {
		return new SpatialDatabaseRecord(layer, nodeIterator.next());
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Cannot remove from results");
	}

	public int count() {
		return results.count();
	}

}
