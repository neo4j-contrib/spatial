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
package org.neo4j.spatial.api;

import java.util.Iterator;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import org.neo4j.graphdb.Node;
import org.neo4j.spatial.api.layer.Layer;

public class SpatialRecords implements Iterable<SpatialRecord> {

	private final SearchResults results;
	private final BiFunction<Layer, Node, SpatialRecord> searchRecordsProducer;
	private final Layer layer;

	public SpatialRecords(Layer layer, SearchResults results,
			BiFunction<Layer, Node, SpatialRecord> searchRecordsProducer) {
		this.layer = layer;
		this.results = results;
		this.searchRecordsProducer = searchRecordsProducer;
	}

	@Override
	@Nonnull
	public Iterator<SpatialRecord> iterator() {
		return new SpatialRecordIterator();
	}

	private class SpatialRecordIterator implements Iterator<SpatialRecord> {

		private final Iterator<Node> nodeIterator;

		public SpatialRecordIterator() {
			this.nodeIterator = results.iterator();
		}

		@Override
		public boolean hasNext() {
			return nodeIterator.hasNext();
		}

		@Override
		public SpatialRecord next() {
			return searchRecordsProducer.apply(layer, nodeIterator.next());
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Cannot remove from results");
		}
	}
}
