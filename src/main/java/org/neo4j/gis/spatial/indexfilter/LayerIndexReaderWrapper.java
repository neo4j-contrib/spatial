/**
 * Copyright (c) 2010-2013 "Neo Technology,"
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
package org.neo4j.gis.spatial.indexfilter;

import java.util.List;
import java.util.Set;

import org.neo4j.collections.rtree.Envelope;
import org.neo4j.collections.rtree.EnvelopeDecoder;
import org.neo4j.collections.rtree.filter.SearchFilter;
import org.neo4j.collections.rtree.filter.SearchResults;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.LayerIndexReader;
import org.neo4j.gis.spatial.LayerTreeIndexReader;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.graphdb.Node;


/**
 * This class wraps a SpatialIndexReader instance, passing through all calls
 * transparently. This is a class that should not be used as it, as it provides
 * not additional functionality. However extending this class allows for
 * wrapping an existing index and modifying its behaviour through overriding
 * only specific methods. For example, override the excecuteSearch method with a
 * modification to the search parameter.
 */
public class LayerIndexReaderWrapper implements LayerIndexReader {

	protected LayerTreeIndexReader index;

	public LayerIndexReaderWrapper(LayerTreeIndexReader index) {
		this.index = index;
	}

	@Override
	public Layer getLayer() {
		return index.getLayer();
	}

	@Override
	public EnvelopeDecoder getEnvelopeDecoder() {
		return index.getEnvelopeDecoder();
	}

	@Override
	public int count() {
		return index.count();
	}

	@Override
	public boolean isNodeIndexed(Long nodeId) {
		return index.isNodeIndexed(nodeId);
	}

	@Override
	public SpatialDatabaseRecord get(Long geomNodeId) {
		return index.get(geomNodeId);
	}

	@Override
	public List<SpatialDatabaseRecord> get(Set<Long> geomNodeIds) {
		return index.get(geomNodeIds);
	}

	@Override
	public Envelope getBoundingBox() {
		return index.getBoundingBox();
	}

	@Override
	public boolean isEmpty() {
		return index.isEmpty();
	}

	@Override
	public Iterable<Node> getAllIndexedNodes() {
		return index.getAllIndexedNodes();
	}

	@Override
	public SearchResults searchIndex(SearchFilter filter) {
		return index.searchIndex(filter);
	}

	@Override
	public SearchRecords search(SearchFilter filter) {
		return index.search(filter);
	}
}
