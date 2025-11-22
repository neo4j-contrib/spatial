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
package org.neo4j.gis.spatial.indexfilter;

import java.util.Map;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.spatial.api.Envelope;
import org.neo4j.spatial.api.EnvelopeDecoder;
import org.neo4j.spatial.api.SearchFilter;
import org.neo4j.spatial.api.SearchResults;
import org.neo4j.spatial.api.SpatialRecords;
import org.neo4j.spatial.api.index.IndexManager;
import org.neo4j.spatial.api.index.LayerTreeIndexReader;
import org.neo4j.spatial.api.index.SpatialIndexReader;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.api.monitoring.TreeMonitor;


/**
 * This class wraps a SpatialIndexReader instance, passing through all calls
 * transparently. This is a class that should not be used as is, as it provides
 * no additional functionality. However, extending this class allows for
 * wrapping an existing index and modifying its behaviour through overriding
 * only specific methods. For example, override the executeSearch method with a
 * modification to the search parameter.
 */
public class LayerIndexReaderWrapper implements SpatialIndexReader {

	protected final LayerTreeIndexReader index;

	public LayerIndexReaderWrapper(LayerTreeIndexReader index) {
		this.index = index;
	}

	@Override
	public void init(Transaction tx, IndexManager indexManager, Layer layer, boolean readOnly) {
		if (layer != getLayer()) {
			throw new IllegalArgumentException("Cannot change layer associated with this index");
		}
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
	public int count(Transaction tx) {
		return index.count(tx);
	}

	@Override
	public boolean isNodeIndexed(Transaction tx, String nodeId) {
		return index.isNodeIndexed(tx, nodeId);
	}

	@Override
	public Envelope getBoundingBox(Transaction tx) {
		return index.getBoundingBox(tx);
	}

	@Override
	public boolean isEmpty(Transaction tx) {
		return index.isEmpty(tx);
	}

	@Override
	public Iterable<Node> getAllIndexedNodes(Transaction tx) {
		return index.getAllIndexedNodes(tx);
	}

	@Override
	public SearchResults searchIndex(Transaction tx, SearchFilter filter) {
		return index.searchIndex(tx, filter);
	}

	@Override
	public void addMonitor(TreeMonitor monitor) {

	}

	@Override
	public void configure(Map<String, Object> config) {

	}

	@Override
	public SpatialRecords search(Transaction tx, SearchFilter filter) {
		return index.search(tx, filter);
	}
}
