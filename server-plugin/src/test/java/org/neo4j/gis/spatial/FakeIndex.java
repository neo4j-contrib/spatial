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
package org.neo4j.gis.spatial;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.index.LayerIndexReader;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.EnvelopeDecoder;
import org.neo4j.gis.spatial.rtree.TreeMonitor;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.rtree.filter.SearchResults;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * An in-memory index used for comparative benchmarking and testing
 */
public class FakeIndex implements LayerIndexReader, Constants {

	public FakeIndex(Layer layer, IndexManager indexManager) {
		init(null, indexManager, layer);
	}

	@Override
	public void init(Transaction ignored, IndexManager indexManager, Layer layer) {
		this.layer = layer;
	}

	@Override
	public Layer getLayer() {
		return layer;
	}

	@Override
	public int count(Transaction tx) {
		int count = 0;

		// @TODO: Consider adding a count method to Layer or SpatialDataset to allow for
		// optimization of this if this kind of code gets used elsewhere
		for (@SuppressWarnings("unused") Node node : layer.getDataset().getAllGeometryNodes(tx)) {
			count++;
		}

		return count;
	}

	@Override
	public boolean isEmpty(Transaction tx) {
		return count(tx) == 0;
	}

	@Override
	public Envelope getBoundingBox(Transaction tx) {
		Envelope bbox = null;

		GeometryEncoder geomEncoder = layer.getGeometryEncoder();
		for (Node node : layer.getDataset().getAllGeometryNodes(tx)) {
			if (bbox == null) {
				bbox = geomEncoder.decodeEnvelope(node);
			} else {
				bbox.expandToInclude(geomEncoder.decodeEnvelope(node));
			}
		}

		return bbox;
	}

	public SpatialDatabaseRecord get(Transaction tx, String geomNodeId) {
		return new SpatialDatabaseRecord(layer, tx.getNodeByElementId(geomNodeId));
	}

	public List<SpatialDatabaseRecord> get(Transaction tx, Set<String> geomNodeIds) {
		List<SpatialDatabaseRecord> results = new ArrayList<>();

		for (String geomNodeId : geomNodeIds) {
			results.add(get(tx, geomNodeId));
		}

		return results;
	}

	@Override
	public Iterable<Node> getAllIndexedNodes(Transaction tx) {
		return layer.getIndex().getAllIndexedNodes(tx);
	}

	@Override
	public EnvelopeDecoder getEnvelopeDecoder() {
		return layer.getGeometryEncoder();
	}


	@Override
	public boolean isNodeIndexed(Transaction tx, String nodeId) {
		// TODO
		return true;
	}

	// Attributes

	private Layer layer;

	private class NodeFilter implements Iterable<Node>, Iterator<Node> {

		private final Transaction tx;
		private final SearchFilter filter;
		private Node nextNode;
		private final Iterator<Node> nodes;

		NodeFilter(Transaction tx, SearchFilter filter, Iterable<Node> nodes) {
			this.tx = tx;
			this.filter = filter;
			this.nodes = nodes.iterator();
			nextNode = getNextNode();
		}

		@Override
		public Iterator<Node> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			return nextNode != null;
		}

		@Override
		public Node next() {
			Node currentNode = nextNode;
			nextNode = getNextNode();
			return currentNode;
		}

		private Node getNextNode() {
			Node nn = null;
			while (nodes.hasNext()) {
				Node node = nodes.next();
				if (filter.geometryMatches(tx, node)) {
					nn = node;
					break;
				}
			}
			return nn;
		}

		@Override
		public void remove() {
		}
	}

	@Override
	public SearchResults searchIndex(Transaction tx, SearchFilter filter) {
		return new SearchResults(new NodeFilter(tx, filter, layer.getDataset().getAllGeometryNodes(tx)));
	}

	@Override
	public void addMonitor(TreeMonitor monitor) {
	}

	@Override
	public void configure(Map<String, Object> config) {
	}

	@Override
	public SearchRecords search(Transaction tx, SearchFilter filter) {
		return new SearchRecords(layer, searchIndex(tx, filter));
	}
}
