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
package org.neo4j.gis.spatial.index;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.EnvelopeDecoder;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.rtree.TreeMonitor;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.rtree.filter.SearchResults;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;

/**
 * 2D Point data can be indexed against a 1D backing index using a 2D->1D mapper.
 * Some mappers, like Geohash mapping, can produce a String value encoding the 2D space
 * such as two points close together in 1D space (the strings) are also likely to be
 * close together in 2D space. The Geohash has the same space filling curve as the Z-Order
 * index, which uses a Long value in 1D space. A space filling curve with more efficient
 * space use is the Hilbert space filling curve where the probability fo being close in
 * 2D space when close in 1D is even higher.
 * <p>
 * All three of these mappings will be backed by the ExplicitIndexBackedPointIndex of
 * type `E` where `E` is either a string (for geohash) or a long (for zorder and hilber).
 *
 * @param <E> either a String or a Long depending on whether the index is geohash or space-filling curve.
 */
public abstract class ExplicitIndexBackedPointIndex<E> implements LayerIndexReader, SpatialIndexWriter {

	protected Layer layer;
	private PropertyEncodingNodeIndex<E> index;
	private final ExplicitIndexBackedMonitor monitor = new ExplicitIndexBackedMonitor();

	protected abstract String indexTypeName();

	@Override
	public void init(Transaction tx, IndexManager indexManager, Layer layer) {
		this.layer = layer;
		String indexName = "_SpatialIndex_" + indexTypeName() + "_" + layer.getName();
		Label label = Label.label("SpatialIndex_" + indexTypeName() + "_" + layer.getName());
		this.index = new PropertyEncodingNodeIndex<>(indexManager, indexName, label, indexName.toLowerCase());
		this.index.initialize(tx);
	}

	@Override
	public Layer getLayer() {
		return layer;
	}

	@Override
	public SearchRecords search(Transaction tx, SearchFilter filter) {
		return new SearchRecords(layer, searchIndex(tx, filter));
	}

	@Override
	public void add(Transaction tx, Node geomNode) {
		index.add(geomNode, getIndexValueFor(tx, geomNode));
	}

	protected abstract E getIndexValueFor(Transaction tx, Node geomNode);

	@Override
	public void add(Transaction tx, List<Node> geomNodes) {
		for (Node node : geomNodes) {
			add(tx, node);
		}
	}

	@Override
	public void remove(Transaction tx, String geomNodeId, boolean deleteGeomNode, boolean throwExceptionIfNotFound) {
		try {
			Node geomNode = tx.getNodeByElementId(geomNodeId);
			if (geomNode != null) {
				index.remove(geomNode);
				if (deleteGeomNode) {
					try (var relationships = geomNode.getRelationships()) {
						for (Relationship rel : relationships) {
							rel.delete();
						}
					}
					geomNode.delete();
				}
			}
		} catch (NotFoundException nfe) {
			if (throwExceptionIfNotFound) {
				throw nfe;
			}
		}
	}

	@Override
	public void removeAll(Transaction tx, boolean deleteGeomNodes, Listener monitor) {
		if (deleteGeomNodes) {
			for (Node node : getAllIndexedNodes(tx)) {
				remove(tx, node.getElementId(), true, true);
			}
		}
		index.delete(tx);
	}

	@Override
	public void clear(Transaction tx, Listener monitor) {
		removeAll(tx, false, monitor);
	}

	@Override
	public EnvelopeDecoder getEnvelopeDecoder() {
		return layer.getGeometryEncoder();
	}

	@Override
	public boolean isEmpty(Transaction tx) {
		return true;
	}

	@Override
	public int count(Transaction ignore) {
		return 0;
	}

	@Override
	public Envelope getBoundingBox(Transaction tx) {
		return null;
	}

	@Override
	public boolean isNodeIndexed(Transaction tx, String nodeId) {
		return false;
	}

	@Override
	public Iterable<Node> getAllIndexedNodes(Transaction tx) {
		return index.queryAll(tx);
	}

	@Override
	public SearchResults searchIndex(Transaction tx, SearchFilter filter) {
		Iterator<Node> indexHits = index.query(tx, searcherFor(tx, filter));
		return new SearchResults(() -> new FilteredIndexIterator(tx, indexHits, filter));
	}

	private class FilteredIndexIterator implements Iterator<Node> {

		private final Transaction tx;
		private final Iterator<Node> inner;
		private final SearchFilter filter;
		private Node next = null;

		private FilteredIndexIterator(Transaction tx, Iterator<Node> inner, SearchFilter filter) {
			this.tx = tx;
			this.inner = inner;
			this.filter = filter;
			prefetch();
		}

		private void prefetch() {
			next = null;
			while (inner.hasNext()) {
				Node node = inner.next();
				if (filter.geometryMatches(tx, node)) {
					next = node;
					monitor.hit();
					break;
				}
				monitor.miss();
			}
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public Node next() {
			Node node = next;
			if (node == null) {
				throw new NoSuchElementException(); // GeoPipes relies on this behaviour instead of hasNext()
			}

			prefetch();
			return node;
		}
	}

	/**
	 * Create a class capable of performing a specific search based on a custom 2D to 1D conversion.
	 */
	protected abstract Neo4jIndexSearcher searcherFor(Transaction tx, SearchFilter filter);

	public interface Neo4jIndexSearcher {

		Iterator<Node> search(KernelTransaction ktx, Label label, String propertyKey);
	}

	@Override
	public void addMonitor(TreeMonitor monitor) {

	}

	public ExplicitIndexBackedMonitor getMonitor() {
		return this.monitor;
	}

	@Override
	public void configure(Map<String, Object> config) {

	}

}
