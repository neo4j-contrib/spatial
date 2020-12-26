/*
 * Copyright (c) 2010-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.EnvelopeDecoder;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.rtree.TreeMonitor;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.rtree.filter.SearchResults;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public abstract class ExplicitIndexBackedPointIndex<E> implements LayerIndexReader, SpatialIndexWriter {

    protected Layer layer;
    private NodeIndex<E> index;
    private final ExplicitIndexBackedMonitor monitor = new ExplicitIndexBackedMonitor();

    protected abstract String indexTypeName();

    @Override
    public void init(Transaction tx, Layer layer) {
        this.layer = layer;
        String indexName = "_Spatial_" + indexTypeName() + "_Index_" + layer.getName();
        this.index = new NodeIndex<>(indexName);
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
        index.add(geomNode, indexTypeName(), getIndexValueFor(tx, geomNode));
    }

    protected abstract E getIndexValueFor(Transaction tx, Node geomNode);

    @Override
    public void add(Transaction tx, List<Node> geomNodes) {
        for (Node node : geomNodes) {
            add(tx, node);
        }
    }

    @Override
    public void remove(Transaction tx, long geomNodeId, boolean deleteGeomNode, boolean throwExceptionIfNotFound) {
        try {
            Node geomNode = tx.getNodeById(geomNodeId);
            if (geomNode != null) {
                index.remove(geomNode);
                if (deleteGeomNode) {
                    for (Relationship rel : geomNode.getRelationships()) {
                        rel.delete();
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

            for (Node node : getAllIndexedNodes()) {
                remove(tx, node.getId(), true, true);
            }
        }
        index.delete();
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
    public boolean isNodeIndexed(Transaction tx, Long nodeId) {
        return false;
    }

    @Override
    public Iterable<Node> getAllIndexedNodes() {
        return index.query(indexTypeName(), "*");
    }

    @Override
    public SearchResults searchIndex(Transaction tx, SearchFilter filter) {
        Iterable<Node> indexHits = index.query(indexTypeName(), queryStringFor(tx, filter));
        return new SearchResults(() -> new FilteredIndexIterator(tx, indexHits.iterator(), filter));
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
                } else {
                    monitor.miss();
                }
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
            } else {
                prefetch();
                return node;
            }
        }
    }

    protected abstract String queryStringFor(Transaction tx, SearchFilter filter);

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
