/*
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
package org.neo4j.gis.spatial.index;

import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.EnvelopeDecoder;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.rtree.TreeMonitor;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.rtree.filter.SearchResults;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class LayerGeohashPointIndex implements LayerIndexReader, SpatialIndexWriter {

    private Layer layer;
    private String indexName;
    private Index<Node> index;
    private GraphDatabaseService graph;

    @Override
    public void init(Layer layer) {
        this.layer = layer;
        this.indexName = "_Spatial_Geohash_Index_" + layer.getName();
        graph = layer.getSpatialDatabase().getDatabase();
        try(Transaction tx = graph.beginTx()) {
            index = graph.index().forNodes(this.indexName);
            tx.success();
        }
    }

    @Override
    public Layer getLayer() {
        return layer;
    }

    @Override
    public SearchRecords search(SearchFilter filter) {
        throw new UnsupportedOperationException("Not implemented: search(SearchFilter)");
    }

    @Override
    public void add(Node geomNode) {
        throw new UnsupportedOperationException("Not implemented: add(Node)");
    }

    @Override
    public void add(List<Node> geomNodes) {
        for (Node node : geomNodes) {
            add(node);
        }
    }

    @Override
    public void remove(long geomNodeId, boolean deleteGeomNode, boolean throwExceptionIfNotFound) {
        throw new UnsupportedOperationException("Not implemented: remove(...)");
    }

    @Override
    public void removeAll(boolean deleteGeomNodes, Listener monitor) {
        throw new UnsupportedOperationException("Not implemented: removeAll(...)");
    }

    @Override
    public void clear(Listener monitor) {

    }

    @Override
    public EnvelopeDecoder getEnvelopeDecoder() {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public int count() {
        return 0;
    }

    @Override
    public Envelope getBoundingBox() {
        return null;
    }

    @Override
    public boolean isNodeIndexed(Long nodeId) {
        return false;
    }

    @Override
    public Iterable<Node> getAllIndexedNodes() {
        return Iterables.empty();
    }

    @Override
    public SearchResults searchIndex(SearchFilter filter) {
        throw new UnsupportedOperationException("Not implemented: searchIndex(SearchFilter)");
    }

    @Override
    public void addMonitor(TreeMonitor monitor) {

    }

    @Override
    public void configure(Map<String, Object> config) {

    }

}
