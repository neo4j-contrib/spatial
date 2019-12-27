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
package org.neo4j.gis.spatial;

import java.util.*;

import org.neo4j.gis.spatial.index.LayerIndexReader;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.EnvelopeDecoder;
import org.neo4j.gis.spatial.rtree.TreeMonitor;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.rtree.filter.SearchResults;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.graphdb.Node;

/**
 * @author Davide Savazzi
 */
public class FakeIndex implements LayerIndexReader, Constants {

	public FakeIndex(Layer layer) {
		init(layer);
	}

	public void init(Layer layer) {
		this.layer = layer;
	}

	public Layer getLayer() {
		return layer;
	}
	
	public int count() {
		int count = 0;
		
        // @TODO: Consider adding a count method to Layer or SpatialDataset to allow for
        // optimization of this if this kind of code gets used elsewhere
		for (@SuppressWarnings("unused") Node node: layer.getDataset().getAllGeometryNodes()) {
		    count++;
		}
		
		return count;
	}

	public boolean isEmpty() {
		return count() == 0;
	}
	
	public Envelope getBoundingBox() {
		Envelope bbox = null;
		
		GeometryEncoder geomEncoder = layer.getGeometryEncoder();
		for (Node node: layer.getDataset().getAllGeometryNodes()) {
			if (bbox == null) {
				bbox = geomEncoder.decodeEnvelope(node);
			} else {
				bbox.expandToInclude(geomEncoder.decodeEnvelope(node));
			}
		}
		
		return bbox;
	}

	public SpatialDatabaseRecord get(Long geomNodeId) {
		return new SpatialDatabaseRecord(layer, layer.getSpatialDatabase().getDatabase().getNodeById(geomNodeId));
	}
	
    public List<SpatialDatabaseRecord> get(Set<Long> geomNodeIds) {
    	List<SpatialDatabaseRecord> results = new ArrayList<>();

    	for (Long geomNodeId : geomNodeIds) {
    		results.add(get(geomNodeId));
    	}
    	
    	return results;
    }	
	
	public Iterable<Node> getAllIndexedNodes() {
		return layer.getIndex().getAllIndexedNodes();
	}

	@Override
	public EnvelopeDecoder getEnvelopeDecoder() {
		return layer.getGeometryEncoder();
	}


	@Override
	public boolean isNodeIndexed(Long nodeId) {
		// TODO
		return true;
	}
	
	
	// Attributes
	
	private Layer layer;

	private class NodeFilter implements Iterable<Node>, Iterator<Node> {
		private SearchFilter filter;
		private Node nextNode;
		private Iterator<Node> nodes;

		NodeFilter(SearchFilter filter, Iterable<Node> nodes) {
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
			while(nodes.hasNext()){
				Node node = nodes.next();
				if(filter.evaluate(node)){
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
	public SearchResults searchIndex(SearchFilter filter) {
		return new SearchResults(new NodeFilter(filter, layer.getDataset().getAllGeometryNodes()));
	}

	@Override
	public void addMonitor( TreeMonitor monitor )
	{

	}

	@Override
	public void configure(Map<String, Object> config) {

	}

	@Override
	public SearchRecords search(SearchFilter filter) {
		return new SearchRecords(layer, searchIndex(filter));
	}
}
