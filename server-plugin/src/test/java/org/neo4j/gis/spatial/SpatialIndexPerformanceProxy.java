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

import java.util.Map;
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
 * A proxy wrapping the normal spatial index for the purpose of performance measurements.
 */
public class SpatialIndexPerformanceProxy implements LayerIndexReader {

	private final LayerIndexReader spatialIndex;

	public SpatialIndexPerformanceProxy(LayerIndexReader spatialIndex) {
		this.spatialIndex = spatialIndex;
	}

	@Override
	public void init(Transaction tx, IndexManager indexManager, Layer layer) {
		if (layer != getLayer()) {
			throw new IllegalArgumentException("Cannot change layer associated with this index");
		}
	}

	@Override
	public Layer getLayer() {
		return spatialIndex.getLayer();
	}

	@Override
	public boolean isEmpty(Transaction tx) {
		long start = System.currentTimeMillis();
		boolean result = spatialIndex.isEmpty(tx);
		long stop = System.currentTimeMillis();
		System.out.println("# exec time(count): " + (stop - start) + "ms");
		return result;
	}

	@Override
	public int count(Transaction tx) {
		long start = System.currentTimeMillis();
		int count = spatialIndex.count(tx);
		long stop = System.currentTimeMillis();
		System.out.println("# exec time(count): " + (stop - start) + "ms");
		return count;
	}

	public Iterable<Node> getAllGeometryNodes(Transaction tx) {
		return spatialIndex.getAllIndexedNodes(tx);
	}

	@Override
	public EnvelopeDecoder getEnvelopeDecoder() {
		return spatialIndex.getEnvelopeDecoder();
	}

	@Override
	public Envelope getBoundingBox(Transaction tx) {
		long start = System.currentTimeMillis();
		Envelope result = spatialIndex.getBoundingBox(tx);
		long stop = System.currentTimeMillis();
		System.out.println("# exec time(getBoundingBox()): " + (stop - start) + "ms");
		return result;
	}

	@Override
	public boolean isNodeIndexed(Transaction tx, String nodeId) {
		long start = System.currentTimeMillis();
		boolean result = spatialIndex.isNodeIndexed(tx, nodeId);
		long stop = System.currentTimeMillis();
		System.out.println("# exec time(isNodeIndexed(" + nodeId + ")): " + (stop - start) + "ms");
		return result;
	}

	@Override
	public Iterable<Node> getAllIndexedNodes(Transaction tx) {
		long start = System.currentTimeMillis();
		Iterable<Node> result = spatialIndex.getAllIndexedNodes(tx);
		long stop = System.currentTimeMillis();
		System.out.println("# exec time(getAllIndexedNodes()): " + (stop - start) + "ms");
		return result;
	}

	@Override
	public SearchResults searchIndex(Transaction tx, SearchFilter filter) {
		long start = System.currentTimeMillis();
		SearchResults results = spatialIndex.searchIndex(tx, filter);
		long stop = System.currentTimeMillis();
		System.out.println("# exec time(executeSearch(" + filter + ")): " + (stop - start) + "ms");
		return results;
	}

	@Override
	public void addMonitor(TreeMonitor monitor) {

	}

	@Override
	public void configure(Map<String, Object> config) {

	}

	@Override
	public SearchRecords search(Transaction tx, SearchFilter filter) {
		long start = System.currentTimeMillis();
		SearchRecords results = spatialIndex.search(tx, filter);
		long stop = System.currentTimeMillis();
		System.out.println("# exec time(executeSearch(" + filter + ")): " + (stop - start) + "ms");
		return results;
	}
}
