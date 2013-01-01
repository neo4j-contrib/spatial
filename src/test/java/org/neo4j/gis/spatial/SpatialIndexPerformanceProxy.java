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
package org.neo4j.gis.spatial;

import java.util.List;
import java.util.Set;

import org.neo4j.collections.rtree.Envelope;
import org.neo4j.collections.rtree.EnvelopeDecoder;
import org.neo4j.collections.rtree.filter.SearchFilter;
import org.neo4j.collections.rtree.filter.SearchResults;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.graphdb.Node;


/**
 * @author Davide Savazzi
 */
public class SpatialIndexPerformanceProxy implements LayerIndexReader {

    // Constructor

    public SpatialIndexPerformanceProxy(LayerIndexReader spatialIndex) {
        this.spatialIndex = spatialIndex;
    }

    // Public methods

    public Layer getLayer() {
    	return spatialIndex.getLayer();
    }

    public boolean isEmpty() {
        long start = System.currentTimeMillis();    	
    	boolean result = spatialIndex.isEmpty();
    	long stop = System.currentTimeMillis();
        System.out.println("# exec time(count): " + (stop - start) + "ms");
        return result;    	
    }
    
    public int count() {
        long start = System.currentTimeMillis();
        int count = spatialIndex.count();
        long stop = System.currentTimeMillis();
        System.out.println("# exec time(count): " + (stop - start) + "ms");
        return count;
    }
    
    public SpatialDatabaseRecord get(Long geomNodeId) {
        long start = System.currentTimeMillis();
        SpatialDatabaseRecord result = spatialIndex.get(geomNodeId);
        long stop = System.currentTimeMillis();
        System.out.println("# exec time(get(" + geomNodeId + ")): " + (stop - start) + "ms");    	
        return result;    	
    }
    
    public List<SpatialDatabaseRecord> get(Set<Long> geomNodeIds) {
        long start = System.currentTimeMillis();
        List<SpatialDatabaseRecord> result = spatialIndex.get(geomNodeIds);
        long stop = System.currentTimeMillis();
        System.out.println("# exec time(get(" + geomNodeIds + ")): " + (stop - start) + "ms");    	
        return result;
    }
 
    public Iterable<Node> getAllGeometryNodes() {
	    return spatialIndex.getAllIndexedNodes();
    }

	@Override
	public EnvelopeDecoder getEnvelopeDecoder() {
		return spatialIndex.getEnvelopeDecoder();
	}

	@Override
	public Envelope getBoundingBox() {
		long start = System.currentTimeMillis();
		Envelope result = spatialIndex.getBoundingBox();
        long stop = System.currentTimeMillis();
        System.out.println("# exec time(getBoundingBox()): " + (stop - start) + "ms");		
        return result;
	}

	@Override
	public boolean isNodeIndexed(Long nodeId) {
		long start = System.currentTimeMillis();
		boolean result = spatialIndex.isNodeIndexed(nodeId);
        long stop = System.currentTimeMillis();
        System.out.println("# exec time(isNodeIndexed(" + nodeId + ")): " + (stop - start) + "ms");		
        return result;		
	}

	@Override
	public Iterable<Node> getAllIndexedNodes() {
		long start = System.currentTimeMillis();
		Iterable<Node> result = getAllIndexedNodes();
        long stop = System.currentTimeMillis();
        System.out.println("# exec time(getAllIndexedNodes()): " + (stop - start) + "ms");		
        return result;
	}
    
	
    // Attributes

    private LayerIndexReader spatialIndex;


	@Override
	public SearchResults searchIndex(SearchFilter filter) {
        long start = System.currentTimeMillis();
        SearchResults results = spatialIndex.searchIndex(filter);
        long stop = System.currentTimeMillis();
        System.out.println("# exec time(executeSearch(" + filter + ")): " + (stop - start) + "ms");
		return results;
	}

	@Override
	public SearchRecords search(SearchFilter filter) {
        long start = System.currentTimeMillis();
        SearchRecords results = spatialIndex.search(filter);
        long stop = System.currentTimeMillis();
        System.out.println("# exec time(executeSearch(" + filter + ")): " + (stop - start) + "ms");
		return results;
	}
}