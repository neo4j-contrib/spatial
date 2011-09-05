/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.neo4j.graphdb.Node;

import org.neo4j.collections.rtree.Envelope;
import org.neo4j.collections.rtree.EnvelopeDecoder;
import org.neo4j.collections.rtree.Search;


/**
 * @author Davide Savazzi
 */
public class SpatialIndexPerformanceProxy implements LayerIndexReader {

    // Constructor

    public SpatialIndexPerformanceProxy(LayerIndexReader spatialIndex) {
        this.spatialIndex = spatialIndex;
    }

    // Public methods

    public Envelope getLayerBoundingBox() {
        long start = System.currentTimeMillis();
        Envelope result = spatialIndex.getBoundingBox();
        long stop = System.currentTimeMillis();
        System.out.println("# exec time(getLayerBoundingBox): " + (stop - start) + "ms");
        return result;
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
    
    public void executeSearch(Search search) {
        long start = System.currentTimeMillis();
        spatialIndex.executeSearch(search);
        long stop = System.currentTimeMillis();
        System.out.println("# exec time(executeSearch(" + search + ")): " + (stop - start) + "ms");
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
}