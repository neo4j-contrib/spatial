/*
 * Copyright (c) 2010
 *
 * This program is free software: you can redistribute it and/or modify
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

import java.util.List;

import org.neo4j.gis.spatial.SpatialIndexReader;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Envelope;


/**
 * @author Davide Savazzi
 */
public class SpatialIndexPerformanceProxy implements SpatialIndexReader {

	// Constructor
	
	public SpatialIndexPerformanceProxy(SpatialIndexReader spatialIndex) {
		this.spatialIndex = spatialIndex;
	}
	
	
	// Public methods
	
	public Envelope getBoundingBox() {
		long start = System.currentTimeMillis();
		Envelope result = spatialIndex.getBoundingBox();
		long stop = System.currentTimeMillis();
		System.out.println("# exec time: " + (stop - start));
		return result;
	}

	public List<Node> search(Envelope bbox) {
		long start = System.currentTimeMillis();
		List<Node> result = spatialIndex.search(bbox);
		long stop = System.currentTimeMillis();
		System.out.println("# exec time: " + (stop - start));
		return result;
	}

	
	// Attributes
	
	private SpatialIndexReader spatialIndex;
}
