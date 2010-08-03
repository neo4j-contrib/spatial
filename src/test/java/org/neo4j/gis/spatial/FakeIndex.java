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
package org.neo4j.gis.spatial;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Envelope;


/**
 * @author Davide Savazzi
 */
public class FakeIndex implements SpatialIndexReader, Constants {

	// Constructor
	
	public FakeIndex(Layer layer) {
		this.layer = layer;
	}
		
	
	// Public methods
	
	public int count() {
		int count = 0;
        // @TODO: Consider adding a count method to Layer or SpatialDataset to allow for
        // optimization of this if this kind of code gets used elsewhere
		for (@SuppressWarnings("unused") Node node: layer.getAllGeometryNodes()) {
		    count++;
		}
		return count;
	}

	public boolean isEmpty() {
		return count() == 0;
	}
	
	public Envelope getLayerBoundingBox() {
		Envelope bbox = null;
		for (Node node: layer.getAllGeometryNodes()) {
			if (bbox == null) {
				bbox = layer.getGeometryEncoder().decodeEnvelope(node);
			} else {
				bbox.expandToInclude(layer.getGeometryEncoder().decodeEnvelope(node));
			}
		}
		return bbox;
	}

	public SpatialDatabaseRecord get(Long geomNodeId) {
		return new SpatialDatabaseRecord(layer.getName(), 
				layer.getGeometryEncoder(), 
				layer.getCoordinateReferenceSystem(), 
				layer.getExtraPropertyNames(),
				layer.getSpatialDatabase().getDatabase().getNodeById(geomNodeId));
	}
	
    public List<SpatialDatabaseRecord> get(Set<Long> geomNodeIds) {
    	List<SpatialDatabaseRecord> results = new ArrayList<SpatialDatabaseRecord>();

    	for (Long geomNodeId : geomNodeIds) {
    		results.add(get(geomNodeId));
    	}
    	
    	return results;
    }	
	
	public void executeSearch(Search search) {
        search.setLayer(layer);
		for (Node node: layer.getAllGeometryNodes()) {
			search.onIndexReference(node);
		}
	}
	
	// Attributes
	private Layer layer;
}