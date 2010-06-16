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

import static org.neo4j.gis.spatial.GeometryUtils.getEnvelope;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Envelope;


/**
 * @author Davide Savazzi
 */
public class FakeIndex implements SpatialIndexReader, Constants {

	// Constructor
	
	public FakeIndex(GraphDatabaseService database, Layer layer) {
		this.database = database;
		this.layer = layer;
	}
		
	
	// Public methods
	
	public int count() {
		int count = 0;
		for (Node node: database.getAllNodes()) {
			if (nodeIsInLayer(node)) {
				count++;
			}
		}
		return count;
	}

	public boolean isEmpty() {
		return count() == 0;
	}
	
	public Envelope getLayerBoundingBox() {
		Envelope bbox = null;
		for (Node node: database.getAllNodes()) {
			if (nodeIsInLayer(node)) {
				if (bbox == null) {
					bbox = getEnvelope(node);
				} else {
					bbox.expandToInclude(getEnvelope(node));
				}
			}
		}
		return bbox;
	}

	public void executeSearch(Search search) {
        search.setGeometryFactory(layer.getGeometryFactory());
		for (Node node: database.getAllNodes()) {
			if (nodeIsInLayer(node)) {
				search.onIndexReference(node);
			}
		}
	}
	
	
	// Private methods
	
	private boolean nodeIsInLayer(Node geomNode) {
		return geomNode.hasProperty(PROP_WKB) && ((Long) geomNode.getProperty(PROP_LAYER)) == layer.getLayerNodeId();
	}
	
	
	// Attributes
	
	private GraphDatabaseService database;
	private Layer layer;
}