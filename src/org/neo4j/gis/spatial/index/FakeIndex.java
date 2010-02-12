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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.gis.spatial.SpatialIndexReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;


/**
 * @author Davide Savazzi
 */
public class FakeIndex extends AbstractSpatialIndex implements SpatialIndexReader {

	// Constructor
	
	public FakeIndex(GraphDatabaseService database) {
		this.database = database;
	}
		
	
	// Public methods
	
	public Envelope getBoundingBox() {
		Envelope bbox = null;
		for (Node node: database.getAllNodes()) {
			if (node.hasProperty(PROP_TYPE)) {
				if (bbox == null) {
					bbox = getEnvelope(node);
				} else {
					bbox.expandToInclude(getEnvelope(node));
				}
			}
		}
		return bbox;
	}

	public List<Node> search(Envelope searchEnvelope) {
		GeometryFactory geomFactory = new GeometryFactory();
		Geometry searchEnvelopeGeom = geomFactory.toGeometry(searchEnvelope);
		
		List<Node> result = new ArrayList<Node>();
		for (Node node: database.getAllNodes()) {
			if (node.hasProperty(PROP_TYPE)) {
				Envelope geomEnvelope = getEnvelope(node);
				
				if (searchEnvelope.contains(geomEnvelope)) {
					result.add(node);
				} else if (searchEnvelope.intersects(geomEnvelope)) {
					Geometry geom = getGeometry(geomFactory, node);
					if (searchEnvelopeGeom.intersects(geom)) {
						result.add(node);
					}
				}
			}
		}
		return result;
	}
	
	
	// Attributes
	
	private GraphDatabaseService database;
}
