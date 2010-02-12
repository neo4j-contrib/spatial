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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;


/**
 * @author Davide Savazzi
 */
public class SpatialDatabaseService implements Constants {

	// Constructor
	
	public SpatialDatabaseService(GraphDatabaseService database) {
		this.database = database;
	}

	
	// Public methods
	
	public Layer getLayer(String name) {
		Node refNode = database.getReferenceNode();
		for (Relationship relationship : refNode.getRelationships(SpatialRelationshipTypes.LAYER, Direction.OUTGOING)) {
			Node layerNode = relationship.getEndNode();
			if (name.equals(layerNode.getProperty(PROP_LAYER))) {
				return new Layer(database, layerNode);
			}
		}
		
		return null;
	}
	
	public boolean containsLayer(String name) {
		return getLayer(name) != null;
	}
	
	public Layer createLayer(String name) {
		Node layerNode = database.createNode();
		layerNode.setProperty(PROP_LAYER, name);
		layerNode.setProperty(PROP_CREATIONTIME, System.currentTimeMillis());
		
		Node refNode = database.getReferenceNode();
		refNode.createRelationshipTo(layerNode, SpatialRelationshipTypes.LAYER);
		return new Layer(database, layerNode);
	}
	
	
	// Attributes
	
	private GraphDatabaseService database;
}