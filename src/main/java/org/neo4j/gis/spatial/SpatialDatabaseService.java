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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;


/**
 * @author Davide Savazzi
 */
public class SpatialDatabaseService implements Constants {

	// Constructor
	
	public SpatialDatabaseService(GraphDatabaseService database) {
		this.database = database;
	}

	
	// Public methods
	
	public String[] getLayerNames() {
		List<String> names = new ArrayList<String>();
		
		Node refNode = database.getReferenceNode();
		for (Relationship relationship : refNode.getRelationships(SpatialRelationshipTypes.LAYER, Direction.OUTGOING)) {
			Node layerNode = relationship.getEndNode();
			names.add((String) layerNode.getProperty(PROP_LAYER));
		}
		
		return names.toArray(new String[names.size()]);
	}
	
	public Layer getLayer(String name) {
		return getLayer(name, false);
	}

    public Layer getLayer(String name, boolean createIfNotExists) {
        Transaction tx = database.beginTx();
        try {
            Node refNode = database.getReferenceNode();
            for (Relationship relationship : refNode.getRelationships(SpatialRelationshipTypes.LAYER, Direction.OUTGOING)) {
                Node layerNode = relationship.getEndNode();
                if (name.equals(layerNode.getProperty(PROP_LAYER))) {
                    return new Layer(this, name, layerNode);
                }
            }
            Layer layer = null;
            if (createIfNotExists) {
                layer = createLayer(name);
            }
            tx.success();
            return layer;
        } finally {
            tx.finish();
        }
    }
	
	public Layer findLayerContainingGeometryNode(Node geometryNode) {
		Relationship indexRel = geometryNode.getSingleRelationship(SpatialRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING);
		if (indexRel == null) return null;
		
		Node startNode = null;
		while (indexRel != null) {
			startNode = indexRel.getStartNode();
			indexRel = startNode.getSingleRelationship(SpatialRelationshipTypes.RTREE_CHILD, Direction.INCOMING);
		}
		
		indexRel = startNode.getSingleRelationship(SpatialRelationshipTypes.RTREE_ROOT, Direction.INCOMING);	
		if (indexRel != null) {
			startNode = indexRel.getStartNode();
			if (startNode.hasProperty(PROP_LAYER)) {
				return new Layer(this, (String) startNode.getProperty(PROP_LAYER), startNode);
			}
		}
		
		return null;
	}
	
	public boolean containsLayer(String name) {
		return getLayer(name) != null;
	}
	
	public Layer createLayer(String name) {
		return createLayer(name, WKBGeometryEncoder.class);
	}
	
	public Layer createLayer(String name, Class geometryEncoderClass) {
        Transaction tx = database.beginTx();
        try {
            if (containsLayer(name))
                throw new SpatialDatabaseException("Layer " + name + " already exists");

            Node layerNode = database.createNode();
            layerNode.setProperty(PROP_LAYER, name);
            layerNode.setProperty(PROP_CREATIONTIME, System.currentTimeMillis());
            layerNode.setProperty(PROP_GEOMENCODER, geometryEncoderClass.getCanonicalName());

            Node refNode = database.getReferenceNode();
            refNode.createRelationshipTo(layerNode, SpatialRelationshipTypes.LAYER);
            Layer layer = new Layer(this, name, layerNode);
            tx.success();
            return layer;
        } finally {
            tx.finish();
        }
	}
		
	public void deleteLayer(String name, Listener monitor) {
        Layer layer = getLayer(name);
        if (layer == null) throw new SpatialDatabaseException("Layer " + name + " does not exist");
        
		layer.delete(monitor);
	}
	
	public GraphDatabaseService getDatabase() {
		return database;
	}
	
	
	// Attributes
	
	private GraphDatabaseService database;
}
