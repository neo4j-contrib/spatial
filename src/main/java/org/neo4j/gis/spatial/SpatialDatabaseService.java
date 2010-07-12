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
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Traverser.Order;


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
	    //TODO: Remove transaction for newer versions of Neo4j (which do not need transactions for read access)
        Transaction tx = database.beginTx();
        try {
            Node refNode = database.getReferenceNode();
            for (Relationship relationship : refNode.getRelationships(SpatialRelationshipTypes.LAYER, Direction.OUTGOING)) {
                Node layerNode = relationship.getEndNode();
                if (name.equals(layerNode.getProperty(PROP_LAYER))) {
                    return Layer.makeLayer(this, layerNode);
                }
            }
            tx.success();
            return null;
        } finally {
            tx.finish();
        }
	}

    public Layer getOrCreateLayer(String name) {
        return getOrCreateLayer(name, WKBGeometryEncoder.class, Layer.class);
    }

    public Layer getOrCreateLayer(String name, Class<? extends GeometryEncoder> geometryEncoder, Class<? extends Layer> layerClass) {
        Transaction tx = database.beginTx();
        try {
            Layer layer = getLayer(name);
            if (layer == null) {
                layer = createLayer(name, geometryEncoder,layerClass);
            }
            tx.success();
            return layer;
        } finally {
            tx.finish();
        }
    }

    /**
     * This method will find the Layer when given a geometry node that this layer contains. It first
     * searches up the RTree index if it exists, and if it cannot find the layer node, it searches
     * back the NEXT_GEOM chain. This is the structure created by the default implementation of the
     * Layer class, so we should consider moving this to the Layer class, so it can be overridden by
     * other implementations that do not use that structure.
     * 
     * @TODO: Find a way to override this as we can override normal Layer with different graph structures.
     * 
     * @param geometryNode to start search
     * @return Layer object containing this geometry
     */
    public Layer findLayerContainingGeometryNode(Node geometryNode) {
        Node root = null;
        for (Node node : geometryNode.traverse(Order.DEPTH_FIRST, StopEvaluator.END_OF_GRAPH,
                ReturnableEvaluator.ALL_BUT_START_NODE, SpatialRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING,
                SpatialRelationshipTypes.RTREE_CHILD, Direction.INCOMING)) {
            root = node;
        }
        if (root != null) {
            return getLayerFromChild(root, SpatialRelationshipTypes.RTREE_ROOT);
        }
        System.out.println("Failed to find layer by following RTree index, will search back geometry list");
        for (Node node : geometryNode.traverse(Order.DEPTH_FIRST, StopEvaluator.END_OF_GRAPH,
                ReturnableEvaluator.ALL_BUT_START_NODE, SpatialRelationshipTypes.NEXT_GEOM, Direction.INCOMING)) {
            root = node;
        }
        if (root != null) {
            return getLayerFromChild(root, SpatialRelationshipTypes.NEXT_GEOM);
        }
        return null;
    }

    private Layer getLayerFromChild(Node child, RelationshipType relType) {
        Relationship indexRel = child.getSingleRelationship(relType, Direction.INCOMING);
        if (indexRel != null) {
            Node layerNode = indexRel.getStartNode();
            if (layerNode.hasProperty(PROP_LAYER)) {
                return Layer.makeLayer(this, layerNode);
            }
        }
        return null;
    }
	
	public boolean containsLayer(String name) {
		return getLayer(name) != null;
	}
	
    public Layer createLayer(String name) {
        return createLayer(name, WKBGeometryEncoder.class, Layer.class);
    }

    public Layer createLayer(String name, Class<? extends GeometryEncoder> geometryEncoderClass, Class<? extends Layer> layerClass) {
        Transaction tx = database.beginTx();
        try {
            if (containsLayer(name))
                throw new SpatialDatabaseException("Layer " + name + " already exists");

            Layer layer = Layer.makeLayer(this, name, geometryEncoderClass, layerClass);
            Node refNode = database.getReferenceNode();
            refNode.createRelationshipTo(layer.getLayerNode(), SpatialRelationshipTypes.LAYER);
            tx.success();
            return layer;
        } finally {
            tx.finish();
        }
	}
		
	public void deleteLayer(String name, Listener monitor) {
		Layer layer = null;
		
		Transaction tx = database.beginTx();
		try {
			layer = getLayer(name);
			
			tx.success();
		} finally {
			tx.finish();
		}
		
		if (layer == null) throw new SpatialDatabaseException("Layer " + name + " does not exist");

		layer.delete(monitor);
	}
	
	public GraphDatabaseService getDatabase() {
		return database;
	}
	
	
	// Attributes
	
	private GraphDatabaseService database;
}