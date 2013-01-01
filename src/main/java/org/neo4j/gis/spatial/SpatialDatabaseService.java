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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.collections.rtree.Listener;
import org.neo4j.collections.rtree.RTreeRelationshipTypes;
import org.neo4j.gis.spatial.encoders.Configurable;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Traverser.Order;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

/**
 * @author Davide Savazzi
 * @author Craig Taverner
 */
public class SpatialDatabaseService implements Constants {

    private Node spatialRoot;


    // Constructor
	
	public SpatialDatabaseService(GraphDatabaseService database) {
		this.database = database;
	}

	
	// Public methods

    private Node getOrCreateRootFrom(Node ref, RelationshipType relType) {
        Relationship rel = ref.getSingleRelationship(relType, Direction.OUTGOING);
        if (rel == null) {
            Transaction tx = database.beginTx();
            try {
                Node node = database.createNode();
                node.setProperty("type", "spatial");
                ref.createRelationshipTo(node, relType);
                tx.success();
                return node;
            } finally {
                tx.finish();
            }
        } else {
            return rel.getEndNode();
        }
    }

    protected Node getSpatialRoot() {
        if (spatialRoot == null) {
            spatialRoot = getOrCreateRootFrom(database.getReferenceNode(), SpatialRelationshipTypes.SPATIAL);
        }
        return spatialRoot;
    }

	public String[] getLayerNames() {
		List<String> names = new ArrayList<String>();
		
		for (Relationship relationship : getSpatialRoot().getRelationships(SpatialRelationshipTypes.LAYER, Direction.OUTGOING)) {
            Layer layer = DefaultLayer.makeLayerFromNode(this, relationship.getEndNode());
            if (layer instanceof DynamicLayer) {
            	names.addAll(((DynamicLayer)layer).getLayerNames());
            } else {
            	names.add(layer.getName());
            }
        }
        
		return names.toArray(new String[names.size()]);
	}
	
	public Layer getLayer(String name) {
		for (Relationship relationship : getSpatialRoot().getRelationships(SpatialRelationshipTypes.LAYER, Direction.OUTGOING)) {
			Node node = relationship.getEndNode();
			if (name.equals(node.getProperty(PROP_LAYER))) {
				return DefaultLayer.makeLayerFromNode(this, node);
			}
		}
		return getDynamicLayer(name);
	}

	public Layer getDynamicLayer(String name) {
		ArrayList<DynamicLayer> dynamicLayers = new ArrayList<DynamicLayer>();
		for (Relationship relationship : getSpatialRoot().getRelationships(SpatialRelationshipTypes.LAYER, Direction.OUTGOING)) {
			Node node = relationship.getEndNode();
			if (!node.getProperty(PROP_LAYER_CLASS, "").toString().startsWith("DefaultLayer")) {
				Layer layer = DefaultLayer.makeLayerFromNode(this, node);
				if (layer instanceof DynamicLayer) {
					dynamicLayers.add((DynamicLayer) DefaultLayer.makeLayerFromNode(this, node));
				}
			}
		}
		for (DynamicLayer layer : dynamicLayers) {
			for (String dynLayerName : layer.getLayerNames()) {
				if (name.equals(dynLayerName)) {
					return layer.getLayer(dynLayerName);
				}
			}
		}
		return null;
	}

	/**
	 * Convert a layer into a DynamicLayer. This will expose the ability to add
	 * views, or 'dynamic layers' to the layer.
	 * 
	 * @param layer
	 * @return new DynamicLayer version of the original layer
	 */
	public DynamicLayer asDynamicLayer(Layer layer) {
		if (layer instanceof DynamicLayer) {
			return (DynamicLayer) layer;
		} else {
			Transaction tx = database.beginTx();
			try {
				Node node = layer.getLayerNode();
				node.setProperty(PROP_LAYER_CLASS, DynamicLayer.class.getCanonicalName());
				tx.success();
				return (DynamicLayer) DefaultLayer.makeLayerFromNode(this, node);
			} finally {
				tx.finish();
			}
		}
	}

    public DefaultLayer getOrCreateDefaultLayer(String name) {
        return (DefaultLayer)getOrCreateLayer(name, WKBGeometryEncoder.class, DefaultLayer.class, "");
    }

	public EditableLayer getOrCreateEditableLayer(String name, String format, String propertyNameConfig) {
		Class<? extends GeometryEncoder> geClass = WKBGeometryEncoder.class;
		if (format != null && format.toUpperCase().startsWith("WKT")) {
			geClass = WKTGeometryEncoder.class;
		}
		return (EditableLayer) getOrCreateLayer(name, geClass, EditableLayerImpl.class, propertyNameConfig);
	}

    public EditableLayer getOrCreateEditableLayer(String name) {
        return getOrCreateEditableLayer(name, "WKB", "");
    }
    
    public EditableLayer getOrCreateEditableLayer(String name, String wktProperty) {
        return getOrCreateEditableLayer(name, "WKT", wktProperty);
    }

	public EditableLayer getOrCreatePointLayer(String name, String xProperty, String yProperty) {
		Layer layer = getLayer(name);
		if (layer == null) {
			String encoderConfig = null;
			if (xProperty != null && yProperty != null)
				encoderConfig = xProperty + ":" + yProperty;
			return (EditableLayer) createLayer(name, SimplePointEncoder.class, EditableLayerImpl.class, encoderConfig);
		} else if (layer instanceof EditableLayer) {
			return (EditableLayer) layer;
		} else {
			throw new SpatialDatabaseException("Existing layer '" + layer + "' is not of the expected type: " + EditableLayer.class);
		}
	}

    public Layer getOrCreateLayer(String name, Class< ? extends GeometryEncoder> geometryEncoder, Class< ? extends Layer> layerClass, String config) {
        Layer layer = getLayer(name);
        if (layer == null) {
            return createLayer(name, geometryEncoder, layerClass, config);
        } else if(layerClass == null || layerClass.isInstance(layer)) {
        	return layer;
        } else {
        	throw new SpatialDatabaseException("Existing layer '"+layer+"' is not of the expected type: "+layerClass);
        }
    }

    public Layer getOrCreateLayer(String name, Class< ? extends GeometryEncoder> geometryEncoder, Class< ? extends Layer> layerClass) {
        return getOrCreateLayer(name, geometryEncoder, layerClass, "");
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
                ReturnableEvaluator.ALL_BUT_START_NODE, RTreeRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING,
                RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING)) {
            root = node;
        }
        
        if (root != null) {
            return getLayerFromChild(root, RTreeRelationshipTypes.RTREE_ROOT);
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
                return DefaultLayer.makeLayerFromNode(this, layerNode);
            }
        }
        return null;
    }
	
	public boolean containsLayer(String name) {
		return getLayer(name) != null;
	}

    public Layer createWKBLayer(String name) {
        return createLayer(name, WKBGeometryEncoder.class, EditableLayerImpl.class);
    }

	public SimplePointLayer createSimplePointLayer(String name) {
		return (SimplePointLayer) createLayer(name, SimplePointEncoder.class, SimplePointLayer.class, null,
				org.geotools.referencing.crs.DefaultGeographicCRS.WGS84);
	}

	public SimplePointLayer createSimplePointLayer(String name, String xProperty, String yProperty) {
		return (SimplePointLayer) createLayer(name, SimplePointEncoder.class, SimplePointLayer.class, xProperty + ":" + yProperty,
				org.geotools.referencing.crs.DefaultGeographicCRS.WGS84);
	}

    public Layer createLayer(String name, Class<? extends GeometryEncoder> geometryEncoderClass, Class<? extends Layer> layerClass) {
    	return createLayer(name, geometryEncoderClass, layerClass, null);
    }

    public Layer createLayer(String name, Class<? extends GeometryEncoder> geometryEncoderClass, Class<? extends Layer> layerClass, String encoderConfig) {
    	return createLayer(name, geometryEncoderClass, layerClass, encoderConfig, null);
    }

	public Layer createLayer(String name, Class<? extends GeometryEncoder> geometryEncoderClass, Class<? extends Layer> layerClass,
			String encoderConfig, CoordinateReferenceSystem crs) {
        Transaction tx = database.beginTx();
        try {
            if (containsLayer(name))
                throw new SpatialDatabaseException("Layer " + name + " already exists");

            Layer layer = DefaultLayer.makeLayerAndNode(this, name, geometryEncoderClass, layerClass);
            getSpatialRoot().createRelationshipTo(layer.getLayerNode(), SpatialRelationshipTypes.LAYER);
			if (encoderConfig != null) {
				GeometryEncoder encoder = layer.getGeometryEncoder();
				if (encoder instanceof Configurable) {
					((Configurable) encoder).setConfiguration(encoderConfig);
					layer.getLayerNode().setProperty(PROP_GEOMENCODER_CONFIG, encoderConfig);
				} else {
					System.out.println("Warning: encoder configuration '" + encoderConfig
							+ "' passed to non-configurable encoder: " + geometryEncoderClass);
				}
			}
			if (crs != null && layer instanceof EditableLayer) {
				((EditableLayer) layer).setCoordinateReferenceSystem(crs);
			}
            tx.success();
            return layer;
        } finally {
            tx.finish();
        }
	}

    public void deleteLayer(String name, Listener monitor) {
        Layer layer = getLayer(name);
        if (layer == null)
            throw new SpatialDatabaseException("Layer " + name + " does not exist");

        layer.delete(monitor);
    }
	
	public GraphDatabaseService getDatabase() {
		return database;
	}
	
	
	// Attributes
	
	private GraphDatabaseService database;

	@SuppressWarnings("unchecked")
	public static int convertGeometryNameToType(String geometryName) {
		if(geometryName == null) return GTYPE_GEOMETRY;
		try {
			return convertJtsClassToGeometryType((Class<? extends Geometry>) Class.forName("com.vividsolutions.jts.geom."
					+ geometryName));
		} catch (ClassNotFoundException e) {
			System.err.println("Unrecognized geometry '" + geometryName + "': " + e);
			return GTYPE_GEOMETRY;
		}
	}

	public static String convertGeometryTypeToName(Integer geometryType) {
		return convertGeometryTypeToJtsClass(geometryType).getName().replace("com.vividsolutions.jts.geom.", "");
	}

	public static Class<? extends Geometry> convertGeometryTypeToJtsClass(Integer geometryType) {
		switch (geometryType) {
			case GTYPE_POINT: return Point.class;
			case GTYPE_LINESTRING: return LineString.class; 
			case GTYPE_POLYGON: return Polygon.class;
			case GTYPE_MULTIPOINT: return MultiPoint.class;
			case GTYPE_MULTILINESTRING: return MultiLineString.class;
			case GTYPE_MULTIPOLYGON: return MultiPolygon.class;
			default: return Geometry.class;
		}
	}

	public static int convertJtsClassToGeometryType(Class<? extends Geometry> jtsClass) {
		if (jtsClass.equals(Point.class)) {
			return GTYPE_POINT;
		} else if (jtsClass.equals(LineString.class)) {
			return GTYPE_LINESTRING;
		} else if (jtsClass.equals(Polygon.class)) {
			return GTYPE_POLYGON;
		} else if (jtsClass.equals(MultiPoint.class)) {
			return GTYPE_MULTIPOINT;
		} else if (jtsClass.equals(MultiLineString.class)) {
			return GTYPE_MULTILINESTRING;
		} else if (jtsClass.equals(MultiPolygon.class)) {
			return GTYPE_MULTIPOLYGON;
		} else {
			return GTYPE_GEOMETRY;
		}
	}

	/**
	 * Create a new layer from the results of a previous query. This actually
	 * copies the resulting geometries and their attributes into entirely new
	 * geometries using WKBGeometryEncoder. This means it is independent of the
	 * format of the original data. As a consequence it will have lost any
	 * domain specific capabilities of the original graph, if any. Use it only
	 * if you want a copy of the geometries themselves, and nothing more. One
	 * common use case would be to create a temporary layer of the results of a
	 * query than you wish to now export to a format that only supports
	 * geometries, like Shapefile, or the PNG images produced by the
	 * ImageExporter.
	 * 
	 * @param layerName
	 * @param results
	 * @return new Layer with copy of all geometries
	 */
	public Layer createResultsLayer(String layerName, List<SpatialDatabaseRecord> results) {
		EditableLayer layer = (EditableLayer) createWKBLayer(layerName);
		for (SpatialDatabaseRecord record : results) {
			layer.add(record.getGeometry());
		}
		return layer;
	}

}