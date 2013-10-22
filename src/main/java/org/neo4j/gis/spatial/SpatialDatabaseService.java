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
import org.neo4j.gis.spatial.encoders.Configurable;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
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

    //DONE
	public SpatialDatabaseService(GraphDatabaseService database) {
		this.database = database;
	}

    public Transaction beginTx() {
        return database.beginTx();
    }
	
	// Public methods

    //DONE
    private Node getOrCreateRootFrom(Node ref, RelationshipType relType) {
        Relationship rel = ref.getSingleRelationship(relType, Direction.OUTGOING);
        if (rel == null) {
            Node node = database.createNode();
            node.setProperty("type", "spatial");
            ref.createRelationshipTo(node, relType);
            return node;
        } else {
            return rel.getEndNode();
        }
    }

    //DONE
    private Node getSpatialRoot() {
        if (spatialRoot == null) {
            spatialRoot = getOrCreateRootFrom(database.getReferenceNode(), SpatialRelationshipTypes.SPATIAL);
        }
        return spatialRoot;
    }

    //DONE
	public String[] getLayerNames() {
		List<String> names = new ArrayList<String>();
        try ( Transaction tx = database.beginTx() ) {
            for (Relationship relationship : getSpatialRoot().getRelationships(SpatialRelationshipTypes.LAYER, Direction.OUTGOING)) {
                Layer layer = DefaultLayer.makeLayerFromNode(this, relationship.getEndNode());
                if (layer instanceof DynamicLayer) {
                    names.addAll(((DynamicLayer)layer).getLayerNames());
                } else {
                    names.add(layer.getName());
                }
            }
            tx.success();
        }
		return names.toArray(new String[names.size()]);
	}

    //DONE
	public Layer getLayer(String name) {
        try ( Transaction tx = database.beginTx() ) {
            Layer layer =  getLayerFromGraph(name);
            tx.success();
            return layer;
        }
	}

    //DONE - centralized logic to allow for transactions on all public methods
    private Layer getLayerFromGraph(String name) {
        for (Relationship relationship : getSpatialRoot().getRelationships(SpatialRelationshipTypes.LAYER, Direction.OUTGOING)) {
            Node node = relationship.getEndNode();
            if (name.equals(node.getProperty(PROP_LAYER))) {
                return DefaultLayer.makeLayerFromNode(this, node);
            }
        }
        return getDynamicLayer(name);
    }

    //DONE
	public Layer getDynamicLayer(String name) {
        Layer toReturn = null;
        try ( Transaction tx = database.beginTx() ) {
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
                        toReturn =  layer.getLayer(dynLayerName);
                    }
                }
            }
            tx.success();
        }
		return toReturn;
	}

	/**
	 * Convert a layer into a DynamicLayer. This will expose the ability to add
	 * views, or 'dynamic layers' to the layer.
	 * 
	 * @param layer
	 * @return new DynamicLayer version of the original layer
	 */
    //DONE
	public DynamicLayer asDynamicLayer(Layer layer) {
		if (layer instanceof DynamicLayer) {
			return (DynamicLayer) layer;
		} else {
            try ( Transaction tx = database.beginTx() ) {
				Node node = layer.getLayerNode();
				node.setProperty(PROP_LAYER_CLASS, DynamicLayer.class.getCanonicalName());
				tx.success();
				return (DynamicLayer) DefaultLayer.makeLayerFromNode(this, node);
			}
		}
	}

    //DONE
    public DefaultLayer getOrCreateDefaultLayer(String name) {
        return (DefaultLayer)getOrCreateLayer(name, WKBGeometryEncoder.class, DefaultLayer.class, "");
    }

    //DONE
	public EditableLayer getOrCreateEditableLayer(String name, String format, String propertyNameConfig) {
		Class<? extends GeometryEncoder> geClass = WKBGeometryEncoder.class;
		if (format != null && format.toUpperCase().startsWith("WKT")) {
			geClass = WKTGeometryEncoder.class;
		}
		return (EditableLayer) getOrCreateLayer(name, geClass, EditableLayerImpl.class, propertyNameConfig);
	}

    //DONE
    public EditableLayer getOrCreateEditableLayer(String name) {
        return getOrCreateEditableLayer(name, "WKB", "");
    }

    //DONE
    public EditableLayer getOrCreateEditableLayer(String name, String wktProperty) {
        return getOrCreateEditableLayer(name, "WKT", wktProperty);
    }

    //DONE
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

    //DONE
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

    //DONE
    public Layer getOrCreateLayer(String name, Class< ? extends GeometryEncoder> geometryEncoder, Class< ? extends Layer> layerClass) {
        return getOrCreateLayer(name, geometryEncoder, layerClass, "");
    }

    //DONE
	public boolean containsLayer(String name) {
		return getLayerFromGraph(name) != null;
	}

    //DONE
    public Layer createWKBLayer(String name) {
        return createLayer(name, WKBGeometryEncoder.class, EditableLayerImpl.class);
    }

    //DONE
	public SimplePointLayer createSimplePointLayer(String name) {
		return (SimplePointLayer) createLayer(name, SimplePointEncoder.class, SimplePointLayer.class, null,
				org.geotools.referencing.crs.DefaultGeographicCRS.WGS84);
	}

    //DONE
	public SimplePointLayer createSimplePointLayer(String name, String xProperty, String yProperty) {
		return (SimplePointLayer) createLayer(name, SimplePointEncoder.class, SimplePointLayer.class, xProperty + ":" + yProperty,
				org.geotools.referencing.crs.DefaultGeographicCRS.WGS84);
	}

    //DONE
    public Layer createLayer(String name, Class<? extends GeometryEncoder> geometryEncoderClass, Class<? extends Layer> layerClass) {
    	return createLayer(name, geometryEncoderClass, layerClass, null);
    }

    //DONE
    public Layer createLayer(String name, Class<? extends GeometryEncoder> geometryEncoderClass, Class<? extends Layer> layerClass, String encoderConfig) {
    	return createLayer(name, geometryEncoderClass, layerClass, encoderConfig, null);
    }

    //DONE
	public Layer createLayer(String name, Class<? extends GeometryEncoder> geometryEncoderClass, Class<? extends Layer> layerClass,
			String encoderConfig, CoordinateReferenceSystem crs) {
        try(Transaction tx = database.beginTx()) {
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
        }
	}

    //DONE
    public void deleteLayer(String name, Listener monitor) {
        Layer layer = getLayerFromGraph(name);
        if (layer == null)
            throw new SpatialDatabaseException("Layer " + name + " does not exist");

        layer.delete(monitor);
    }

    //DONE
	public GraphDatabaseService getDatabase() {
		return database;
	}
	
	
	// Attributes
	
	private GraphDatabaseService database;

	@SuppressWarnings("unchecked")
    //DONE
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
    //DONE
	public static String convertGeometryTypeToName(Integer geometryType) {
		return convertGeometryTypeToJtsClass(geometryType).getName().replace("com.vividsolutions.jts.geom.", "");
	}
    //DONE
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
    //DONE
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