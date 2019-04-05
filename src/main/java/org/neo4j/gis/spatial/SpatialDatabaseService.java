/*
 * Copyright (c) 2010-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j Spatial.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial;

import org.locationtech.jts.geom.*;
import org.geotools.referencing.crs.AbstractCRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.neo4j.gis.spatial.encoders.Configurable;
import org.neo4j.gis.spatial.encoders.NativePointEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.index.*;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.utilities.LayerUtilities;
import org.neo4j.gis.spatial.utilities.ReferenceNodes;
import org.neo4j.graphdb.*;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Davide Savazzi
 * @author Craig Taverner
 */
public class SpatialDatabaseService implements Constants {

    private Node spatialRoot;

	public SpatialDatabaseService(GraphDatabaseService database) {
		this.database = database;
	}

    private Node getOrCreateRootFrom(Node ref, RelationshipType relType) {
        try (Transaction tx = database.beginTx()) {
            Relationship rel = ref.getSingleRelationship(relType, Direction.OUTGOING);
            Node node;
            if (rel == null) {
                node = database.createNode();
                node.setProperty("type", "spatial");
                ref.createRelationshipTo(node, relType);
            } else {
                node = rel.getEndNode();
            }
            tx.success();
            return node;
        }
    }

    private Node getSpatialRoot() {
        if (spatialRoot == null || !isValid(spatialRoot)) {
            spatialRoot = ReferenceNodes.getReferenceNode(database, "spatial_root");
        }
        return spatialRoot;
    }

    private boolean isValid(Node node) {
        if (node==null) return false;
        try {
            node.getPropertyKeys().iterator().hasNext();
            return true;
        } catch(NotFoundException nfe) {
            return false;
        }
    }

    public String[] getLayerNames() {
		List<String> names = new ArrayList<String>();
		
		try (Transaction tx = getDatabase().beginTx()) {
			for (Relationship relationship : getSpatialRoot().getRelationships(SpatialRelationshipTypes.LAYER,
					Direction.OUTGOING)) {
				Layer layer = LayerUtilities.makeLayerFromNode(this, relationship.getEndNode());
				if (layer instanceof DynamicLayer) {
					names.addAll(((DynamicLayer) layer).getLayerNames());
				} else {
					names.add(layer.getName());
				}
			}
			tx.success();
		}
        
		return names.toArray(new String[names.size()]);
	}
	
	public Layer getLayer(String name) {
        try (Transaction tx = getDatabase().beginTx()) {
            for (Relationship relationship : getSpatialRoot().getRelationships(SpatialRelationshipTypes.LAYER, Direction.OUTGOING)) {
                Node node = relationship.getEndNode();
                if (name.equals(node.getProperty(PROP_LAYER))) {
                    Layer layer = LayerUtilities.makeLayerFromNode(this, node);
                    tx.success();
                    return layer;
                }
            }
            Layer layer = getDynamicLayer(name);
            tx.success();
            return layer;
        }
	}

	public Layer getDynamicLayer(String name) {
		ArrayList<DynamicLayer> dynamicLayers = new ArrayList<DynamicLayer>();
		for (Relationship relationship : getSpatialRoot().getRelationships(SpatialRelationshipTypes.LAYER, Direction.OUTGOING)) {
			Node node = relationship.getEndNode();
			if (!node.getProperty(PROP_LAYER_CLASS, "").toString().startsWith("DefaultLayer")) {
				Layer layer = LayerUtilities.makeLayerFromNode(this, node);
				if (layer instanceof DynamicLayer) {
					dynamicLayers.add((DynamicLayer) LayerUtilities.makeLayerFromNode(this, node));
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
	 * @return new DynamicLayer version of the original layer
	 */
	public DynamicLayer asDynamicLayer(Layer layer) {
		if (layer instanceof DynamicLayer) {
			return (DynamicLayer) layer;
		} else {
			try (Transaction tx = database.beginTx()) {
				Node node = layer.getLayerNode();
				node.setProperty(PROP_LAYER_CLASS, DynamicLayer.class.getCanonicalName());
				tx.success();
				return (DynamicLayer) LayerUtilities.makeLayerFromNode(this, node);
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

	public static final String RTREE_INDEX_NAME = "rtree";
	public static final String GEOHASH_INDEX_NAME = "geohash";

	public Class<? extends LayerIndexReader> resolveIndexClass(String index) {
		if (index == null) {
			return LayerRTreeIndex.class;
		}
		switch (index.toLowerCase()) {
			case RTREE_INDEX_NAME:
				return LayerRTreeIndex.class;
			case GEOHASH_INDEX_NAME:
				return LayerGeohashPointIndex.class;
			case "zorder":
				return LayerZOrderPointIndex.class;
			case "hilbert":
				return LayerHilbertPointIndex.class;
		}
		throw new IllegalArgumentException("Unknown index: " + index);
	}

	public EditableLayer getOrCreateSimplePointLayer(String name, String index, String xProperty, String yProperty) {
		return getOrCreatePointLayer(name, resolveIndexClass(index), SimplePointEncoder.class, xProperty, yProperty);
	}

	public EditableLayer getOrCreateNativePointLayer(String name, String index, String locationProperty) {
		return getOrCreatePointLayer(name, resolveIndexClass(index), SimplePointEncoder.class, locationProperty);
	}

	public EditableLayer getOrCreatePointLayer(String name, Class<? extends LayerIndexReader> indexClass, Class<? extends GeometryEncoder> encoderClass, String... encoderConfig) {
		Layer layer = getLayer(name);
		if (layer == null) {
			return (EditableLayer) createLayer(name, encoderClass, SimplePointLayer.class, indexClass, makeEncoderConfig(encoderConfig), DefaultGeographicCRS.WGS84);
		} else if (layer instanceof EditableLayer) {
			return (EditableLayer) layer;
		} else {
			throw new SpatialDatabaseException("Existing layer '" + layer + "' is not of the expected type: " + EditableLayer.class);
		}
	}

    public Layer getOrCreateLayer(String name, Class< ? extends GeometryEncoder> geometryEncoder, Class< ? extends Layer> layerClass, String config) {
        try (Transaction tx = database.beginTx()) {
            Layer layer = getLayer(name);
            if (layer == null) {
                layer = createLayer(name, geometryEncoder, layerClass, null, config);
            } else if (!(layerClass == null || layerClass.isInstance(layer))) {
                throw new SpatialDatabaseException("Existing layer '"+layer+"' is not of the expected type: "+layerClass);
            }
            tx.success();
            return layer;
        }
    }

    public Layer getOrCreateLayer(String name, Class< ? extends GeometryEncoder> geometryEncoder, Class< ? extends Layer> layerClass) {
        return getOrCreateLayer(name, geometryEncoder, layerClass, "");
    }

    /**
     * This method will find the Layer when given a geometry node that this layer contains. This method
     * used to make use of knowledge of the RTree, traversing backwards up the tree to find the layer node, which is fast. However, for reasons of clean abstraction, 
     * this has been refactored to delegate the logic to the layer, so that each layer can do this in an
     * implementation specific way. Now we simply iterate through the layers datasets and the first one
     * to return true on the SpatialDataset.containsGeometryNode(Node) method is returned.
     * 
     * We can consider removing this method for a few reasons:
     * * It is non-deterministic if more than one layer contains the same geometry
     * * None of the current code appears to use this method
     * 
     * @param geometryNode to start search
     * @return Layer object containing this geometry
     */
    public Layer findLayerContainingGeometryNode(Node geometryNode) {
        for (String layerName: getLayerNames()) {
        	Layer layer = getLayer(layerName);
        	if (layer.getDataset().containsGeometryNode(geometryNode)) {
        		return layer;
        	}
        }
        return null;
    }

    private Layer getLayerFromChild(Node child, RelationshipType relType) {
        Relationship indexRel = child.getSingleRelationship(relType, Direction.INCOMING);
        if (indexRel != null) {
            Node layerNode = indexRel.getStartNode();
            if (layerNode.hasProperty(PROP_LAYER)) {
                return LayerUtilities.makeLayerFromNode(this, layerNode);
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
		return createSimplePointLayer(name, null);
	}

	public SimplePointLayer createSimplePointLayer(String name, String xProperty, String yProperty) {
		return createSimplePointLayer(name, xProperty, yProperty, null);
	}

	public SimplePointLayer createSimplePointLayer(String name, String... xybProperties) {
		return createPointLayer(name, LayerRTreeIndex.class, SimplePointEncoder.class, xybProperties);
	}

	public SimplePointLayer createNativePointLayer(String name) {
		return createNativePointLayer(name, null);
	}

	public SimplePointLayer createNativePointLayer(String name, String locationProperty, String bboxProperty) {
		return createNativePointLayer(name, locationProperty, bboxProperty, null);
	}

	public SimplePointLayer createNativePointLayer(String name, String... encoderConfig) {
		return createPointLayer(name, LayerRTreeIndex.class, NativePointEncoder.class, encoderConfig);
	}

	public SimplePointLayer createPointLayer(String name, Class<? extends LayerIndexReader> indexClass, Class<? extends GeometryEncoder> encoderClass, String... encoderConfig) {
        return (SimplePointLayer) createLayer(name, encoderClass, SimplePointLayer.class, indexClass,
                makeEncoderConfig(encoderConfig), org.geotools.referencing.crs.DefaultGeographicCRS.WGS84);
    }

    public String makeEncoderConfig(String... args) {
        StringBuilder sb = new StringBuilder();
        if(args != null) {
            for (String arg : args) {
                if (arg != null) {
                    if (sb.length() > 0)
                        sb.append(":");
                    sb.append(arg);
                }
            }
        }
        return sb.toString();
    }

    public Layer createLayer(String name, Class<? extends GeometryEncoder> geometryEncoderClass, Class<? extends Layer> layerClass) {
    	return createLayer(name, geometryEncoderClass, layerClass, null, null);
    }

    public Layer createLayer(String name, Class<? extends GeometryEncoder> geometryEncoderClass,
                             Class<? extends Layer> layerClass, Class<? extends LayerIndexReader> indexClass,
                             String encoderConfig) {
        return createLayer(name, geometryEncoderClass, layerClass, indexClass, encoderConfig, null);
    }

    public Layer createLayer(String name, Class<? extends GeometryEncoder> geometryEncoderClass,
                             Class<? extends Layer> layerClass, Class<? extends LayerIndexReader> indexClass,
                             String encoderConfig, CoordinateReferenceSystem crs) {
		try (Transaction tx = database.beginTx()) {
			if (containsLayer(name))
				throw new SpatialDatabaseException("Layer " + name + " already exists");

			Layer layer = LayerUtilities.makeLayerAndNode(this, name, geometryEncoderClass, layerClass, indexClass);
			getSpatialRoot().createRelationshipTo(layer.getLayerNode(), SpatialRelationshipTypes.LAYER);
			if (encoderConfig != null && encoderConfig.length() > 0) {
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

    public void deleteLayer(String name, Listener monitor) {
        Layer layer = getLayer(name);
        if (layer == null)
            throw new SpatialDatabaseException("Layer " + name + " does not exist");

        try (Transaction tx = database.beginTx()) {
            layer.delete(monitor);
            tx.success();
        }
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
			return convertJtsClassToGeometryType((Class<? extends Geometry>) Class.forName("org.locationtech.jts.geom."
					+ geometryName));
		} catch (ClassNotFoundException e) {
			System.err.println("Unrecognized geometry '" + geometryName + "': " + e);
			return GTYPE_GEOMETRY;
		}
	}

	public static String convertGeometryTypeToName(Integer geometryType) {
		return convertGeometryTypeToJtsClass(geometryType).getName().replace("org.locationtech.jts.geom.", "");
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
	 * @param layerName name of new layer to create
	 * @param results collection of SpatialDatabaseRecords to add to new layer
	 * @return new Layer with copy of all geometries
	 */
	public Layer createResultsLayer(String layerName, List<SpatialDatabaseRecord> results) {
		EditableLayer layer = (EditableLayer) createWKBLayer(layerName);
		for (SpatialDatabaseRecord record : results) {
			layer.add(record.getGeometry());
		}
		return layer;
	}


    /**
     * Support mapping a String (ex: 'SimplePoint') to the respective GeometryEncoder and Layer classes
     * to allow for more streamlined method for creating Layers
     * This was added to help support Spatial Cypher project.
     */
    public static class RegisteredLayerType {
        String typeName;
        Class< ? extends GeometryEncoder> geometryEncoder;
        Class< ? extends Layer> layerClass;
		Class<? extends LayerIndexReader> layerIndexClass;
        String defaultConfig;
        org.geotools.referencing.crs.AbstractCRS crs;

        RegisteredLayerType(String typeName, Class<? extends GeometryEncoder> geometryEncoder,
							Class<? extends Layer> layerClass, AbstractCRS crs,
							Class<? extends LayerIndexReader> layerIndexClass, String defaultConfig) {
            this.typeName = typeName;
            this.geometryEncoder = geometryEncoder;
            this.layerClass = layerClass;
            this.layerIndexClass = layerIndexClass;
            this.crs = crs;
            this.defaultConfig = defaultConfig;
        }
		/**
		 * For external expression of the configuration of this geometry encoder
		 * @return descriptive signature of encoder, type and configuration
		 */
		String getSignature() {
			return "RegisteredLayerType(name='" + typeName + "', geometryEncoder=" +
					geometryEncoder.getSimpleName() + ", layerClass=" + layerClass.getSimpleName() +
					", index=" + layerIndexClass.getSimpleName() +
					", crs='" + crs.getName(null) + "', defaultConfig='" + defaultConfig + "')";
		}
	}

    private static Map<String, RegisteredLayerType> registeredLayerTypes = new LinkedHashMap<>();
    static {
		addRegisteredLayerType(new RegisteredLayerType("SimplePoint", SimplePointEncoder.class,
				SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerRTreeIndex.class, "longitude:latitude"));
		addRegisteredLayerType(new RegisteredLayerType("Geohash", SimplePointEncoder.class,
				SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerGeohashPointIndex.class, "longitude:latitude"));
		addRegisteredLayerType(new RegisteredLayerType("ZOrder", SimplePointEncoder.class,
				SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerZOrderPointIndex.class, "longitude:latitude"));
		addRegisteredLayerType(new RegisteredLayerType("Hilbert", SimplePointEncoder.class,
				SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerHilbertPointIndex.class, "longitude:latitude"));
		addRegisteredLayerType(new RegisteredLayerType("NativePoint", NativePointEncoder.class,
				SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerRTreeIndex.class, "location"));
		addRegisteredLayerType(new RegisteredLayerType("NativeGeohash", NativePointEncoder.class,
				SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerGeohashPointIndex.class, "location"));
		addRegisteredLayerType(new RegisteredLayerType("NativeZOrder", NativePointEncoder.class,
				SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerZOrderPointIndex.class, "location"));
		addRegisteredLayerType(new RegisteredLayerType("NativeHilbert", NativePointEncoder.class,
				SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerHilbertPointIndex.class, "location"));
		addRegisteredLayerType(new RegisteredLayerType("WKT", WKTGeometryEncoder.class, EditableLayerImpl.class,
				DefaultGeographicCRS.WGS84, LayerRTreeIndex.class, "geometry"));
		addRegisteredLayerType(new RegisteredLayerType("WKB", WKBGeometryEncoder.class, EditableLayerImpl.class,
				DefaultGeographicCRS.WGS84, LayerRTreeIndex.class, "geometry"));
		addRegisteredLayerType(new RegisteredLayerType("OSM", OSMGeometryEncoder.class, OSMLayer.class,
				DefaultGeographicCRS.WGS84, LayerRTreeIndex.class, "geometry"));
	}

	private static void addRegisteredLayerType(RegisteredLayerType type) {
		registeredLayerTypes.put(type.typeName.toLowerCase(), type);
	}

    public Layer getOrCreateRegisteredTypeLayer(String name, String type, String config){
        RegisteredLayerType registeredLayerType = registeredLayerTypes.get(type.toLowerCase());
        return getOrCreateRegisteredTypeLayer(name, registeredLayerType, config);
    }

    public Layer getOrCreateRegisteredTypeLayer(String name, RegisteredLayerType registeredLayerType, String config) {
        return getOrCreateLayer(name, registeredLayerType.geometryEncoder, registeredLayerType.layerClass,
                (config == null) ? registeredLayerType.defaultConfig : config);
    }

	public Map<String, String> getRegisteredLayerTypes() {
		Map<String, String> results = new LinkedHashMap<>();
		registeredLayerTypes.forEach((s, definition) -> results.put(s, definition.getSignature()));
		return results;
	}

	public Class suggestLayerClassForEncoder(Class encoderClass) {
		for (RegisteredLayerType type : registeredLayerTypes.values()) {
			if (type.geometryEncoder == encoderClass) {
				return type.layerClass;
			}
		}
		return EditableLayerImpl.class;
	}
}
