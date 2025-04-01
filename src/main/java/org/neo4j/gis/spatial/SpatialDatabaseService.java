/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.referencing.crs.AbstractCRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.neo4j.gis.spatial.encoders.Configurable;
import org.neo4j.gis.spatial.encoders.NativePointEncoder;
import org.neo4j.gis.spatial.encoders.NativePointsEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.index.LayerGeohashPointIndex;
import org.neo4j.gis.spatial.index.LayerHilbertPointIndex;
import org.neo4j.gis.spatial.index.LayerIndexReader;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.gis.spatial.index.LayerZOrderPointIndex;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.utilities.LayerUtilities;
import org.neo4j.gis.spatial.utilities.ReferenceNodes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

/**
 * This is the main API entrypoint for the embedded access to spatial database capabilities.
 * Primarily it allows finding and or creating Layer objects, which are of many types, each
 * depending on the actual data model backing the GIS. All real data access is then done
 * through the layer instance which interprets the GIS functions in terms of the underlying model.
 */
public class SpatialDatabaseService implements Constants {

	public final IndexManager indexManager;

	public SpatialDatabaseService(IndexManager indexManager) {
		this.indexManager = indexManager;
	}

	public static void assertNotOldModel(Transaction tx) {
		Node oldReferenceNode = ReferenceNodes.findDeprecatedReferenceNode(tx, "spatial_root");
		if (oldReferenceNode != null) {
			throw new IllegalStateException(
					"Old reference node exists - please upgrade the spatial database to the new format");
		}
	}

	public List<String> upgradeFromOldModel(Transaction tx) {
		ArrayList<String> layersConverted = new ArrayList<>();
		Node oldReferenceNode = ReferenceNodes.findDeprecatedReferenceNode(tx, "spatial_root");
		if (oldReferenceNode != null) {
			List<Node> layers = new ArrayList<>();

			try (var relationships = oldReferenceNode.getRelationships(Direction.OUTGOING,
					SpatialRelationshipTypes.LAYER)) {
				for (Relationship relationship : relationships) {
					layers.add(relationship.getEndNode());
				}
			}

			for (Node layer : layers) {
				Relationship fromRoot = layer.getSingleRelationship(SpatialRelationshipTypes.LAYER, Direction.INCOMING);
				fromRoot.delete();
				layer.addLabel(LABEL_LAYER);
				layersConverted.add((String) layer.getProperty(PROP_LAYER));
			}

			try (var relationships = oldReferenceNode.getRelationships()) {
				if (relationships.iterator().hasNext()) {
					throw new IllegalStateException(
							"Cannot upgrade - ReferenceNode 'spatial_root' still has relationships other than layers");
				}
			}

			oldReferenceNode.delete();
		}
		indexManager.makeIndexFor(tx, "SpatialLayers", LABEL_LAYER, PROP_LAYER);
		return layersConverted;
	}

	public String[] getLayerNames(Transaction tx) {
		assertNotOldModel(tx);
		List<String> names = new ArrayList<>();

		try (var layers = tx.findNodes(LABEL_LAYER)) {
			while (layers.hasNext()) {
				Layer layer = LayerUtilities.makeLayerFromNode(tx, indexManager, layers.next());
				if (layer instanceof DynamicLayer) {
					names.addAll(((DynamicLayer) layer).getLayerNames(tx));
				} else {
					names.add(layer.getName());
				}
			}
		}

		return names.toArray(new String[0]);
	}

	public Layer getLayer(Transaction tx, String name) {
		assertNotOldModel(tx);
		try (var layers = tx.findNodes(LABEL_LAYER)) {
			while (layers.hasNext()) {
				Node node = layers.next();
				if (name.equals(node.getProperty(PROP_LAYER))) {
					return LayerUtilities.makeLayerFromNode(tx, indexManager, node);
				}
			}
		}
		return getDynamicLayer(tx, name);
	}

	public Layer getDynamicLayer(Transaction tx, String name) {
		assertNotOldModel(tx);
		ArrayList<DynamicLayer> dynamicLayers = new ArrayList<>();
		try (var layers = tx.findNodes(LABEL_LAYER)) {
			while (layers.hasNext()) {
				Node node = layers.next();
				if (!node.getProperty(PROP_LAYER_CLASS, "").toString().startsWith("DefaultLayer")) {
					Layer layer = LayerUtilities.makeLayerFromNode(tx, indexManager, node);
					if (layer instanceof DynamicLayer) {
						dynamicLayers.add((DynamicLayer) LayerUtilities.makeLayerFromNode(tx, indexManager, node));
					}
				}
			}
		}
		for (DynamicLayer layer : dynamicLayers) {
			for (String dynLayerName : layer.getLayerNames(tx)) {
				if (name.equals(dynLayerName)) {
					return layer.getLayer(tx, dynLayerName);
				}
			}
		}
		return null;
	}

	/**
	 * Convert a layer into a DynamicLayer. This will expose the ability to add
	 * views, or 'dynamic layers' to the layer.
	 *
	 * @return new DynamicLayer version of the original layer
	 */
	public DynamicLayer asDynamicLayer(Transaction tx, Layer layer) {
		if (layer instanceof DynamicLayer) {
			return (DynamicLayer) layer;
		}
		Node node = layer.getLayerNode(tx);
		node.setProperty(PROP_LAYER_CLASS, DynamicLayer.class.getCanonicalName());
		return (DynamicLayer) LayerUtilities.makeLayerFromNode(tx, indexManager, node);
	}

	public DefaultLayer getOrCreateDefaultLayer(Transaction tx, String name, String indexConfig) {
		return (DefaultLayer) getOrCreateLayer(tx, name, WKBGeometryEncoder.class, EditableLayerImpl.class, "",
				indexConfig);
	}

	public EditableLayer getOrCreateEditableLayer(Transaction tx, String name, String format, String propertyNameConfig,
			String indexConfig) {
		Class<? extends GeometryEncoder> geClass = WKBGeometryEncoder.class;
		if (format != null && format.toUpperCase().startsWith("WKT")) {
			geClass = WKTGeometryEncoder.class;
		}
		return (EditableLayer) getOrCreateLayer(tx, name, geClass, EditableLayerImpl.class, propertyNameConfig,
				indexConfig);
	}

	public EditableLayer getOrCreateEditableLayer(Transaction tx, String name, String indexConfig) {
		return getOrCreateEditableLayer(tx, name, "WKB", "", indexConfig);
	}

	public EditableLayer getOrCreateEditableLayer(Transaction tx, String name, String wktProperty, String indexConfig) {
		return getOrCreateEditableLayer(tx, name, "WKT", wktProperty, indexConfig);
	}

	public static Class<? extends LayerIndexReader> resolveIndexClass(String index) {
		if (index == null) {
			return LayerRTreeIndex.class;
		}
		return switch (index.toLowerCase()) {
			case INDEX_TYPE_RTREE -> LayerRTreeIndex.class;
			case INDEX_TYPE_GEOHASH -> LayerGeohashPointIndex.class;
			case INDEX_TYPE_ZORDER -> LayerZOrderPointIndex.class;
			case INDEX_TYPE_HILBERT -> LayerHilbertPointIndex.class;
			default -> throw new IllegalArgumentException("Unknown index: " + index);
		};
	}

	public EditableLayer getOrCreateSimplePointLayer(Transaction tx, String name, String index, String xProperty,
			String yProperty, String indexConfig) {
		return getOrCreatePointLayer(tx, name, resolveIndexClass(index), SimplePointEncoder.class, indexConfig,
				xProperty,
				yProperty);
	}

	public EditableLayer getOrCreateNativePointLayer(Transaction tx, String name, String index,
			String locationProperty, String indexConfig) {
		return getOrCreatePointLayer(tx, name, resolveIndexClass(index), SimplePointEncoder.class, indexConfig,
				locationProperty);
	}

	public EditableLayer getOrCreatePointLayer(Transaction tx, String name,
			Class<? extends LayerIndexReader> indexClass, Class<? extends GeometryEncoder> encoderClass,
			String indexConfig, String... encoderConfig) {
		Layer layer = getLayer(tx, name);
		if (layer == null) {
			return (EditableLayer) createLayer(tx, name, encoderClass, SimplePointLayer.class, indexClass,
					makeEncoderConfig(encoderConfig), indexConfig, DefaultGeographicCRS.WGS84);
		}
		if (layer instanceof EditableLayer) {
			return (EditableLayer) layer;
		}
		throw new SpatialDatabaseException(
				"Existing layer '" + layer + "' is not of the expected type: " + EditableLayer.class);
	}

	public Layer getOrCreateLayer(Transaction tx, String name, Class<? extends GeometryEncoder> geometryEncoder,
			Class<? extends Layer> layerClass, String encoderConfig, String indexConfig) {
		Layer layer = getLayer(tx, name);
		if (layer == null) {
			layer = createLayer(tx, name, geometryEncoder, layerClass, null, encoderConfig, indexConfig);
		} else if (!(layerClass == null || layerClass.isInstance(layer))) {
			throw new SpatialDatabaseException(
					"Existing layer '" + layer + "' is not of the expected type: " + layerClass);
		}
		return layer;
	}

	public Layer getOrCreateLayer(Transaction tx, String name, Class<? extends GeometryEncoder> geometryEncoder,
			Class<? extends Layer> layerClass, String indexConfig) {
		return getOrCreateLayer(tx, name, geometryEncoder, layerClass, "", indexConfig);
	}

	/**
	 * This method will find the Layer when given a geometry node that this layer contains. This method
	 * used to make use of knowledge of the RTree, traversing backwards up the tree to find the layer node, which is
	 * fast. However, for reasons of clean abstraction,
	 * this has been refactored to delegate the logic to the layer, so that each layer can do this in an
	 * implementation specific way. Now we simply iterate through the layers datasets and the first one
	 * to return true on the SpatialDataset.containsGeometryNode(Transaction,Node) method is returned.
	 * <p>
	 * We can consider removing this method for a few reasons:
	 * * It is non-deterministic if more than one layer contains the same geometry
	 * * None of the current code appears to use this method
	 *
	 * @param geometryNode to start search
	 * @return Layer object containing this geometry
	 */
	public Layer findLayerContainingGeometryNode(Transaction tx, Node geometryNode) {
		for (String layerName : getLayerNames(tx)) {
			Layer layer = getLayer(tx, layerName);
			if (layer.getDataset().containsGeometryNode(tx, geometryNode)) {
				return layer;
			}
		}
		return null;
	}

	public boolean containsLayer(Transaction tx, String name) {
		return getLayer(tx, name) != null;
	}

	public Layer createWKBLayer(Transaction tx, String name, String indexConfig) {
		return createLayer(tx, name, WKBGeometryEncoder.class, EditableLayerImpl.class, indexConfig);
	}

	public SimplePointLayer createSimplePointLayer(Transaction tx, String name) {
		return createSimplePointLayer(tx, name, (String[]) null);
	}

	public SimplePointLayer createSimplePointLayer(Transaction tx, String name, String xProperty, String yProperty) {
		return createSimplePointLayer(tx, name, xProperty, yProperty, null);
	}

	public SimplePointLayer createSimplePointLayer(Transaction tx, String name, String... xybProperties) {
		return createPointLayer(tx, name, LayerRTreeIndex.class, SimplePointEncoder.class, null, xybProperties);
	}

	public SimplePointLayer createNativePointLayer(Transaction tx, String name) {
		return createNativePointLayer(tx, name, (String[]) null);
	}

	public SimplePointLayer createNativePointLayer(Transaction tx, String name, String locationProperty,
			String bboxProperty) {
		return createNativePointLayer(tx, name, locationProperty, bboxProperty, null);
	}

	public SimplePointLayer createNativePointLayer(Transaction tx, String name, String... encoderConfig) {
		return createPointLayer(tx, name, LayerRTreeIndex.class, NativePointEncoder.class, null, encoderConfig);
	}

	public SimplePointLayer createPointLayer(Transaction tx, String name, Class<? extends LayerIndexReader> indexClass,
			Class<? extends GeometryEncoder> encoderClass, String indexConfig, String... encoderConfig
	) {
		return (SimplePointLayer) createLayer(tx, name, encoderClass, SimplePointLayer.class, indexClass,
				makeEncoderConfig(encoderConfig), indexConfig, org.geotools.referencing.crs.DefaultGeographicCRS.WGS84);
	}

	public static String makeEncoderConfig(String... args) {
		StringBuilder sb = new StringBuilder();
		if (args != null) {
			for (String arg : args) {
				if (arg != null) {
					if (!sb.isEmpty()) {
						sb.append(":");
					}
					sb.append(arg);
				}
			}
		}
		return sb.toString();
	}

	public Layer createLayer(Transaction tx, String name, Class<? extends GeometryEncoder> geometryEncoderClass,
			Class<? extends Layer> layerClass, String indexConfig) {
		return createLayer(tx, name, geometryEncoderClass, layerClass, null, null, indexConfig);
	}

	public Layer createLayer(Transaction tx, String name, Class<? extends GeometryEncoder> geometryEncoderClass,
			Class<? extends Layer> layerClass, Class<? extends LayerIndexReader> indexClass,
			String encoderConfig,
			String indexConfig
	) {
		return createLayer(tx, name, geometryEncoderClass, layerClass, indexClass, encoderConfig, indexConfig, null);
	}

	public Layer createLayer(Transaction tx,
			String name,
			Class<? extends GeometryEncoder> geometryEncoderClass,
			Class<? extends Layer> layerClass,
			Class<? extends LayerIndexReader> indexClass,
			String encoderConfig,
			String indexConfig,
			CoordinateReferenceSystem crs
	) {
		if (containsLayer(tx, name)) {
			throw new SpatialDatabaseException("Layer " + name + " already exists");
		}

		Layer layer = LayerUtilities.makeLayerAndNode(tx, indexManager, name, geometryEncoderClass, layerClass,
				indexClass);
		if (encoderConfig != null && !encoderConfig.isEmpty()) {
			GeometryEncoder encoder = layer.getGeometryEncoder();
			if (encoder instanceof Configurable) {
				((Configurable) encoder).setConfiguration(encoderConfig);
				layer.getLayerNode(tx).setProperty(PROP_GEOMENCODER_CONFIG, encoderConfig);
			} else {
				System.out.println(
						"Warning: encoder configuration '" + encoderConfig + "' passed to non-configurable encoder: "
								+ geometryEncoderClass);
			}
		}
		if (indexConfig != null && !indexConfig.isEmpty()) {
			LayerIndexReader index = layer.getIndex();
			if (index instanceof Configurable) {
				((Configurable) index).setConfiguration(indexConfig);
				layer.getLayerNode(tx).setProperty(PROP_INDEX_CONFIG, indexConfig);
			} else {
				System.out.println(
						"Warning: index configuration '" + indexConfig + "' passed to non-configurable index: "
								+ indexClass);
			}
		}
		if (crs != null && layer instanceof EditableLayer) {
			((EditableLayer) layer).setCoordinateReferenceSystem(tx, crs);
		}
		return layer;
	}

	public void deleteLayer(Transaction tx, String name, Listener monitor) {
		Layer layer = getLayer(tx, name);
		if (layer == null) {
			throw new SpatialDatabaseException("Layer " + name + " does not exist");
		}
		layer.delete(tx, monitor);
	}

	public static int convertGeometryNameToType(String geometryName) {
		if (geometryName == null) {
			return GTYPE_GEOMETRY;
		}
		try {
			Class<?> aClass = Class.forName("org.locationtech.jts.geom." + geometryName);
			if (!Geometry.class.isAssignableFrom(aClass)) {
				throw new ClassNotFoundException("Not a geometry class");
			}
			//noinspection unchecked
			return convertJtsClassToGeometryType((Class<? extends Geometry>) aClass);
		} catch (ClassNotFoundException e) {
			System.err.println("Unrecognized geometry '" + geometryName + "': " + e);
			return GTYPE_GEOMETRY;
		}
	}

	public static String convertGeometryTypeToName(Integer geometryType) {
		return convertGeometryTypeToJtsClass(geometryType).getName().replace("org.locationtech.jts.geom.", "");
	}

	public static Class<? extends Geometry> convertGeometryTypeToJtsClass(Integer geometryType) {
		return switch (geometryType) {
			case GTYPE_POINT -> Point.class;
			case GTYPE_LINESTRING -> LineString.class;
			case GTYPE_POLYGON -> Polygon.class;
			case GTYPE_MULTIPOINT -> MultiPoint.class;
			case GTYPE_MULTILINESTRING -> MultiLineString.class;
			case GTYPE_MULTIPOLYGON -> MultiPolygon.class;
			default -> Geometry.class;
		};
	}

	public static int convertJtsClassToGeometryType(Class<? extends Geometry> jtsClass) {
		if (jtsClass.equals(Point.class)) {
			return GTYPE_POINT;
		}
		if (jtsClass.equals(LineString.class)) {
			return GTYPE_LINESTRING;
		}
		if (jtsClass.equals(Polygon.class)) {
			return GTYPE_POLYGON;
		}
		if (jtsClass.equals(MultiPoint.class)) {
			return GTYPE_MULTIPOINT;
		}
		if (jtsClass.equals(MultiLineString.class)) {
			return GTYPE_MULTILINESTRING;
		}
		if (jtsClass.equals(MultiPolygon.class)) {
			return GTYPE_MULTIPOLYGON;
		}
		return GTYPE_GEOMETRY;
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
	 * @param results   collection of SpatialDatabaseRecords to add to new layer
	 * @return new Layer with copy of all geometries
	 */
	public Layer createResultsLayer(Transaction tx, String layerName, List<SpatialDatabaseRecord> results) {
		EditableLayer layer = (EditableLayer) createWKBLayer(tx, layerName, "");
		for (SpatialDatabaseRecord record : results) {
			layer.add(tx, record.getGeometry());
		}
		return layer;
	}


	/**
	 * Support mapping a String (ex: 'SimplePoint') to the respective GeometryEncoder and Layer classes
	 * to allow for more streamlined method for creating Layers
	 * This was added to help support Spatial Cypher project.
	 */
	public static class RegisteredLayerType {

		final String typeName;
		final Class<? extends GeometryEncoder> geometryEncoder;
		final Class<? extends Layer> layerClass;
		final Class<? extends LayerIndexReader> layerIndexClass;
		final String defaultConfig;
		final org.geotools.referencing.crs.AbstractCRS crs;

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
		 *
		 * @return descriptive signature of encoder, type and configuration
		 */
		String getSignature() {
			return "RegisteredLayerType(name='" + typeName + "', geometryEncoder=" +
					geometryEncoder.getSimpleName() + ", layerClass=" + layerClass.getSimpleName() +
					", index=" + layerIndexClass.getSimpleName() +
					", crs='" + crs.getName(null) + "', defaultConfig='" + defaultConfig + "')";
		}
	}

	private static final Map<String, RegisteredLayerType> registeredLayerTypes = new LinkedHashMap<>();

	static {
		addRegisteredLayerType(new RegisteredLayerType("SimplePoint", SimplePointEncoder.class,
				SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerRTreeIndex.class, "longitude:latitude"));
		addRegisteredLayerType(new RegisteredLayerType("Geohash", SimplePointEncoder.class,
				SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerGeohashPointIndex.class,
				"longitude:latitude"));
		addRegisteredLayerType(new RegisteredLayerType("ZOrder", SimplePointEncoder.class,
				SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerZOrderPointIndex.class, "longitude:latitude"));
		addRegisteredLayerType(new RegisteredLayerType("Hilbert", SimplePointEncoder.class,
				SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerHilbertPointIndex.class,
				"longitude:latitude"));
		addRegisteredLayerType(new RegisteredLayerType("NativePoint", NativePointEncoder.class,
				SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerRTreeIndex.class, "location"));
		addRegisteredLayerType(new RegisteredLayerType("NativePoints", NativePointsEncoder.class,
				EditableLayerImpl.class, DefaultGeographicCRS.WGS84, LayerRTreeIndex.class, "geometry"));
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

	public Layer getOrCreateRegisteredTypeLayer(Transaction tx, String name, String type, String encoderConfig,
			String indexConfig) {
		RegisteredLayerType registeredLayerType = registeredLayerTypes.get(type.toLowerCase());
		return getOrCreateRegisteredTypeLayer(tx, name, registeredLayerType, encoderConfig, indexConfig);
	}

	public Layer getOrCreateRegisteredTypeLayer(Transaction tx, String name, RegisteredLayerType registeredLayerType,
			String encoderConfig, String indexConfig) {
		return getOrCreateLayer(tx, name, registeredLayerType.geometryEncoder, registeredLayerType.layerClass,
				(encoderConfig == null) ? registeredLayerType.defaultConfig : encoderConfig, indexConfig);
	}

	public static Map<String, String> getRegisteredLayerTypes() {
		Map<String, String> results = new LinkedHashMap<>();
		registeredLayerTypes.forEach((s, definition) -> results.put(s, definition.getSignature()));
		return results;
	}

	public static Class<? extends Layer> suggestLayerClassForEncoder(Class<? extends GeometryEncoder> encoderClass) {
		for (RegisteredLayerType type : registeredLayerTypes.values()) {
			if (type.geometryEncoder == encoderClass) {
				return type.layerClass;
			}
		}
		return EditableLayerImpl.class;
	}
}
