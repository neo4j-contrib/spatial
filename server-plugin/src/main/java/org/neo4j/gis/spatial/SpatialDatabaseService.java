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

import static org.neo4j.gis.spatial.Constants.GTYPE_GEOMETRY;
import static org.neo4j.gis.spatial.Constants.GTYPE_LINESTRING;
import static org.neo4j.gis.spatial.Constants.GTYPE_MULTILINESTRING;
import static org.neo4j.gis.spatial.Constants.GTYPE_MULTIPOINT;
import static org.neo4j.gis.spatial.Constants.GTYPE_MULTIPOLYGON;
import static org.neo4j.gis.spatial.Constants.GTYPE_POINT;
import static org.neo4j.gis.spatial.Constants.GTYPE_POLYGON;
import static org.neo4j.gis.spatial.Constants.LABEL_LAYER;
import static org.neo4j.gis.spatial.Constants.PROP_LAYER;
import static org.neo4j.gis.spatial.Constants.PROP_LAYER_CLASS;
import static org.neo4j.gis.spatial.Constants.PROP_LAYER_TYPE;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.neo4j.gis.spatial.encoders.NativePointEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.encoders.WKBGeometryEncoder;
import org.neo4j.gis.spatial.encoders.WKTGeometryEncoder;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.gis.spatial.utilities.IndexRegistry;
import org.neo4j.gis.spatial.utilities.LayerTypePresetRegistry;
import org.neo4j.gis.spatial.utilities.LayerUtilities;
import org.neo4j.gis.spatial.utilities.ReferenceNodes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.spatial.api.SpatialRecord;
import org.neo4j.spatial.api.encoder.GeometryEncoder;
import org.neo4j.spatial.api.index.IndexManager;
import org.neo4j.spatial.api.index.SpatialIndexWriter;
import org.neo4j.spatial.api.layer.EditableLayer;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.api.layer.LayerTypePresets.RegisteredLayerType;
import org.neo4j.spatial.api.monitoring.ProgressListener;

/**
 * This is the main API entrypoint for the embedded access to spatial database capabilities.
 * Primarily it allows finding and or creating Layer objects, which are of many types, each
 * depending on the actual data model backing the GIS. All real data access is then done
 * through the layer instance which interprets the GIS functions in terms of the underlying model.
 */
public class SpatialDatabaseService {

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

	public static String convertGeometryTypeToName(Integer geometryType) {
		return convertGeometryTypeToJtsClass(geometryType).getName().replace("org.locationtech.jts.geom.", "");
	}

	public static Class<? extends Geometry> convertGeometryTypeToJtsClass(Integer geometryType) {
		if (geometryType == null) {
			return Geometry.class;
		}
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

	public List<String> getLayerNames(Transaction tx) {
		assertNotOldModel(tx);
		List<String> names = new ArrayList<>();

		try (var layers = tx.findNodes(LABEL_LAYER)) {
			while (layers.hasNext()) {
				Layer layer = LayerUtilities.makeLayerFromNode(tx, indexManager, layers.next(), true);
				if (layer instanceof DynamicLayer) {
					names.addAll(((DynamicLayer) layer).getLayerNames(tx));
				} else {
					names.add(layer.getName());
				}
			}
		}

		return names;
	}

	public Layer getLayer(Transaction tx, String name, boolean readOnly) {
		assertNotOldModel(tx);
		try (var layers = tx.findNodes(LABEL_LAYER)) {
			while (layers.hasNext()) {
				Node node = layers.next();
				if (name.equals(node.getProperty(PROP_LAYER))) {
					return LayerUtilities.makeLayerFromNode(tx, indexManager, node, readOnly);
				}
			}
		}
		return getDynamicLayer(tx, name, readOnly);
	}

	public Layer getDynamicLayer(Transaction tx, String name, boolean readOnly) {
		assertNotOldModel(tx);
		ArrayList<DynamicLayer> dynamicLayers = new ArrayList<>();
		try (var layers = tx.findNodes(LABEL_LAYER)) {
			while (layers.hasNext()) {
				Node node = layers.next();
				if (!node.getProperty(PROP_LAYER_CLASS, "").toString().startsWith("DefaultLayer")) {
					Layer layer = LayerUtilities.makeLayerFromNode(tx, indexManager, node, readOnly);
					if (layer instanceof DynamicLayer) {
						dynamicLayers.add(
								(DynamicLayer) LayerUtilities.makeLayerFromNode(tx, indexManager, node, readOnly));
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
		node.setProperty(PROP_LAYER_TYPE, DynamicLayer.class.getCanonicalName());
		return (DynamicLayer) LayerUtilities.makeLayerFromNode(tx, indexManager, node, layer.isReadOnly());
	}

	public DefaultLayer getOrCreateDefaultLayer(Transaction tx, String name, String indexConfig, boolean readOnly) {
		return (DefaultLayer) getOrCreateLayer(tx, name, WKBGeometryEncoder.class, EditableLayerImpl.class, "",
				indexConfig, readOnly);
	}

	public EditableLayer getOrCreateEditableLayer(Transaction tx, String name, String format, String propertyNameConfig,
			String indexConfig, boolean readOnly) {
		Class<? extends GeometryEncoder> geClass = WKBGeometryEncoder.class;
		if (format != null && format.toUpperCase().startsWith("WKT")) {
			geClass = WKTGeometryEncoder.class;
		}
		return (EditableLayer) getOrCreateLayer(tx, name, geClass, EditableLayerImpl.class, propertyNameConfig,
				indexConfig, readOnly);
	}

	public EditableLayer getOrCreateEditableLayer(Transaction tx, String name, String wktProperty, String indexConfig,
			boolean readOnly) {
		return getOrCreateEditableLayer(tx, name, "WKT", wktProperty, indexConfig, readOnly);
	}

	public EditableLayer getOrCreateSimplePointLayer(Transaction tx, String name, String index, String xProperty,
			String yProperty, String indexConfig, boolean readOnly) {
		return getOrCreatePointLayer(tx, name, IndexRegistry.INSTANCE.getRegisteredIndices().get(index),
				SimplePointEncoder.class, indexConfig,
				readOnly, xProperty,
				yProperty);
	}

	public EditableLayer getOrCreateNativePointLayer(
			@Nonnull Transaction tx,
			@Nonnull String name,
			@Nonnull String index,
			String locationProperty,
			String indexConfig,
			boolean readOnly
	) {
		return getOrCreatePointLayer(tx, name, IndexRegistry.INSTANCE.getRegisteredIndices().get(index),
				SimplePointEncoder.class, indexConfig,
				readOnly, locationProperty);
	}

	public EditableLayer getOrCreatePointLayer(
			@Nonnull Transaction tx,
			@Nonnull String name,
			@Nonnull Class<? extends SpatialIndexWriter> indexClass,
			@Nonnull Class<? extends GeometryEncoder> encoderClass,
			String indexConfig,
			boolean readOnly,
			String... encoderConfig
	) {
		Layer layer = getLayer(tx, name, readOnly);
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
			Class<? extends Layer> layerClass, String encoderConfig, String indexConfig, boolean readOnly) {
		Layer layer = getLayer(tx, name, readOnly);
		if (layer == null) {
			layer = createLayer(tx, name, geometryEncoder, layerClass, LayerRTreeIndex.class, encoderConfig,
					indexConfig);
		} else if (!(layerClass == null || layerClass.isInstance(layer))) {
			throw new SpatialDatabaseException(
					"Existing layer '" + layer + "' is not of the expected type: " + layerClass);
		}
		return layer;
	}

	public Layer getOrCreateLayer(Transaction tx, String name, Class<? extends GeometryEncoder> geometryEncoder,
			Class<? extends Layer> layerClass, String indexConfig, boolean readOnly) {
		return getOrCreateLayer(tx, name, geometryEncoder, layerClass, "", indexConfig, readOnly);
	}

	public boolean containsLayer(Transaction tx, String name) {
		return getLayer(tx, name, true) != null;
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

	public SimplePointLayer createSimplePointLayer(Transaction tx, String name,
			String... xybProperties) {
		return createPointLayer(tx, name, LayerRTreeIndex.class, SimplePointEncoder.class, null,
				xybProperties);
	}

	public SimplePointLayer createNativePointLayer(Transaction tx, String name,
			String... encoderConfig) {
		return createPointLayer(tx, name, LayerRTreeIndex.class, NativePointEncoder.class, null,
				encoderConfig);
	}

	public SimplePointLayer createPointLayer(
			@Nonnull Transaction tx,
			@Nonnull String name,
			@Nonnull Class<? extends SpatialIndexWriter> indexClass,
			@Nonnull Class<? extends GeometryEncoder> encoderClass,
			String indexConfig,
			String... encoderConfig
	) {
		return (SimplePointLayer) createLayer(tx, name, encoderClass, SimplePointLayer.class, indexClass,
				makeEncoderConfig(encoderConfig), indexConfig, org.geotools.referencing.crs.DefaultGeographicCRS.WGS84
		);
	}

	public Layer createLayer(
			@Nonnull Transaction tx,
			@Nonnull String name,
			@Nonnull Class<? extends GeometryEncoder> geometryEncoderClass,
			@Nonnull Class<? extends Layer> layerClass,
			String indexConfig
	) {
		return createLayer(tx, name, geometryEncoderClass, layerClass, LayerRTreeIndex.class, null, indexConfig);
	}

	public Layer createLayer(
			@Nonnull Transaction tx,
			@Nonnull String name,
			@Nonnull Class<? extends GeometryEncoder> geometryEncoderClass,
			@Nonnull Class<? extends Layer> layerClass,
			@Nonnull Class<? extends SpatialIndexWriter> indexClass,
			String encoderConfig,
			String indexConfig
	) {
		return createLayer(tx, name, geometryEncoderClass, layerClass, indexClass, encoderConfig, indexConfig, null
		);
	}

	public Layer createLayer(
			@Nonnull Transaction tx,
			@Nonnull String name,
			@Nonnull Class<? extends GeometryEncoder> geometryEncoderClass,
			@Nonnull Class<? extends Layer> layerClass,
			@Nonnull Class<? extends SpatialIndexWriter> indexClass,
			String encoderConfig,
			String indexConfig,
			CoordinateReferenceSystem crs
	) {
		if (containsLayer(tx, name)) {
			throw new SpatialDatabaseException("Layer " + name + " already exists");
		}

		var layer = LayerUtilities.makeLayerAndNode(tx, indexManager, name, geometryEncoderClass, encoderConfig,
				layerClass, indexClass, indexConfig);
		if (crs != null && layer instanceof EditableLayer) {
			((EditableLayer) layer).setCoordinateReferenceSystem(tx, crs);
		}
		return layer;
	}

	public void deleteLayer(Transaction tx, String name, ProgressListener monitor) {
		EditableLayer layer = (EditableLayer) getLayer(tx, name, false);
		if (layer == null) {
			throw new SpatialDatabaseException("Layer " + name + " does not exist");
		}
		layer.delete(tx, monitor);
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
	public Layer createResultsLayer(Transaction tx, String layerName, List<SpatialRecord> results) {
		EditableLayer layer = (EditableLayer) createWKBLayer(tx, layerName, "");
		for (SpatialRecord record : results) {
			layer.add(tx, record.getGeometry());
		}
		layer.finalizeTransaction(tx);
		return layer;
	}


	public Layer getOrCreateRegisteredTypeLayer(Transaction tx, String name, String type, String encoderConfig,
			String indexConfig, boolean readOnly) {
		RegisteredLayerType registeredLayerType = LayerTypePresetRegistry.INSTANCE.getRegisteredLayerType(type);
		if (registeredLayerType == null) {
			throw new SpatialDatabaseException("Unknown layer type: " + type);
		}
		return getOrCreateRegisteredTypeLayer(tx, name, registeredLayerType, encoderConfig, indexConfig, readOnly);
	}


	public Layer getOrCreateRegisteredTypeLayer(Transaction tx, String name, RegisteredLayerType registeredLayerType,
			String encoderConfig, String indexConfig, boolean readOnly) {
		return getOrCreateLayer(tx, name, registeredLayerType.geometryEncoder(), registeredLayerType.layerClass(),
				(encoderConfig == null) ? registeredLayerType.defaultEncoderConfig() : encoderConfig, indexConfig,
				readOnly);
	}
}
