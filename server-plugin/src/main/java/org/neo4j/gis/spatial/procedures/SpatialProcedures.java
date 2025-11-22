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
package org.neo4j.gis.spatial.procedures;

import static org.neo4j.gis.spatial.Constants.DOC_COORDINATE;
import static org.neo4j.gis.spatial.Constants.DOC_CRS;
import static org.neo4j.gis.spatial.Constants.DOC_ENCODER_CONFIG;
import static org.neo4j.gis.spatial.Constants.DOC_ENCODER_NAME;
import static org.neo4j.gis.spatial.Constants.DOC_INDEX_CONFIG;
import static org.neo4j.gis.spatial.Constants.DOC_INDEX_TYPE;
import static org.neo4j.gis.spatial.Constants.DOC_JTS_GEOMETRY;
import static org.neo4j.gis.spatial.Constants.DOC_LAYER_NAME;
import static org.neo4j.gis.spatial.Constants.DOC_LAYER_TYPE;
import static org.neo4j.gis.spatial.Constants.DOC_URI;
import static org.neo4j.gis.spatial.Constants.PROP_CRS;
import static org.neo4j.gis.spatial.Constants.WGS84_CRS_NAME;
import static org.neo4j.gis.spatial.SpatialDatabaseService.INDEX_TYPE_RTREE;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.gis.spatial.EditableLayerImpl;
import org.neo4j.gis.spatial.ShapefileImporter;
import org.neo4j.gis.spatial.SimplePointLayer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.SpatialTopologyUtils;
import org.neo4j.gis.spatial.encoders.NativePointEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.index.LayerGeohashPointIndex;
import org.neo4j.gis.spatial.index.LayerHilbertPointIndex;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.gis.spatial.index.LayerZOrderPointIndex;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.pipes.processing.OrthodromicDistance;
import org.neo4j.gis.spatial.rtree.ProgressLoggingListener;
import org.neo4j.gis.spatial.utilities.GeometryEncoderRegistry;
import org.neo4j.gis.spatial.utilities.IndexRegistry;
import org.neo4j.gis.spatial.utilities.LayerTypePresetRegistry;
import org.neo4j.gis.spatial.utilities.SpatialApiBase;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.spatial.api.Identifiable;
import org.neo4j.spatial.api.encoder.GeometryEncoder;
import org.neo4j.spatial.api.layer.EditableLayer;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.api.layer.LayerTypePresets.RegisteredLayerType;

/*
TODO:
* don't pass raw coordinates, take an object which can be a property-container, geometry-point or a map
* optional default simplePointLayer should use the long form of "latitude and longitude" like the spatial functions do
*/

public class SpatialProcedures extends SpatialApiBase {

	@Context
	public GraphDatabaseService db;

	@Context
	public Log log;


	public record NodeResult(Node node) {

	}

	public record NodeIdResult(String nodeId) {

	}

	public record CountResult(long count) {

	}

	public record NameResult(String name, String signature) {

	}

	public record FeatureAttributeResult(String name, String className) {

	}

	public record NodeDistanceResult(Node node, double distance) {

	}

	public record BoundingBoxResult(
			double minX,
			double minY,
			double maxX,
			double maxY,
			@Description("The CRS in geotools format, e.g. WGS84(DD)")
			String crs) {

	}

	public record LayerMetaResult(
			@Description("The name of the layer")
			String name,
			@Description("The class name of the jts geometry, e.g. org.locationtech.jts.geom.Point")
			String geometryType,
			@Description("The CRS in geotools format, e.g. WGS84(DD)")
			String crs,
			@Description("If true the feature-attributes are complex (defined by the encoder) and must be read form the node via `spatial.extractAttributes`")
			boolean hasComplexAttributes,
			@Description("Additional attributes of the Layer")
			Map<String, String> extraAttributes
	) {

	}

	public record LayerType(
			@Description("The id of the Layer-Type")
			String id,
			@Description("The identifier of the encoder to use")
			String encoder,
			@Description("The identifier of the layer to use")
			String layer,
			@Description("The identifier of the index to use")
			String index,
			@Description("The CRS to use")
			String crsName,
			@Description("The default configuration used for the encoder")
			String defaultEncoderConfig
	) {

		public LayerType(RegisteredLayerType registeredLayerType) {
			this(
					registeredLayerType.typeName(),
					resolveIdentifier(registeredLayerType.geometryEncoder()),
					resolveIdentifier(registeredLayerType.layerClass()),
					resolveIdentifier(registeredLayerType.layerIndexClass()),
					registeredLayerType.crs().getName(null),
					registeredLayerType.defaultEncoderConfig()
			);
		}

		private static String resolveIdentifier(Class<? extends Identifiable> clazz) {
			try {
				return clazz.getDeclaredConstructor().newInstance().getIdentifier();
			} catch (InstantiationException | InvocationTargetException | IllegalAccessException |
					 NoSuchMethodException e) {
				return clazz.getSimpleName();
			}
		}
	}


	public static class GeometryResult {

		public final Object geometry;

		public GeometryResult(org.neo4j.graphdb.spatial.Geometry geometry) {
			// Unfortunately Neo4j 3.4 only copes with Points, other types need to be converted to a public type
			if (geometry instanceof org.neo4j.graphdb.spatial.Point) {
				this.geometry = geometry;
			} else {
				this.geometry = toMap(geometry);
			}
		}
	}

	@Procedure("spatial.procedures")
	@Description("Lists all spatial procedures with name and signature")
	public Stream<NameResult> listProcedures() {
		GlobalProcedures procedures = ((GraphDatabaseAPI) db).getDependencyResolver()
				.resolveDependency(GlobalProcedures.class);
		Stream.Builder<NameResult> builder = Stream.builder();

		procedures.getCurrentView().getAllProcedures(QueryLanguage.CYPHER_5)
				.filter(proc -> proc.name().namespace()[0].equals("spatial"))
				.map(proc -> new NameResult(proc.name().toString(), proc.toString()))
				.forEach(builder);

		return builder.build();
	}

	@Procedure(name = "spatial.upgrade", mode = WRITE)
	@Description("Upgrades an older spatial data model and returns a list of layers upgraded")
	public Stream<NameResult> upgradeSpatial() {
		SpatialDatabaseService sdb = spatial();
		Stream.Builder<NameResult> builder = Stream.builder();
		for (String name : sdb.upgradeFromOldModel(tx)) {
			Layer layer = sdb.getLayer(tx, name, false);
			if (layer != null) {
				builder.accept(new NameResult(name, layer.getSignature()));
			}
		}
		return builder.build();
	}

	@Procedure(value = "spatial.layers", mode = READ)
	@Description("Returns name, and details for all layers")
	public Stream<NameResult> getAllLayers() {
		SpatialDatabaseService sdb = spatial();
		Stream.Builder<NameResult> builder = Stream.builder();
		for (String name : sdb.getLayerNames(tx)) {
			Layer layer = sdb.getLayer(tx, name, true);
			if (layer != null) {
				builder.accept(new NameResult(name, layer.getSignature()));
			}
		}
		return builder.build();
	}

	@Procedure("spatial.layerTypes")
	@Description("Returns the different registered layer types")
	public Stream<LayerType> getAllLayerTypes() {
		return LayerTypePresetRegistry.INSTANCE.getRegisteredLayerPresets().values()
				.stream()
				.map(LayerType::new)
				.sorted(Comparator.comparing(LayerType::id));
	}

	@Procedure(value = "spatial.addPointLayer", mode = WRITE)
	@Description("Adds a new simple point layer, returns the layer root node")
	public Stream<NodeResult> addSimplePointLayer(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "indexType", defaultValue = INDEX_TYPE_RTREE, description = DOC_LAYER_TYPE) String indexType,
			@Name(value = "crsName", defaultValue = UNSET_CRS_NAME, description = DOC_CRS) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig
	) {
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);
		return streamNode(sdb.createLayer(tx, name, SimplePointEncoder.class, SimplePointLayer.class,
						IndexRegistry.INSTANCE.getRegisteredIndices().get(indexType), null, indexConfig, selectCRS(crsName)
				)
				.getLayerNode(tx));
	}

	@Procedure(value = "spatial.addPointLayerGeohash", mode = WRITE)
	@Description("Adds a new simple point layer with geohash based index, returns the layer root node")
	public Stream<NodeResult> addSimplePointLayerGeohash(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "crsName", defaultValue = WGS84_CRS_NAME, description = DOC_CRS) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);
		return streamNode(sdb.createLayer(tx, name, SimplePointEncoder.class, SimplePointLayer.class,
						LayerGeohashPointIndex.class, null, indexConfig, selectCRS(crsName))
				.getLayerNode(tx));
	}

	@Procedure(value = "spatial.addPointLayerZOrder", mode = WRITE)
	@Description("Adds a new simple point layer with z-order curve based index, returns the layer root node")
	public Stream<NodeResult> addSimplePointLayerZOrder(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);
		return streamNode(sdb.createLayer(tx, name, SimplePointEncoder.class, SimplePointLayer.class,
						LayerZOrderPointIndex.class, null, indexConfig, DefaultGeographicCRS.WGS84)
				.getLayerNode(tx));
	}

	@Procedure(value = "spatial.addPointLayerHilbert", mode = WRITE)
	@Description("Adds a new simple point layer with hilbert curve based index, returns the layer root node")
	public Stream<NodeResult> addSimplePointLayerHilbert(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig
	) {
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);
		return streamNode(sdb.createLayer(tx, name, SimplePointEncoder.class, SimplePointLayer.class,
						LayerHilbertPointIndex.class, null, indexConfig, DefaultGeographicCRS.WGS84)
				.getLayerNode(tx));
	}

	@Procedure(value = "spatial.addPointLayerXY", mode = WRITE)
	@Description("Adds a new simple point layer with the given properties for x and y coordinates, returns the layer root node")
	public Stream<NodeResult> addSimplePointLayer(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "xProperty", description = "The property of the node to read the x coordinate from") String xProperty,
			@Name(value = "yProperty", description = "The property of the node to read the y coordinate from") String yProperty,
			@Name(value = "indexType", defaultValue = INDEX_TYPE_RTREE, description = DOC_INDEX_TYPE) String indexType,
			@Name(value = "crsName", defaultValue = UNSET_CRS_NAME, description = DOC_CRS) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig) {
		if (xProperty == null || yProperty == null) {
			throw new IllegalArgumentException(
					"Cannot create layer '" + name + "': Missing encoder config values: xProperty[" + xProperty
							+ "], yProperty[" + yProperty + "]");
		}
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);
		return streamNode(sdb.createLayer(tx, name, SimplePointEncoder.class, SimplePointLayer.class,
						IndexRegistry.INSTANCE.getRegisteredIndices().get(indexType),
						SpatialDatabaseService.makeEncoderConfig(xProperty, yProperty), indexConfig,
						selectCRS(hintCRSName(crsName, yProperty)))
				.getLayerNode(tx));
	}

	@Procedure(value = "spatial.addPointLayerWithConfig", mode = WRITE)
	@Description("Adds a new simple point layer with the given configuration, returns the layer root node")
	public Stream<NodeResult> addSimplePointLayerWithConfig(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "encoderConfig", description = DOC_ENCODER_CONFIG) String encoderConfig,
			@Name(value = "indexType", defaultValue = INDEX_TYPE_RTREE, description = DOC_INDEX_TYPE) String indexType,
			@Name(value = "crsName", defaultValue = UNSET_CRS_NAME, description = DOC_CRS) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig) {
		if (encoderConfig.indexOf(':') <= 0) {
			throw new IllegalArgumentException(
					"Cannot create layer '" + name + "': invalid encoder config '" + encoderConfig + "'");
		}
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);
		return streamNode(sdb.createLayer(tx, name, SimplePointEncoder.class, SimplePointLayer.class,
						IndexRegistry.INSTANCE.getRegisteredIndices().get(indexType), encoderConfig, indexConfig,
						selectCRS(hintCRSName(crsName, encoderConfig)))
				.getLayerNode(tx));
	}

	@Procedure(value = "spatial.addNativePointLayer", mode = WRITE)
	@Description("Adds a new native point layer, returns the layer root node")
	public Stream<NodeResult> addNativePointLayer(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "indexType", defaultValue = INDEX_TYPE_RTREE, description = DOC_INDEX_TYPE) String indexType,
			@Name(value = "crsName", defaultValue = UNSET_CRS_NAME, description = DOC_CRS) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);
		return streamNode(sdb.createLayer(tx, name, NativePointEncoder.class, SimplePointLayer.class,
						IndexRegistry.INSTANCE.getRegisteredIndices().get(indexType), null, indexConfig, selectCRS(crsName)
				)
				.getLayerNode(tx));
	}

	@Procedure(value = "spatial.addNativePointLayerGeohash", mode = WRITE)
	@Description("Adds a new native point layer with geohash based index, returns the layer root node")
	public Stream<NodeResult> addNativePointLayerGeohash(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "crsName", defaultValue = WGS84_CRS_NAME, description = DOC_CRS) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);
		return streamNode(sdb.createLayer(tx, name, NativePointEncoder.class, SimplePointLayer.class,
						LayerGeohashPointIndex.class, null, indexConfig, selectCRS(crsName))
				.getLayerNode(tx));
	}

	@Procedure(value = "spatial.addNativePointLayerZOrder", mode = WRITE)
	@Description("Adds a new native point layer with z-order curve based index, returns the layer root node")
	public Stream<NodeResult> addNativePointLayerZOrder(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);
		return streamNode(sdb.createLayer(tx, name, NativePointEncoder.class, SimplePointLayer.class,
						LayerZOrderPointIndex.class, null, indexConfig, DefaultGeographicCRS.WGS84)
				.getLayerNode(tx));
	}

	@Procedure(value = "spatial.addNativePointLayerHilbert", mode = WRITE)
	@Description("Adds a new native point layer with hilbert curve based index, returns the layer root node")
	public Stream<NodeResult> addNativePointLayerHilbert(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig
	) {
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);
		return streamNode(sdb.createLayer(tx, name, NativePointEncoder.class, SimplePointLayer.class,
						LayerHilbertPointIndex.class, null, indexConfig, DefaultGeographicCRS.WGS84)
				.getLayerNode(tx));
	}

	@Deprecated
	@Procedure(value = "spatial.addNativePointLayerXY", mode = WRITE, deprecatedBy = "spatial.addPointLayerXY")
	@Description("Adds a new point layer with the given properties for x and y coordinates, returns the layer root node")
	public Stream<NodeResult> addNativePointLayer(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "xProperty", description = "The name of the property with the x coordinate") String xProperty,
			@Name(value = "yProperty", description = "The name of the property with the y coordinate") String yProperty,
			@Name(value = "indexType", defaultValue = INDEX_TYPE_RTREE, description = DOC_INDEX_TYPE) String indexType,
			@Name(value = "crsName", defaultValue = UNSET_CRS_NAME, description = DOC_CRS) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig) {
		if (xProperty == null || yProperty == null) {
			throw new IllegalArgumentException(
					"Cannot create layer '" + name + "': Missing encoder config values: xProperty[" + xProperty
							+ "], yProperty[" + yProperty + "]");
		}
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);
		return streamNode(sdb.createLayer(tx, name, SimplePointEncoder.class, SimplePointLayer.class,
						IndexRegistry.INSTANCE.getRegisteredIndices().get(indexType),
						SpatialDatabaseService.makeEncoderConfig(xProperty, yProperty), indexConfig,
						selectCRS(hintCRSName(crsName, yProperty)))
				.getLayerNode(tx));
	}

	@Procedure(value = "spatial.addNativePointLayerWithConfig", mode = WRITE)
	@Description("Adds a new native point layer with the given configuration, returns the layer root node")
	public Stream<NodeResult> addNativePointLayerWithConfig(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "encoderConfig", description = DOC_ENCODER_CONFIG) String encoderConfig,
			@Name(value = "indexType", defaultValue = INDEX_TYPE_RTREE, description = DOC_INDEX_TYPE) String indexType,
			@Name(value = "crsName", defaultValue = UNSET_CRS_NAME, description = DOC_CRS) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig
	) {
		if (encoderConfig.indexOf(':') <= 0) {
			throw new IllegalArgumentException(
					"Cannot create layer '" + name + "': invalid encoder config '" + encoderConfig + "'");
		}
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);

		CoordinateReferenceSystem crs = selectCRS(hintCRSName(crsName, encoderConfig));
		Layer layer = sdb.createLayer(tx, name, NativePointEncoder.class, SimplePointLayer.class,
				IndexRegistry.INSTANCE.getRegisteredIndices().get(indexType),
				encoderConfig,
				indexConfig, crs);
		return streamNode(layer.getLayerNode(tx));
	}

	public static final String UNSET_CRS_NAME = "";
	public static final String UNSET_INDEX_CONFIG = "";

	/**
	 * Currently this only supports the string 'WGS84', for the convenience of procedure users.
	 * This should be expanded with CRS table lookup.
	 *
	 * @param name CRS name
	 * @return null or WGS84
	 */
	public static CoordinateReferenceSystem selectCRS(String name) {
		if (name == null) {
			return null;
		}
		return switch (name.toLowerCase()) {
			case WGS84_CRS_NAME -> DefaultGeographicCRS.WGS84;
			case UNSET_CRS_NAME -> null;
			default -> throw new IllegalArgumentException("Unsupported CRS name: " + name);
		};
	}

	private static String hintCRSName(String crsName, String hint) {
		if (crsName.equals(UNSET_CRS_NAME) && hint.toLowerCase().contains("lat")) {
			crsName = WGS84_CRS_NAME;
		}
		return crsName;
	}

	@Procedure(value = "spatial.addLayerWithEncoder", mode = WRITE)
	@Description("Adds a new layer with the given encoder class and configuration, returns the layer root node")
	public Stream<NodeResult> addLayerWithEncoder(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "encoder", description = DOC_ENCODER_NAME) String encoderName,
			@Name(value = "encoderConfig", description = DOC_ENCODER_CONFIG) String encoderConfig,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig) {
		Class<? extends GeometryEncoder> encoderClass = GeometryEncoderRegistry.INSTANCE.getRegisteredEncoders()
				.get(encoderName);
		if (encoderClass == null) {
			throw new IllegalArgumentException(
					"Cannot create layer '" + name + "': invalid encoder '" + encoderName + "'");
		}
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);
		Class<? extends Layer> layerClass = LayerTypePresetRegistry.INSTANCE.suggestLayerClassForEncoder(encoderClass);
		return streamNode(sdb
				.createLayer(tx, name, encoderClass, layerClass, LayerRTreeIndex.class, encoderConfig, indexConfig)
				.getLayerNode(tx));
	}

	@Procedure(value = "spatial.addLayer", mode = WRITE)
	@Description("Adds a new layer with the given `type` (see `spatial.layerTypes`) and configuration. Returns the layers root node.")
	public Stream<NodeResult> addLayerOfType(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "type", description = DOC_LAYER_TYPE) String type,
			@Name(value = "encoderConfig", description = DOC_ENCODER_CONFIG) String encoderConfig,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig) {
		RegisteredLayerType registeredLayerType = LayerTypePresetRegistry.INSTANCE.getRegisteredLayerType(type);
		if (registeredLayerType == null) {
			throw new IllegalArgumentException(
					"Cannot create layer '" + name + "': unknown type '" + type + "' - supported types are "
							+ LayerTypePresetRegistry.INSTANCE.getRegisteredLayerPresets().keySet());
		}
		SpatialDatabaseService sdb = spatial();
		assertLayerDoesNotExist(sdb, name);
		return streamNode(
				sdb.getOrCreateRegisteredTypeLayer(tx, name, registeredLayerType, encoderConfig, indexConfig, false)
						.getLayerNode(tx));
	}

	private static Stream<NodeResult> streamNode(Node node) {
		return Stream.of(new NodeResult(node));
	}

	private static Stream<NodeIdResult> streamNode(String nodeId) {
		return Stream.of(new NodeIdResult(nodeId));
	}

	@Procedure(value = "spatial.addWKTLayer", mode = WRITE)
	@Description("Adds a new WKT layer with the given node property to hold the WKT string, returns the layer root node")
	public Stream<NodeResult> addWKTLayer(
			@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "nodePropertyName", description = "The property from which the WKT will be read") String nodePropertyName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG, description = DOC_INDEX_CONFIG) String indexConfig) {
		return addLayerOfType(name, "WKT", nodePropertyName, indexConfig);
	}

	@Procedure(value = "spatial.layer", mode = READ)
	@Description("Returns the layer root node for the given layer `name`")
	public Stream<NodeResult> getLayer(@Name(value = "name", description = DOC_LAYER_NAME) String name) {
		return streamNode(getLayerOrThrow(tx, spatial(), name, true).getLayerNode(tx));
	}

	@Procedure(value = "spatial.layerMeta", mode = READ)
	@Description("Returns the layer details for the given layer `name`")
	public Stream<LayerMetaResult> getLayerMeta(@Name(value = "name", description = DOC_LAYER_NAME) String name) {
		var layer = getLayerOrThrow(tx, spatial(), name, true);
		var layerNode = layer.getLayerNode(tx);
		var crs = layerNode.hasProperty(PROP_CRS) ? (String) layerNode.getProperty(PROP_CRS) : null;
		var geometryTypeId = layer.getGeometryType(tx);
		String geometryType = null;
		if (geometryTypeId != null) {
			var geometryClass = SpatialDatabaseService.convertGeometryTypeToJtsClass(geometryTypeId);
			geometryType = geometryClass.getName();
		}
		var extraProperties = new HashMap<String, String>();
		layer.getExtraProperties(tx).forEach((s, aClass) -> extraProperties.put(s, aClass.getName()));
		return Stream.of(
				new LayerMetaResult(
						name,
						geometryType,
						crs,
						layer.getGeometryEncoder().hasComplexAttributes(),
						extraProperties

				)
		);
	}

	@Procedure(value = "spatial.getFeatureAttributes", mode = READ)
	@Description("Returns feature attributes of the given layer")
	public Stream<FeatureAttributeResult> getFeatureAttributes(
			@Name(value = "name", description = DOC_LAYER_NAME) String name) {
		Layer layer = getLayerOrThrow(tx, spatial(), name, true);
		return layer.getExtraProperties(tx)
				.entrySet()
				.stream()
				.map(entry -> new FeatureAttributeResult(entry.getKey(), entry.getValue().getName()));
	}

	@Procedure(value = "spatial.getFeatureCount", mode = READ)
	@Description("Returns the number of features in the layer")
	public Stream<CountResult> getLayerCount(
			@Name(value = "name", description = DOC_LAYER_NAME) String name) {
		Layer layer = getLayerOrThrow(tx, spatial(), name, true);
		int count = layer.getIndex().count(tx);
		return Stream.of(new CountResult(count));
	}

	@Procedure(value = "spatial.getLayerBoundingBox", mode = READ)
	@Description("Returns the bounding box of the layer")
	public Stream<BoundingBoxResult> getLayerBoundingBox(
			@Name(value = "name", description = DOC_LAYER_NAME) String name
	) {
		Layer layer = getLayerOrThrow(tx, spatial(), name, true);
		org.neo4j.spatial.api.Envelope envelope = layer.getIndex().getBoundingBox(tx);
		CoordinateReferenceSystem crs = layer.getCoordinateReferenceSystem(tx);
		String crsName = crs != null ? crs.getName().toString() : null;
		return Stream.of(new BoundingBoxResult(
				envelope.getMinX(), envelope.getMinY(),
				envelope.getMaxX(), envelope.getMaxY(),
				crsName));
	}

	@Procedure(
			value = "spatial.setFeatureAttributes", mode = WRITE,
			deprecatedBy = "feature attributes are now automatically discovered when a new node is added to the index"
	)
	@Description("Sets the feature attributes of the given layer")
	public Stream<NodeResult> setFeatureAttributes(@Name(value = "name", description = DOC_LAYER_NAME) String name,
			@Name(value = "attributeNames", description = "The attributes to set") List<String> attributeNames) {
		EditableLayerImpl layer = getEditableLayerOrThrow(tx, spatial(), name);
		layer.setExtraPropertyNames(attributeNames.toArray(new String[0]), tx);
		return streamNode(layer.getLayerNode(tx));
	}

	@Procedure(value = "spatial.removeLayer", mode = WRITE)
	@Description("Removes the given layer")
	public void removeLayer(@Name(value = "name", description = DOC_LAYER_NAME) String name) {
		SpatialDatabaseService sdb = spatial();
		sdb.deleteLayer(tx, name, new ProgressLoggingListener("Deleting layer '" + name + "'", log, Level.INFO));
	}

	@Procedure(value = "spatial.addNode", mode = WRITE)
	@Description("Adds the given node to the layer, returns the geometry-node")
	public Stream<NodeResult> addNodeToLayer(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "node", description = "the node to be added to the index") Node node) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		Node geomNode = layer.add(tx, node).getGeomNode();
		layer.finalizeTransaction(tx);
		return streamNode(geomNode);
	}

	@Procedure(value = "spatial.addNodes", mode = WRITE)
	@Description("Adds the given nodes list to the layer, returns the count")
	public Stream<CountResult> addNodesToLayer(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "nodes", description = "the nodes to be added to the index") List<Node> nodes) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		int count = layer.addAll(tx, nodes);
		layer.finalizeTransaction(tx);
		return Stream.of(new CountResult(count));
	}

	@Deprecated
	@Procedure(value = "spatial.addNode.byId", mode = WRITE, deprecatedBy = "spatial.addNode")
	@Description("Adds the given node to the layer, returns the geometry-node")
	public Stream<NodeResult> addNodeIdToLayer(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "nodeId", description = "The elementId of the node to add") String nodeId) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		Node geomNode = layer.add(tx, tx.getNodeByElementId(nodeId)).getGeomNode();
		layer.finalizeTransaction(tx);
		return streamNode(geomNode);
	}

	@Deprecated
	@Procedure(value = "spatial.addNodes.byId", mode = WRITE, deprecatedBy = "spatial.addNodes")
	@Description("Adds the given nodes list to the layer, returns the count")
	public Stream<CountResult> addNodeIdsToLayer(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "nodeIds", description = "A list of elementIds of the nodes to add") List<String> nodeIds) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		List<Node> nodes = nodeIds.stream().map(id -> tx.getNodeByElementId(id)).collect(Collectors.toList());
		int count = layer.addAll(tx, nodes);
		layer.finalizeTransaction(tx);
		return Stream.of(new CountResult(count));
	}

	@Procedure(value = "spatial.removeNode", mode = WRITE)
	@Description("Removes the given node from the layer, returns the geometry-node")
	public Stream<NodeIdResult> removeNodeFromLayer(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "node", description = "The node to remove from the index") Node node) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		layer.removeFromIndex(tx, node.getElementId());
		layer.finalizeTransaction(tx);
		return streamNode(node.getElementId());
	}

	@Procedure(value = "spatial.removeNodes", mode = WRITE)
	@Description("Removes the given nodes from the layer, returns the count of nodes removed")
	public Stream<CountResult> removeNodesFromLayer(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "nodes", description = "The nodes to remove from the index") List<Node> nodes) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		//TODO optimize bulk node removal from RTree like we have done for node additions
		int before = layer.getIndex().count(tx);
		for (Node node : nodes) {
			layer.removeFromIndex(tx, node.getElementId());
		}
		int after = layer.getIndex().count(tx);
		layer.finalizeTransaction(tx);
		return Stream.of(new CountResult(before - after));
	}

	@Deprecated
	@Procedure(value = "spatial.removeNode.byId", mode = WRITE, deprecatedBy = "spatial.removeNode")
	@Description("Removes the given node from the layer, returns the geometry-node")
	public Stream<NodeIdResult> removeNodeFromLayer(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "nodeId", description = "The elementId of the node to remove") String nodeId) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		layer.removeFromIndex(tx, nodeId);
		layer.finalizeTransaction(tx);
		return streamNode(nodeId);
	}

	@Deprecated
	@Procedure(value = "spatial.removeNodes.byId", mode = WRITE, deprecatedBy = "spatial.removeNodes")
	@Description("Removes the given nodes from the layer, returns the count of nodes removed")
	public Stream<CountResult> removeNodeIdsFromLayer(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "nodeIds", description = "A list of elementIds of the nodes to remove") List<String> nodeIds) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		//TODO optimize bulk node removal from RTree like we have done for node additions
		int before = layer.getIndex().count(tx);
		for (String nodeId : nodeIds) {
			layer.removeFromIndex(tx, nodeId);
		}
		int after = layer.getIndex().count(tx);
		layer.finalizeTransaction(tx);
		return Stream.of(new CountResult(before - after));
	}

	@Procedure(value = "spatial.addWKT", mode = WRITE)
	@Description("Adds the given WKT string to the layer, returns the created geometry node")
	public Stream<NodeResult> addGeometryWKTToLayer(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "geometry", description = "A WKT to add to the index") String geometryWKT) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		WKTReader reader = new WKTReader(layer.getGeometryFactory());
		Node node = addGeometryWkt(layer, reader, geometryWKT);
		layer.finalizeTransaction(tx);
		return streamNode(node);
	}

	@Procedure(value = "spatial.updateWKT", mode = WRITE)
	@Description("Updates a node with the geometry defined by the given WKT, returns the node")
	public Stream<NodeResult> addGeometryWKTToLayer(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "node", description = "The indexed node to update") Node node,
			@Name(value = "geometry", description = "The WKT to set on the given node") String geometryWKT) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		WKTReader reader = new WKTReader(layer.getGeometryFactory());
		try {
			Geometry geometry = reader.read(geometryWKT);
			layer.update(tx, node.getElementId(), geometry);
			streamNode(node);
		} catch (ParseException e) {
			throw new RuntimeException("Error parsing geometry: " + geometryWKT, e);
		} finally {
			layer.finalizeTransaction(tx);
		}
		return streamNode(node);
	}

	@Procedure(value = "spatial.addWKTs", mode = WRITE)
	@Description("Adds the given WKT string list to the layer, returns the created geometry nodes")
	public Stream<NodeResult> addGeometryWKTsToLayer(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "geometry", description = "A list of WKTs to add to the index") List<String> geometryWKTs) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		WKTReader reader = new WKTReader(layer.getGeometryFactory());
		return geometryWKTs.stream().map(geometryWKT -> addGeometryWkt(layer, reader, geometryWKT))
				.map(NodeResult::new)
				.onClose(() -> layer.finalizeTransaction(tx));
	}

	private Node addGeometryWkt(EditableLayer layer, WKTReader reader, String geometryWKT) {
		try {
			Geometry geometry = reader.read(geometryWKT);
			return layer.add(tx, geometry).getGeomNode();
		} catch (ParseException e) {
			throw new RuntimeException("Error parsing geometry: " + geometryWKT, e);
		}
	}

	@Procedure(value = "spatial.importShapefileToLayer", mode = WRITE)
	@Description("Imports the the provided shape-file from URI to the given layer, returns the count of data added")
	public Stream<CountResult> importShapefile(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "uri", description = DOC_URI) String uri) throws IOException {
		EditableLayerImpl layer = getEditableLayerOrThrow(tx, spatial(), name);
		List<Node> nodes = importShapefileToLayer(uri, layer, 1000);
		layer.finalizeTransaction(tx);
		return Stream.of(new CountResult(nodes.size()));
	}

	@Procedure(value = "spatial.importShapefile", mode = WRITE)
	@Description("Imports the the provided shape-file from URI to a layer of the same name, returns the count of data added")
	public Stream<CountResult> importShapefile(
			@Name(value = "uri", description = DOC_URI) String uri) throws IOException {
		return Stream.of(new CountResult(importShapefileToLayer(uri, null, 1000).size()));
	}

	private List<Node> importShapefileToLayer(String shpPath, EditableLayerImpl layer, int commitInterval)
			throws IOException {
		// remove extension
		if (shpPath.toLowerCase().endsWith(".shp")) {
			shpPath = shpPath.substring(0, shpPath.lastIndexOf("."));
		}

		ShapefileImporter importer = new ShapefileImporter(db,
				new ProgressLoggingListener("Importing " + shpPath, log, Level.DEBUG), commitInterval);
		if (layer == null) {
			String layerName = shpPath.substring(shpPath.lastIndexOf(File.separator) + 1);
			return importer.importFile(shpPath, layerName);
		}
		return importer.importFile(shpPath, layer, Charset.defaultCharset());
	}

	@Procedure(value = "spatial.importOSMToLayer", mode = WRITE)
	@Description("Imports the the provided osm-file from URI to a layer, returns the count of data added")
	public Stream<CountResult> importOSM(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String layerName,
			@Name(value = "uri", description = DOC_URI) String uri)
			throws InterruptedException {
		// Delegate finding the layer to the inner thread, so we do not pollute the procedure transaction with anything that might conflict.
		// Since the procedure transaction starts before, and ends after, all inner transactions.
		BiFunction<Transaction, String, OSMLayer> layerFinder = (tx, name) -> (OSMLayer) getEditableLayerOrThrow(tx,
				spatial(), name);
		return Stream.of(new CountResult(importOSMToLayer(uri, layerName, layerFinder)));
	}

	@Procedure(value = "spatial.importOSM", mode = WRITE)
	@Description("Imports the the provided osm-file from URI to a layer of the same name, returns the count of data added")
	public Stream<CountResult> importOSM(
			@Name(value = "uri", description = DOC_URI) String uri)
			throws InterruptedException {
		String layerName = uri.substring(uri.lastIndexOf(File.separator) + 1);
		assertLayerDoesNotExist(spatial(), layerName);
		// Delegate creating the layer to the inner thread, so we do not pollute the procedure transaction with anything that might conflict.
		// Since the procedure transaction starts before, and ends after, all inner transactions.
		BiFunction<Transaction, String, OSMLayer> layerMaker = (tx, name) -> (OSMLayer) spatial().getOrCreateLayer(tx,
				name, OSMGeometryEncoder.class, OSMLayer.class, "", false);
		return Stream.of(new CountResult(importOSMToLayer(uri, layerName, layerMaker)));
	}

	private long importOSMToLayer(String osmPath, String layerName,
			BiFunction<Transaction, String, OSMLayer> layerMaker) throws InterruptedException {
		// add extension
		if (!osmPath.toLowerCase().endsWith(".osm")) {
			osmPath = osmPath + ".osm";
		}
		OSMImportRunner runner = new OSMImportRunner(api, ktx.securityContext(), osmPath, layerName, layerMaker, log,
				Level.DEBUG);
		Thread importerThread = new Thread(runner);
		importerThread.start();
		importerThread.join();
		return runner.getResult();
	}

	private static class OSMImportRunner implements Runnable {

		private final GraphDatabaseAPI db;
		private final String osmPath;
		private final String layerName;
		private final BiFunction<Transaction, String, OSMLayer> layerMaker;
		private final Log log;
		private final Level level;
		private final SecurityContext securityContext;
		private Exception e;
		private long rc = -1;

		OSMImportRunner(GraphDatabaseAPI db, SecurityContext securityContext, String osmPath, String layerName,
				BiFunction<Transaction, String, OSMLayer> layerMaker, Log log, Level level) {
			this.db = db;
			this.osmPath = osmPath;
			this.layerName = layerName;
			this.layerMaker = layerMaker;
			this.log = log;
			this.level = level;
			this.securityContext = securityContext;
		}

		long getResult() {
			if (e == null) {
				return rc;
			}
			throw new RuntimeException(
					"Failed to import " + osmPath + " to layer '" + layerName + "': " + e.getMessage(), e);
		}

		@Override
		public void run() {
			// Create the layer in the same thread as doing the import, otherwise we have an outer thread doing a creation,
			// and the inner thread repeating it, resulting in duplicates
			try (Transaction tx = db.beginTransaction(KernelTransaction.Type.EXPLICIT, securityContext)) {
				layerMaker.apply(tx, layerName);
				tx.commit();
			}
			OSMImporter importer = new OSMImporter(layerName,
					new ProgressLoggingListener("Importing " + osmPath, log, level));
			try {
				// Provide the security context for all inner transactions that will be made during import
				importer.setSecurityContext(securityContext);
				// import using multiple, serial inner transactions (using the security context of the outer thread)
				importer.importFile(db, osmPath, false, 10000);
				// Re-index using inner transactions (using the security context of the outer thread)
				rc = importer.reIndex(db, 10000, false);
			} catch (Exception e) {
				log.error("Error running OSMImporter: " + e.getMessage());
				this.e = e;
			}
		}
	}

	@Procedure(value = "spatial.bbox", mode = READ)
	@Description(
			"Finds all geometry nodes in the given layer within the lower left and upper right coordinates of a box. "
					+ DOC_COORDINATE)
	public Stream<NodeResult> findGeometriesInBBox(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "min", description = "The lower left coordinate") Object min,
			@Name(value = "max", description = "The upper right coordinate") Object max) {
		Layer layer = getLayerOrThrow(tx, spatial(), name, true);
		// TODO why a SearchWithin and not a SearchIntersectWindow?
		Envelope envelope = new Envelope(toCoordinate(min), toCoordinate(max));
		return GeoPipeline
				.startWithinSearch(tx, layer, layer.getGeometryFactory().toGeometry(envelope))
				.stream().map(GeoPipeFlow::getGeomNode).map(NodeResult::new);
	}

	@Procedure(value = "spatial.cql", mode = READ)
	@Description("Finds all geometry nodes in the given layer that matches the given CQL")
	public Stream<NodeResult> findGeometriesByCQL(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "ecql", description = "The [ECQL](https://docs.geoserver.org/latest/en/user/filter/ecql_reference.html) to find / filter nodes of the layer") String ecql
	) {
		Layer layer = getLayerOrThrow(tx, spatial(), name, true);
		return GeoPipeline
				.startECQL(tx, layer, ecql)
				.stream().map(GeoPipeFlow::getGeomNode).map(NodeResult::new);
	}

	@Procedure(value = "spatial.closest", mode = READ)
	@Description("Finds all geometry nodes in the layer within the distance to the given coordinate")
	public Stream<NodeResult> findClosestGeometries(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "coordinate", description = DOC_COORDINATE) Object coordinate,
			@Name(value = "distanceInKm", description = "The distance in kilometers within which to search for geometries") double distanceInKm) {
		Layer layer = getLayerOrThrow(tx, spatial(), name, true);
		GeometryFactory factory = layer.getGeometryFactory();
		Point point = factory.createPoint(toCoordinate(coordinate));
		List<SpatialTopologyUtils.PointResult> edgeResults = SpatialTopologyUtils.findClosestEdges(tx, point, layer,
				distanceInKm);
		return edgeResults.stream().map(e -> e.getValue().getGeomNode()).map(NodeResult::new);
	}

	@Procedure(value = "spatial.withinDistance", mode = READ)
	@Description("Returns all geometry nodes and their ordered distance in the layer within the distance to the given coordinate")
	public Stream<NodeDistanceResult> findGeometriesWithinDistance(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "coordinate", description = DOC_COORDINATE) Object coordinate,
			@Name(value = "distanceInKm", description = "The distance in kilometers within which to search for geometries") double distanceInKm) {

		Layer layer = getLayerOrThrow(tx, spatial(), name, true);
		return GeoPipeline
				.startNearestNeighborLatLonSearch(tx, layer, toCoordinate(coordinate), distanceInKm)
				.sort(OrthodromicDistance.DISTANCE)
				.stream().map(r -> {
					double distance = r.hasProperty(tx, OrthodromicDistance.DISTANCE) ? ((Number) r.getProperty(tx,
							OrthodromicDistance.DISTANCE)).doubleValue() : -1;
					return new NodeDistanceResult(r.getGeomNode(), distance);
				});
	}

	@Deprecated
	@Procedure("spatial.asGeometry")
	@Description("Returns a geometry object as the Neo4j geometry type, to be passed to other procedures or returned to a client")
	public Stream<GeometryResult> asGeometryProc(
			@Name("geometry") Object geometry) {

		return Stream.of(geometry).map(geom -> new GeometryResult(toNeo4jGeometry(null, geom)));
	}

	@Deprecated
	@Procedure(value = "spatial.asExternalGeometry", deprecatedBy = "spatial.asGeometry")
	@Description("Returns a geometry object as an external geometry type to be returned to a client")
	// This only existed temporarily because the other method, asGeometry, returned the wrong type due to a bug in Neo4j 3.0
	public Stream<GeometryResult> asExternalGeometry(
			@Name("geometry") Object geometry) {

		return Stream.of(geometry).map(geom -> new GeometryResult(toNeo4jGeometry(null, geom)));
	}

	@Procedure(value = "spatial.intersects", mode = READ)
	@Description("Returns all geometry nodes that intersect the given geometry (shape, polygon) in the layer")
	public Stream<NodeResult> findGeometriesIntersecting(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String name,
			@Name(value = "geometry", description = DOC_JTS_GEOMETRY) Object geometry) {

		Layer layer = getLayerOrThrow(tx, spatial(), name, true);
		return GeoPipeline
				.startIntersectSearch(tx, layer, toJTSGeometry(layer, geometry))
				.stream().map(GeoPipeFlow::getGeomNode).map(NodeResult::new);
	}

	private static Geometry toJTSGeometry(Layer layer, Object value) {
		GeometryFactory factory = layer.getGeometryFactory();
		if (value instanceof org.neo4j.graphdb.spatial.Point point) {
			double[] coord = point.getCoordinate().getCoordinate();
			return factory.createPoint(new Coordinate(coord[0], coord[1]));
		}
		if (value instanceof String) {
			WKTReader reader = new WKTReader(factory);
			try {
				return reader.read((String) value);
			} catch (ParseException e) {
				throw new IllegalArgumentException("Invalid WKT: " + e.getMessage());
			}
		}
		Map<String, Object> latLon = null;
		if (value instanceof Entity) {
			latLon = ((Entity) value).getProperties("latitude", "longitude", "lat", "lon");
		}
		if (value instanceof Map) {
			//noinspection unchecked
			latLon = (Map<String, Object>) value;
		}
		Coordinate coord = toCoordinate(latLon);
		if (coord != null) {
			return factory.createPoint(coord);
		}
		throw new RuntimeException("Can't convert " + value + " to a geometry");
	}

	private static EditableLayerImpl getEditableLayerOrThrow(Transaction tx, SpatialDatabaseService spatial,
			String name) {
		return (EditableLayerImpl) getLayerOrThrow(tx, spatial, name, false);
	}

	private void assertLayerDoesNotExist(SpatialDatabaseService sdb, String name) {
		if (sdb.getLayer(tx, name, true) != null) {
			throw new IllegalArgumentException("Layer already exists: '" + name + "'");
		}
	}
}
