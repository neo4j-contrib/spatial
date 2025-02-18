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

import static org.neo4j.gis.spatial.SpatialDatabaseService.RTREE_INDEX_NAME;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
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
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.EditableLayerImpl;
import org.neo4j.gis.spatial.GeometryEncoder;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.ShapefileImporter;
import org.neo4j.gis.spatial.SimplePointLayer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.SpatialTopologyUtils;
import org.neo4j.gis.spatial.WKBGeometryEncoder;
import org.neo4j.gis.spatial.WKTGeometryEncoder;
import org.neo4j.gis.spatial.encoders.NativePointEncoder;
import org.neo4j.gis.spatial.encoders.SimpleGraphEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.encoders.SimplePropertyEncoder;
import org.neo4j.gis.spatial.index.LayerGeohashPointIndex;
import org.neo4j.gis.spatial.index.LayerHilbertPointIndex;
import org.neo4j.gis.spatial.index.LayerZOrderPointIndex;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.pipes.processing.OrthodromicDistance;
import org.neo4j.gis.spatial.rtree.ProgressLoggingListener;
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

	public record StringResult(String name) {

	}

	public record NodeDistanceResult(Node node, double distance) {

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

	private static final Map<String, Class<? extends GeometryEncoder>> encoderClasses = new HashMap<>();

	static {
		populateEncoderClasses();
	}

	private static void populateEncoderClasses() {
		encoderClasses.clear();
		// TODO: Make this auto-find classes that implement GeometryEncoder
		for (Class<? extends GeometryEncoder> cls : Arrays.asList(
				SimplePointEncoder.class, OSMGeometryEncoder.class, SimplePropertyEncoder.class,
				WKTGeometryEncoder.class, WKBGeometryEncoder.class, SimpleGraphEncoder.class,
				NativePointEncoder.class
		)) {
			String name = cls.getSimpleName();
			encoderClasses.put(name, cls);
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
			Layer layer = sdb.getLayer(tx, name);
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
			Layer layer = sdb.getLayer(tx, name);
			if (layer != null) {
				builder.accept(new NameResult(name, layer.getSignature()));
			}
		}
		return builder.build();
	}

	@Procedure("spatial.layerTypes")
	@Description("Returns the different registered layer types")
	public Stream<NameResult> getAllLayerTypes() {
		Stream.Builder<NameResult> builder = Stream.builder();
		for (Map.Entry<String, String> entry : SpatialDatabaseService.getRegisteredLayerTypes().entrySet()) {
			builder.accept(new NameResult(entry.getKey(), entry.getValue()));
		}
		return builder.build();
	}

	@Procedure(value = "spatial.addPointLayer", mode = WRITE)
	@Description("Adds a new simple point layer, returns the layer root node")
	public Stream<NodeResult> addSimplePointLayer(
			@Name("name") String name,
			@Name(value = "indexType", defaultValue = RTREE_INDEX_NAME) String indexType,
			@Name(value = "crsName", defaultValue = UNSET_CRS_NAME) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig
	) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			return streamNode(sdb.createLayer(tx, name, SimplePointEncoder.class, SimplePointLayer.class,
							SpatialDatabaseService.resolveIndexClass(indexType), null, indexConfig, selectCRS(crsName))
					.getLayerNode(tx));
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	@Procedure(value = "spatial.addPointLayerGeohash", mode = WRITE)
	@Description("Adds a new simple point layer with geohash based index, returns the layer root node")
	public Stream<NodeResult> addSimplePointLayerGeohash(
			@Name("name") String name,
			@Name(value = "crsName", defaultValue = WGS84_CRS_NAME) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			return streamNode(sdb.createLayer(tx, name, SimplePointEncoder.class, SimplePointLayer.class,
							LayerGeohashPointIndex.class, null, indexConfig, selectCRS(crsName))
					.getLayerNode(tx));
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	@Procedure(value = "spatial.addPointLayerZOrder", mode = WRITE)
	@Description("Adds a new simple point layer with z-order curve based index, returns the layer root node")
	public Stream<NodeResult> addSimplePointLayerZOrder(@Name("name") String name,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			return streamNode(sdb.createLayer(tx, name, SimplePointEncoder.class, SimplePointLayer.class,
							LayerZOrderPointIndex.class, null, indexConfig, DefaultGeographicCRS.WGS84)
					.getLayerNode(tx));
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	@Procedure(value = "spatial.addPointLayerHilbert", mode = WRITE)
	@Description("Adds a new simple point layer with hilbert curve based index, returns the layer root node")
	public Stream<NodeResult> addSimplePointLayerHilbert(
			@Name("name") String name,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig
	) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			return streamNode(sdb.createLayer(tx, name, SimplePointEncoder.class, SimplePointLayer.class,
							LayerHilbertPointIndex.class, null, indexConfig, DefaultGeographicCRS.WGS84)
					.getLayerNode(tx));
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	@Procedure(value = "spatial.addPointLayerXY", mode = WRITE)
	@Description("Adds a new simple point layer with the given properties for x and y coordinates, returns the layer root node")
	public Stream<NodeResult> addSimplePointLayer(
			@Name("name") String name,
			@Name("xProperty") String xProperty,
			@Name("yProperty") String yProperty,
			@Name(value = "indexType", defaultValue = RTREE_INDEX_NAME) String indexType,
			@Name(value = "crsName", defaultValue = UNSET_CRS_NAME) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			if (xProperty != null && yProperty != null) {
				return streamNode(sdb.createLayer(tx, name, SimplePointEncoder.class, SimplePointLayer.class,
								SpatialDatabaseService.resolveIndexClass(indexType),
								SpatialDatabaseService.makeEncoderConfig(xProperty, yProperty), indexConfig,
								selectCRS(hintCRSName(crsName, yProperty)))
						.getLayerNode(tx));
			}
			throw new IllegalArgumentException(
					"Cannot create layer '" + name + "': Missing encoder config values: xProperty[" + xProperty
							+ "], yProperty[" + yProperty + "]");
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	@Procedure(value = "spatial.addPointLayerWithConfig", mode = WRITE)
	@Description("Adds a new simple point layer with the given configuration, returns the layer root node")
	public Stream<NodeResult> addSimplePointLayerWithConfig(
			@Name("name") String name,
			@Name("encoderConfig") String encoderConfig,
			@Name(value = "indexType", defaultValue = RTREE_INDEX_NAME) String indexType,
			@Name(value = "crsName", defaultValue = UNSET_CRS_NAME) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			if (encoderConfig.indexOf(':') > 0) {
				return streamNode(sdb.createLayer(tx, name, SimplePointEncoder.class, SimplePointLayer.class,
								SpatialDatabaseService.resolveIndexClass(indexType), encoderConfig, indexConfig,
								selectCRS(hintCRSName(crsName, encoderConfig)))
						.getLayerNode(tx));
			}
			throw new IllegalArgumentException(
					"Cannot create layer '" + name + "': invalid encoder config '" + encoderConfig + "'");
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	@Procedure(value = "spatial.addNativePointLayer", mode = WRITE)
	@Description("Adds a new native point layer, returns the layer root node")
	public Stream<NodeResult> addNativePointLayer(
			@Name("name") String name,
			@Name(value = "indexType", defaultValue = RTREE_INDEX_NAME) String indexType,
			@Name(value = "crsName", defaultValue = UNSET_CRS_NAME) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			return streamNode(sdb.createLayer(tx, name, NativePointEncoder.class, SimplePointLayer.class,
							SpatialDatabaseService.resolveIndexClass(indexType), null, indexConfig, selectCRS(crsName))
					.getLayerNode(tx));
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	@Procedure(value = "spatial.addNativePointLayerGeohash", mode = WRITE)
	@Description("Adds a new native point layer with geohash based index, returns the layer root node")
	public Stream<NodeResult> addNativePointLayerGeohash(
			@Name("name") String name,
			@Name(value = "crsName", defaultValue = WGS84_CRS_NAME) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			return streamNode(sdb.createLayer(tx, name, NativePointEncoder.class, SimplePointLayer.class,
							LayerGeohashPointIndex.class, null, indexConfig, selectCRS(crsName))
					.getLayerNode(tx));
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	@Procedure(value = "spatial.addNativePointLayerZOrder", mode = WRITE)
	@Description("Adds a new native point layer with z-order curve based index, returns the layer root node")
	public Stream<NodeResult> addNativePointLayerZOrder(@Name("name") String name,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			return streamNode(sdb.createLayer(tx, name, NativePointEncoder.class, SimplePointLayer.class,
							LayerZOrderPointIndex.class, null, indexConfig, DefaultGeographicCRS.WGS84)
					.getLayerNode(tx));
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	@Procedure(value = "spatial.addNativePointLayerHilbert", mode = WRITE)
	@Description("Adds a new native point layer with hilbert curve based index, returns the layer root node")
	public Stream<NodeResult> addNativePointLayerHilbert(@Name("name") String name,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig
	) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			return streamNode(sdb.createLayer(tx, name, NativePointEncoder.class, SimplePointLayer.class,
							LayerHilbertPointIndex.class, null, indexConfig, DefaultGeographicCRS.WGS84)
					.getLayerNode(tx));
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	@Procedure(value = "spatial.addNativePointLayerXY", mode = WRITE)
	@Description("Adds a new native point layer with the given properties for x and y coordinates, returns the layer root node")
	public Stream<NodeResult> addNativePointLayer(
			@Name("name") String name,
			@Name("xProperty") String xProperty,
			@Name("yProperty") String yProperty,
			@Name(value = "indexType", defaultValue = RTREE_INDEX_NAME) String indexType,
			@Name(value = "crsName", defaultValue = UNSET_CRS_NAME) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			if (xProperty != null && yProperty != null) {
				return streamNode(sdb.createLayer(tx, name, NativePointEncoder.class, SimplePointLayer.class,
								SpatialDatabaseService.resolveIndexClass(indexType),
								SpatialDatabaseService.makeEncoderConfig(xProperty, yProperty), indexConfig,
								selectCRS(hintCRSName(crsName, yProperty)))
						.getLayerNode(tx));
			}
			throw new IllegalArgumentException(
					"Cannot create layer '" + name + "': Missing encoder config values: xProperty[" + xProperty
							+ "], yProperty[" + yProperty + "]");
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	@Procedure(value = "spatial.addNativePointLayerWithConfig", mode = WRITE)
	@Description("Adds a new native point layer with the given configuration, returns the layer root node")
	public Stream<NodeResult> addNativePointLayerWithConfig(
			@Name("name") String name,
			@Name("encoderConfig") String encoderConfig,
			@Name(value = "indexType", defaultValue = RTREE_INDEX_NAME) String indexType,
			@Name(value = "crsName", defaultValue = UNSET_CRS_NAME) String crsName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			if (encoderConfig.indexOf(':') > 0) {
				return streamNode(sdb.createLayer(tx, name, NativePointEncoder.class, SimplePointLayer.class,
								SpatialDatabaseService.resolveIndexClass(indexType), encoderConfig, indexConfig,
								selectCRS(hintCRSName(crsName, encoderConfig)))
						.getLayerNode(tx));
			}
			throw new IllegalArgumentException(
					"Cannot create layer '" + name + "': invalid encoder config '" + encoderConfig + "'");
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	public static final String UNSET_CRS_NAME = "";
	public static final String UNSET_INDEX_CONFIG = "";
	public static final String WGS84_CRS_NAME = "wgs84";

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
			@Name("name") String name,
			@Name("encoder") String encoderClassName,
			@Name("encoderConfig") String encoderConfig,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			Class<? extends GeometryEncoder> encoderClass = encoderClasses.get(encoderClassName);
			Class<? extends Layer> layerClass = SpatialDatabaseService.suggestLayerClassForEncoder(encoderClass);
			if (encoderClass != null) {
				return streamNode(sdb
						.createLayer(tx, name, encoderClass, layerClass, null, encoderConfig, indexConfig)
						.getLayerNode(tx));
			}
			throw new IllegalArgumentException(
					"Cannot create layer '" + name + "': invalid encoder class '" + encoderClassName + "'");
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	@Procedure(value = "spatial.addLayer", mode = WRITE)
	@Description("Adds a new layer with the given type (see spatial().getAllLayerTypes) and configuration, returns the layer root node")
	public Stream<NodeResult> addLayerOfType(
			@Name("name") String name,
			@Name("type") String type,
			@Name("encoderConfig") String encoderConfig,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig) {
		SpatialDatabaseService sdb = spatial();
		Layer layer = sdb.getLayer(tx, name);
		if (layer == null) {
			Map<String, String> knownTypes = SpatialDatabaseService.getRegisteredLayerTypes();
			if (knownTypes.containsKey(type.toLowerCase())) {
				return streamNode(sdb.getOrCreateRegisteredTypeLayer(tx, name, type, encoderConfig, indexConfig)
						.getLayerNode(tx));
			}
			throw new IllegalArgumentException(
					"Cannot create layer '" + name + "': unknown type '" + type + "' - supported types are "
							+ knownTypes);
		}
		throw new IllegalArgumentException("Cannot create existing layer: " + name);
	}

	private static Stream<NodeResult> streamNode(Node node) {
		return Stream.of(new NodeResult(node));
	}

	private static Stream<NodeIdResult> streamNode(String nodeId) {
		return Stream.of(new NodeIdResult(nodeId));
	}

	@Procedure(value = "spatial.addWKTLayer", mode = WRITE)
	@Description("Adds a new WKT layer with the given node property to hold the WKT string, returns the layer root node")
	public Stream<NodeResult> addWKTLayer(@Name("name") String name,
			@Name("nodePropertyName") String nodePropertyName,
			@Name(value = "indexConfig", defaultValue = UNSET_INDEX_CONFIG) String indexConfig) {
		return addLayerOfType(name, "WKT", nodePropertyName, indexConfig);
	}

	@Procedure(value = "spatial.layer", mode = WRITE)
	@Description("Returns the layer root node for the given layer name")
	public Stream<NodeResult> getLayer(@Name("name") String name) {
		return streamNode(getLayerOrThrow(tx, spatial(), name).getLayerNode(tx));
	}

	@Procedure(value = "spatial.getFeatureAttributes", mode = WRITE)
	@Description("Returns feature attributes of the given layer")
	public Stream<StringResult> getFeatureAttributes(@Name("name") String name) {
		Layer layer = getLayerOrThrow(tx, spatial(), name);
		return Arrays.stream(layer.getExtraPropertyNames(tx)).map(StringResult::new);
	}

	@Procedure(value = "spatial.setFeatureAttributes", mode = WRITE)
	@Description("Sets the feature attributes of the given layer")
	public Stream<NodeResult> setFeatureAttributes(@Name("name") String name,
			@Name("attributeNames") List<String> attributeNames) {
		EditableLayerImpl layer = getEditableLayerOrThrow(tx, spatial(), name);
		layer.setExtraPropertyNames(attributeNames.toArray(new String[0]), tx);
		return streamNode(layer.getLayerNode(tx));
	}

	@Procedure(value = "spatial.removeLayer", mode = WRITE)
	@Description("Removes the given layer")
	public void removeLayer(@Name("name") String name) {
		SpatialDatabaseService sdb = spatial();
		sdb.deleteLayer(tx, name, new ProgressLoggingListener("Deleting layer '" + name + "'", log, Level.INFO));
	}

	@Procedure(value = "spatial.addNode", mode = WRITE)
	@Description("Adds the given node to the layer, returns the geometry-node")
	public Stream<NodeResult> addNodeToLayer(@Name("layerName") String name, @Name("node") Node node) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		Node geomNode = layer.add(tx, node).getGeomNode();
		layer.finalizeTransaction(tx);
		return streamNode(geomNode);
	}

	@Procedure(value = "spatial.addNodes", mode = WRITE)
	@Description("Adds the given nodes list to the layer, returns the count")
	public Stream<CountResult> addNodesToLayer(@Name("layerName") String name, @Name("nodes") List<Node> nodes) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		int count = layer.addAll(tx, nodes);
		layer.finalizeTransaction(tx);
		return Stream.of(new CountResult(count));
	}

	@Procedure(value = "spatial.addNode.byId", mode = WRITE)
	@Description("Adds the given node to the layer, returns the geometry-node")
	public Stream<NodeResult> addNodeIdToLayer(@Name("layerName") String name, @Name("nodeId") String nodeId) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		Node geomNode = layer.add(tx, tx.getNodeByElementId(nodeId)).getGeomNode();
		layer.finalizeTransaction(tx);
		return streamNode(geomNode);
	}

	@Procedure(value = "spatial.addNodes.byId", mode = WRITE)
	@Description("Adds the given nodes list to the layer, returns the count")
	public Stream<CountResult> addNodeIdsToLayer(@Name("layerName") String name,
			@Name("nodeIds") List<String> nodeIds) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		List<Node> nodes = nodeIds.stream().map(id -> tx.getNodeByElementId(id)).collect(Collectors.toList());
		int count = layer.addAll(tx, nodes);
		layer.finalizeTransaction(tx);
		return Stream.of(new CountResult(count));
	}

	@Procedure(value = "spatial.removeNode", mode = WRITE)
	@Description("Removes the given node from the layer, returns the geometry-node")
	public Stream<NodeIdResult> removeNodeFromLayer(@Name("layerName") String name, @Name("node") Node node) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		layer.removeFromIndex(tx, node.getElementId());
		layer.finalizeTransaction(tx);
		return streamNode(node.getElementId());
	}

	@Procedure(value = "spatial.removeNodes", mode = WRITE)
	@Description("Removes the given nodes from the layer, returns the count of nodes removed")
	public Stream<CountResult> removeNodesFromLayer(@Name("layerName") String name, @Name("nodes") List<Node> nodes) {
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

	@Procedure(value = "spatial.removeNode.byId", mode = WRITE)
	@Description("Removes the given node from the layer, returns the geometry-node")
	public Stream<NodeIdResult> removeNodeFromLayer(@Name("layerName") String name, @Name("nodeId") String nodeId) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		layer.removeFromIndex(tx, nodeId);
		layer.finalizeTransaction(tx);
		return streamNode(nodeId);
	}

	@Procedure(value = "spatial.removeNodes.byId", mode = WRITE)
	@Description("Removes the given nodes from the layer, returns the count of nodes removed")
	public Stream<CountResult> removeNodeIdsFromLayer(@Name("layerName") String name,
			@Name("nodeIds") List<String> nodeIds) {
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
	public Stream<NodeResult> addGeometryWKTToLayer(@Name("layerName") String name,
			@Name("geometry") String geometryWKT) {
		EditableLayer layer = getEditableLayerOrThrow(tx, spatial(), name);
		WKTReader reader = new WKTReader(layer.getGeometryFactory());
		Node node = addGeometryWkt(layer, reader, geometryWKT);
		layer.finalizeTransaction(tx);
		return streamNode(node);
	}

	@Procedure(value = "spatial.addWKTs", mode = WRITE)
	@Description("Adds the given WKT string list to the layer, returns the created geometry nodes")
	public Stream<NodeResult> addGeometryWKTsToLayer(@Name("layerName") String name,
			@Name("geometry") List<String> geometryWKTs) {
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
			@Name("layerName") String name,
			@Name("uri") String uri) throws IOException {
		EditableLayerImpl layer = getEditableLayerOrThrow(tx, spatial(), name);
		List<Node> nodes = importShapefileToLayer(uri, layer, 1000);
		layer.finalizeTransaction(tx);
		return Stream.of(new CountResult(nodes.size()));
	}

	@Procedure(value = "spatial.importShapefile", mode = WRITE)
	@Description("Imports the the provided shape-file from URI to a layer of the same name, returns the count of data added")
	public Stream<CountResult> importShapefile(
			@Name("uri") String uri) throws IOException {
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
			@Name("layerName") String layerName,
			@Name("uri") String uri) throws InterruptedException {
		// Delegate finding the layer to the inner thread, so we do not pollute the procedure transaction with anything that might conflict.
		// Since the procedure transaction starts before, and ends after, all inner transactions.
		BiFunction<Transaction, String, OSMLayer> layerFinder = (tx, name) -> (OSMLayer) getEditableLayerOrThrow(tx,
				spatial(), name);
		return Stream.of(new CountResult(importOSMToLayer(uri, layerName, layerFinder)));
	}

	@Procedure(value = "spatial.importOSM", mode = WRITE)
	@Description("Imports the the provided osm-file from URI to a layer of the same name, returns the count of data added")
	public Stream<CountResult> importOSM(
			@Name("uri") String uri) throws InterruptedException {
		String layerName = uri.substring(uri.lastIndexOf(File.separator) + 1);
		assertLayerDoesNotExists(tx, spatial(), layerName);
		// Delegate creating the layer to the inner thread, so we do not pollute the procedure transaction with anything that might conflict.
		// Since the procedure transaction starts before, and ends after, all inner transactions.
		BiFunction<Transaction, String, OSMLayer> layerMaker = (tx, name) -> (OSMLayer) spatial().getOrCreateLayer(tx,
				name, OSMGeometryEncoder.class, OSMLayer.class, "");
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
	@Description("Finds all geometry nodes in the given layer within the lower left and upper right coordinates of a box")
	public Stream<NodeResult> findGeometriesInBBox(
			@Name("layerName") String name,
			@Name("min") Object min,
			@Name("max") Object max) {
		Layer layer = getLayerOrThrow(tx, spatial(), name);
		// TODO why a SearchWithin and not a SearchIntersectWindow?
		Envelope envelope = new Envelope(toCoordinate(min), toCoordinate(max));
		return GeoPipeline
				.startWithinSearch(tx, layer, layer.getGeometryFactory().toGeometry(envelope))
				.stream().map(GeoPipeFlow::getGeomNode).map(NodeResult::new);
	}

	@Procedure(value = "spatial.closest", mode = READ)
	@Description("Finds all geometry nodes in the layer within the distance to the given coordinate")
	public Stream<NodeResult> findClosestGeometries(
			@Name("layerName") String name,
			@Name("coordinate") Object coordinate,
			@Name("distanceInKm") double distanceInKm) {
		Layer layer = getLayerOrThrow(tx, spatial(), name);
		GeometryFactory factory = layer.getGeometryFactory();
		Point point = factory.createPoint(toCoordinate(coordinate));
		List<SpatialTopologyUtils.PointResult> edgeResults = SpatialTopologyUtils.findClosestEdges(tx, point, layer,
				distanceInKm);
		return edgeResults.stream().map(e -> e.getValue().getGeomNode()).map(NodeResult::new);
	}

	@Procedure(value = "spatial.withinDistance", mode = READ)
	@Description("Returns all geometry nodes and their ordered distance in the layer within the distance to the given coordinate")
	public Stream<NodeDistanceResult> findGeometriesWithinDistance(
			@Name("layerName") String name,
			@Name("coordinate") Object coordinate,
			@Name("distanceInKm") double distanceInKm) {

		Layer layer = getLayerOrThrow(tx, spatial(), name);
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
			@Name("layerName") String name,
			@Name("geometry") Object geometry) {

		Layer layer = getLayerOrThrow(tx, spatial(), name);
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
		return (EditableLayerImpl) getLayerOrThrow(tx, spatial, name);
	}

	private static void assertLayerDoesNotExists(Transaction tx, SpatialDatabaseService spatial, String name) {
		if (spatial.getLayer(tx, name) != null) {
			throw new IllegalArgumentException("Layer already exists: '" + name + "'");
		}
	}
}
