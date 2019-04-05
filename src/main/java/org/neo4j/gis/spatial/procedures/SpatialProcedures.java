/**
 * Copyright (c) 2010-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j Spatial.
 * <p>
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.procedures;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.neo4j.gis.spatial.*;
import org.neo4j.gis.spatial.encoders.NativePointEncoder;
import org.neo4j.gis.spatial.encoders.SimpleGraphEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.encoders.SimplePropertyEncoder;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jCRS;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jGeometry;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jPoint;
import org.neo4j.gis.spatial.index.LayerGeohashPointIndex;
import org.neo4j.gis.spatial.index.LayerHilbertPointIndex;
import org.neo4j.gis.spatial.index.LayerZOrderPointIndex;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.pipes.processing.OrthodromicDistance;
import org.neo4j.gis.spatial.rtree.ProgressLoggingListener;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.opengis.referencing.ReferenceIdentifier;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Stream;

import static org.neo4j.gis.spatial.SpatialDatabaseService.RTREE_INDEX_NAME;
import static org.neo4j.gis.spatial.encoders.neo4j.Neo4jCRS.findCRS;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.procedure.Mode.WRITE;

/*
TODO:
* don't pass raw coordinates, take an object which can be a property-container, geometry-point or a map
* optional default simplePointLayer should use the long form of "latitude and longitude" like the spatial functions do
*/

public class SpatialProcedures {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    public static class NodeResult {
        public final Node node;

        public NodeResult(Node node) {
            this.node = node;
        }
    }

    public static class CountResult {
        public final long count;

        public CountResult(long count) {
            this.count = count;
        }
    }

    public static class NameResult {
        public final String name;
        public final String signature;

        public NameResult(String name, String signature) {
            this.name = name;
            this.signature = signature;
        }
    }

    public static class StringResult {
        public final String name;

        public StringResult(String name) {
            this.name = name;
        }
    }

    public static class NodeDistanceResult {
        public final Node node;
        public final double distance;

        public NodeDistanceResult(Node node, double distance) {
            this.node = node;
            this.distance = distance;
        }
    }

    public static class GeometryResult {
        public final Object geometry;

        public GeometryResult(org.neo4j.graphdb.spatial.Geometry geometry) {
            // Unfortunately Neo4j 3.4 only copes with Points, other types need to be converted to a public type
            if(geometry instanceof org.neo4j.graphdb.spatial.Point) {
                this.geometry = geometry;
            }else{
                this.geometry = toMap(geometry);
            }
        }
    }

    private static Map<String, Class> encoderClasses = new HashMap<>();

    static {
        populateEncoderClasses();
    }

    private static void populateEncoderClasses() {
        encoderClasses.clear();
        // TODO: Make this auto-find classes that implement GeometryEncoder
        for (Class cls : new Class[]{
                SimplePointEncoder.class, OSMGeometryEncoder.class, SimplePropertyEncoder.class,
                WKTGeometryEncoder.class, WKBGeometryEncoder.class, SimpleGraphEncoder.class,
                NativePointEncoder.class
        }) {
            if (GeometryEncoder.class.isAssignableFrom(cls)) {
                String name = cls.getSimpleName();
                encoderClasses.put(name, cls);
            }
        }
    }

    @Procedure("spatial.procedures")
    @Description("Lists all spatial procedures with name and signature")
    public Stream<NameResult> listProcedures() {
        Procedures procedures = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class);
        Stream.Builder<NameResult> builder = Stream.builder();
        for (ProcedureSignature proc : procedures.getAllProcedures()) {
            if (proc.name().namespace()[0].equals("spatial")) {
                builder.accept(new NameResult(proc.name().toString(), proc.toString()));
            }
        }
        return builder.build();
    }

    @Procedure(value="spatial.layers", mode = WRITE)
    @Description("Returns name, and details for all layers")
    public Stream<NameResult> getAllLayers() {
        Stream.Builder<NameResult> builder = Stream.builder();
        SpatialDatabaseService spatial = wrap(db);
        for (String name : spatial.getLayerNames()) {
            Layer layer = spatial.getLayer(name);
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
        for (Map.Entry<String, String> entry : wrap(db).getRegisteredLayerTypes().entrySet()) {
            builder.accept(new NameResult(entry.getKey(), entry.getValue()));
        }
        return builder.build();
    }

    @Procedure(value="spatial.addPointLayer", mode=WRITE)
    @Description("Adds a new simple point layer, returns the layer root node")
    public Stream<NodeResult> addSimplePointLayer(
            @Name("name") String name,
            @Name(value = "indexType", defaultValue = RTREE_INDEX_NAME) String indexType,
            @Name(value = "crsName", defaultValue = UNSET_CRS_NAME) String crsName) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            return streamNode(sdb.createLayer(name, SimplePointEncoder.class, SimplePointLayer.class,
                    sdb.resolveIndexClass(indexType), null,
                    selectCRS(crsName)).getLayerNode());
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure(value="spatial.addPointLayerGeohash", mode=WRITE)
    @Description("Adds a new simple point layer with geohash based index, returns the layer root node")
    public Stream<NodeResult> addSimplePointLayerGeohash(
            @Name("name") String name,
            @Name(value = "crsName", defaultValue = WGS84_CRS_NAME) String crsName) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            return streamNode(sdb.createLayer(name, SimplePointEncoder.class, SimplePointLayer.class,
                    LayerGeohashPointIndex.class, null,
                    selectCRS(crsName)).getLayerNode());
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure(value="spatial.addPointLayerZOrder", mode=WRITE)
    @Description("Adds a new simple point layer with z-order curve based index, returns the layer root node")
    public Stream<NodeResult> addSimplePointLayerZOrder(@Name("name") String name) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            return streamNode(sdb.createLayer(name, SimplePointEncoder.class, SimplePointLayer.class, LayerZOrderPointIndex.class, null, DefaultGeographicCRS.WGS84).getLayerNode());
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure(value="spatial.addPointLayerHilbert", mode=WRITE)
    @Description("Adds a new simple point layer with hilbert curve based index, returns the layer root node")
    public Stream<NodeResult> addSimplePointLayerHilbert(@Name("name") String name) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            return streamNode(sdb.createLayer(name, SimplePointEncoder.class, SimplePointLayer.class, LayerHilbertPointIndex.class, null, DefaultGeographicCRS.WGS84).getLayerNode());
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure(value="spatial.addPointLayerXY", mode=WRITE)
    @Description("Adds a new simple point layer with the given properties for x and y coordinates, returns the layer root node")
    public Stream<NodeResult> addSimplePointLayer(
            @Name("name") String name,
            @Name("xProperty") String xProperty,
            @Name("yProperty") String yProperty,
            @Name(value = "indexType", defaultValue = RTREE_INDEX_NAME) String indexType,
            @Name(value = "crsName", defaultValue = UNSET_CRS_NAME) String crsName) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            if (xProperty != null && yProperty != null) {
                return streamNode(sdb.createLayer(name, SimplePointEncoder.class, SimplePointLayer.class,
                        sdb.resolveIndexClass(indexType), sdb.makeEncoderConfig(xProperty, yProperty),
                        selectCRS(hintCRSName(crsName, yProperty))).getLayerNode());
            } else {
                throw new IllegalArgumentException("Cannot create layer '" + name + "': Missing encoder config values: xProperty[" + xProperty + "], yProperty[" + yProperty + "]");
            }
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure(value="spatial.addPointLayerWithConfig", mode=WRITE)
    @Description("Adds a new simple point layer with the given configuration, returns the layer root node")
    public Stream<NodeResult> addSimplePointLayerWithConfig(
            @Name("name") String name,
            @Name("encoderConfig") String encoderConfig,
            @Name(value = "indexType", defaultValue = RTREE_INDEX_NAME) String indexType,
            @Name(value = "crsName", defaultValue = UNSET_CRS_NAME) String crsName) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            if (encoderConfig.indexOf(':') > 0) {
                return streamNode(sdb.createLayer(name, SimplePointEncoder.class, SimplePointLayer.class,
                        sdb.resolveIndexClass(indexType), encoderConfig,
                        selectCRS(hintCRSName(crsName, encoderConfig))).getLayerNode());
            } else {
                throw new IllegalArgumentException("Cannot create layer '" + name + "': invalid encoder config '" + encoderConfig + "'");
            }
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure(value="spatial.addNativePointLayer", mode=WRITE)
    @Description("Adds a new native point layer, returns the layer root node")
    public Stream<NodeResult> addNativePointLayer(
            @Name("name") String name,
            @Name(value = "indexType", defaultValue = RTREE_INDEX_NAME) String indexType,
            @Name(value = "crsName", defaultValue = UNSET_CRS_NAME) String crsName) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            return streamNode(sdb.createLayer(name, NativePointEncoder.class, SimplePointLayer.class, sdb.resolveIndexClass(indexType), null, selectCRS(crsName)).getLayerNode());
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure(value="spatial.addNativePointLayerGeohash", mode=WRITE)
    @Description("Adds a new native point layer with geohash based index, returns the layer root node")
    public Stream<NodeResult> addNativePointLayerGeohash(
            @Name("name") String name,
            @Name(value = "crsName", defaultValue = WGS84_CRS_NAME) String crsName) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            return streamNode(sdb.createLayer(name, NativePointEncoder.class, SimplePointLayer.class, LayerGeohashPointIndex.class, null, selectCRS(crsName)).getLayerNode());
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure(value="spatial.addNativePointLayerZOrder", mode=WRITE)
    @Description("Adds a new native point layer with z-order curve based index, returns the layer root node")
    public Stream<NodeResult> addNativePointLayerZOrder(@Name("name") String name) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            return streamNode(sdb.createLayer(name, NativePointEncoder.class, SimplePointLayer.class, LayerZOrderPointIndex.class, null, DefaultGeographicCRS.WGS84).getLayerNode());
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure(value="spatial.addNativePointLayerHilbert", mode=WRITE)
    @Description("Adds a new native point layer with hilbert curve based index, returns the layer root node")
    public Stream<NodeResult> addNativePointLayerHilbert(@Name("name") String name) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            return streamNode(sdb.createLayer(name, NativePointEncoder.class, SimplePointLayer.class, LayerHilbertPointIndex.class, null, DefaultGeographicCRS.WGS84).getLayerNode());
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure(value="spatial.addNativePointLayerXY", mode=WRITE)
    @Description("Adds a new native point layer with the given properties for x and y coordinates, returns the layer root node")
    public Stream<NodeResult> addNativePointLayer(
            @Name("name") String name,
            @Name("xProperty") String xProperty,
            @Name("yProperty") String yProperty,
            @Name(value = "indexType", defaultValue = RTREE_INDEX_NAME) String indexType,
            @Name(value = "crsName", defaultValue = UNSET_CRS_NAME) String crsName) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            if (xProperty != null && yProperty != null) {
                return streamNode(sdb.createLayer(name, NativePointEncoder.class, SimplePointLayer.class,
                        sdb.resolveIndexClass(indexType), sdb.makeEncoderConfig(xProperty, yProperty),
                        selectCRS(hintCRSName(crsName, yProperty))).getLayerNode());
            } else {
                throw new IllegalArgumentException("Cannot create layer '" + name + "': Missing encoder config values: xProperty[" + xProperty + "], yProperty[" + yProperty + "]");
            }
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure(value="spatial.addNativePointLayerWithConfig", mode=WRITE)
    @Description("Adds a new native point layer with the given configuration, returns the layer root node")
    public Stream<NodeResult> addNativePointLayerWithConfig(
            @Name("name") String name,
            @Name("encoderConfig") String encoderConfig,
            @Name(value = "indexType", defaultValue = RTREE_INDEX_NAME) String indexType,
            @Name(value = "crsName", defaultValue = UNSET_CRS_NAME) String crsName) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            if (encoderConfig.indexOf(':') > 0) {
                return streamNode(sdb.createLayer(name, NativePointEncoder.class, SimplePointLayer.class,
                        sdb.resolveIndexClass(indexType), encoderConfig,
                        selectCRS(hintCRSName(crsName, encoderConfig))).getLayerNode());
            } else {
                throw new IllegalArgumentException("Cannot create layer '" + name + "': invalid encoder config '" + encoderConfig + "'");
            }
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    public static final String UNSET_CRS_NAME = "";
    public static final String WGS84_CRS_NAME = "wgs84";

    /**
     * Currently this only supports the string 'WGS84', for the convenience of procedure users.
     * This should be expanded with CRS table lookup.
     * @param name
     * @return null or WGS84
     */
    public CoordinateReferenceSystem selectCRS(String name) {
        if (name == null) {
            return null;
        } else {
            switch (name.toLowerCase()) {
                case WGS84_CRS_NAME:
                    return org.geotools.referencing.crs.DefaultGeographicCRS.WGS84;
                case UNSET_CRS_NAME:
                    return null;
                default:
                    throw new IllegalArgumentException("Unsupported CRS name: " + name);
            }
        }
    }

    private String hintCRSName(String crsName, String hint) {
        if (crsName.equals(UNSET_CRS_NAME) && hint.toLowerCase().contains("lat")) {
            crsName = WGS84_CRS_NAME;
        }
        return crsName;
    }

    @Procedure(value="spatial.addLayerWithEncoder", mode=WRITE)
    @Description("Adds a new layer with the given encoder class and configuration, returns the layer root node")
    public Stream<NodeResult> addLayerWithEncoder(
            @Name("name") String name,
            @Name("encoder") String encoderClassName,
            @Name("encoderConfig") String encoderConfig) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            Class encoderClass = encoderClasses.get(encoderClassName);
            Class layerClass = sdb.suggestLayerClassForEncoder(encoderClass);
            if (encoderClass != null) {
                return streamNode(sdb.createLayer(name, encoderClass, layerClass, null, encoderConfig).getLayerNode());
            } else {
                throw new IllegalArgumentException("Cannot create layer '" + name + "': invalid encoder class '" + encoderClassName + "'");
            }
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure(value="spatial.addLayer", mode=WRITE)
    @Description("Adds a new layer with the given type (see spatial.getAllLayerTypes) and configuration, returns the layer root node")
    public Stream<NodeResult> addLayerOfType(
            @Name("name") String name,
            @Name("type") String type,
            @Name("encoderConfig") String encoderConfig) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            Map<String, String> knownTypes = sdb.getRegisteredLayerTypes();
            if (knownTypes.containsKey(type.toLowerCase())) {
                return streamNode(sdb.getOrCreateRegisteredTypeLayer(name, type, encoderConfig).getLayerNode());
            } else {
                throw new IllegalArgumentException("Cannot create layer '" + name + "': unknown type '" + type + "' - supported types are " + knownTypes.toString());
            }
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    private Stream<NodeResult> streamNode(Node node) {
        return Stream.of(new NodeResult(node));
    }

    @Procedure(value="spatial.addWKTLayer", mode=WRITE)
    @Description("Adds a new WKT layer with the given node property to hold the WKT string, returns the layer root node")
    public Stream<NodeResult> addWKTLayer(@Name("name") String name,
                                          @Name("nodePropertyName") String nodePropertyName) {
        return addLayerOfType(name, "WKT", nodePropertyName);
    }

    @Procedure(value="spatial.layer", mode=WRITE)
    @Description("Returns the layer root node for the given layer name")
    public Stream<NodeResult> getLayer(@Name("name") String name) {
        return streamNode(getLayerOrThrow(name).getLayerNode());
    }

    @Procedure(value="spatial.getFeatureAttributes", mode=WRITE)
    @Description("Returns feature attributes of the given layer")
    public Stream<StringResult> getFeatureAttributes(@Name("name") String name) {
        Layer layer = this.getLayerOrThrow(name);
        return Arrays.asList(layer.getExtraPropertyNames()).stream().map(StringResult::new);
    }

    @Procedure(value="spatial.setFeatureAttributes", mode=WRITE)
    @Description("Sets the feature attributes of the given layer")
    public Stream<NodeResult> setFeatureAttributes(@Name("name") String name,
                                                   @Name("attributeNames") List<String> attributeNames) {
        EditableLayerImpl layer = this.getEditableLayerOrThrow(name);
        layer.setExtraPropertyNames(attributeNames.toArray(new String[attributeNames.size()]));
        return streamNode(layer.getLayerNode());
    }

    @Procedure(value="spatial.removeLayer", mode=WRITE)
    @Description("Removes the given layer")
    public void removeLayer(@Name("name") String name) {
        wrap(db).deleteLayer(name, new ProgressLoggingListener("Deleting layer '" + name + "'", log.infoLogger()));
    }

    @Procedure(value="spatial.addNode", mode=WRITE)
    @Description("Adds the given node to the layer, returns the geometry-node")
    public Stream<NodeResult> addNodeToLayer(@Name("layerName") String name, @Name("node") Node node) {
        EditableLayer layer = getEditableLayerOrThrow(name);
        return streamNode(layer.add(node).getGeomNode());
    }

    @Procedure(value="spatial.addNodes", mode=WRITE)
    @Description("Adds the given nodes list to the layer, returns the count")
    public Stream<CountResult> addNodesToLayer(@Name("layerName") String name, @Name("nodes") List<Node> nodes) {
        EditableLayer layer = getEditableLayerOrThrow(name);
        return Stream.of(new CountResult(layer.addAll(nodes)));
    }

    @Procedure(value="spatial.removeNode", mode=WRITE)
    @Description("Removes the given node from the layer, returns the geometry-node")
    public Stream<NodeResult> removeNodeFromLayer(@Name("layerName") String name, @Name("node") Node node) {
        EditableLayer layer = getEditableLayerOrThrow(name);
        layer.removeFromIndex(node.getId());
        return streamNode(node);
    }

    @Procedure(value="spatial.removeNodes", mode=WRITE)
    @Description("Removes the given nodes from the layer, returns the count of nodes removed")
    public Stream<CountResult> removeNodesFromLayer(@Name("layerName") String name, @Name("nodes") List<Node> nodes) {
        EditableLayer layer = getEditableLayerOrThrow(name);
        //TODO optimize bulk node removal from RTree like we have done for node additions
        int before = layer.getIndex().count();
        for (Node node : nodes) {
            layer.removeFromIndex(node.getId());
        }
        int after = layer.getIndex().count();
        return Stream.of(new CountResult(before - after));
    }

    @Procedure(value="spatial.addWKT", mode=WRITE)
    @Description("Adds the given WKT string to the layer, returns the created geometry node")
    public Stream<NodeResult> addGeometryWKTToLayer(@Name("layerName") String name, @Name("geometry") String geometryWKT) throws ParseException {
        EditableLayer layer = getEditableLayerOrThrow(name);
        WKTReader reader = new WKTReader(layer.getGeometryFactory());
        return streamNode(addGeometryWkt(layer, reader, geometryWKT));
    }

    @Procedure(value="spatial.addWKTs", mode=WRITE)
    @Description("Adds the given WKT string list to the layer, returns the created geometry nodes")
    public Stream<NodeResult> addGeometryWKTsToLayer(@Name("layerName") String name, @Name("geometry") List<String> geometryWKTs) throws ParseException {
        EditableLayer layer = getEditableLayerOrThrow(name);
        WKTReader reader = new WKTReader(layer.getGeometryFactory());
        return geometryWKTs.stream().map(geometryWKT -> addGeometryWkt(layer, reader, geometryWKT)).map(NodeResult::new);
    }

    private Node addGeometryWkt(EditableLayer layer, WKTReader reader, String geometryWKT) {
        try {
            Geometry geometry = reader.read(geometryWKT);
            return layer.add(geometry).getGeomNode();
        } catch (ParseException e) {
            throw new RuntimeException("Error parsing geometry: " + geometryWKT, e);
        }
    }

    @Procedure(value="spatial.importShapefileToLayer", mode=WRITE)
    @Description("Imports the the provided shape-file from URI to the given layer, returns the count of data added")
    public Stream<CountResult> importShapefile(
            @Name("layerName") String name,
            @Name("uri") String uri) throws IOException {
        EditableLayerImpl layer = getEditableLayerOrThrow(name);
        return Stream.of(new CountResult(importShapefileToLayer(uri, layer, 1000).size()));
    }

    @Procedure(value="spatial.importShapefile", mode=WRITE)
    @Description("Imports the the provided shape-file from URI to a layer of the same name, returns the count of data added")
    public Stream<CountResult> importShapefile(
            @Name("uri") String uri) throws IOException {
        return Stream.of(new CountResult(importShapefileToLayer(uri, null, 1000).size()));
    }

    private List<Node> importShapefileToLayer(String shpPath, EditableLayerImpl layer, int commitInterval) throws IOException {
        if (shpPath.toLowerCase().endsWith(".shp")) {
            // remove extension
            shpPath = shpPath.substring(0, shpPath.lastIndexOf("."));
        }

        ShapefileImporter importer = new ShapefileImporter(db, new ProgressLoggingListener("Importing " + shpPath, log.debugLogger()), commitInterval);
        if (layer == null) {
            String layerName = shpPath.substring(shpPath.lastIndexOf(File.separator) + 1);
            return importer.importFile(shpPath, layerName);
        } else {
            return importer.importFile(shpPath, layer, Charset.defaultCharset());
        }
    }

    @Procedure(value="spatial.importOSMToLayer", mode=WRITE)
    @Description("Imports the the provided osm-file from URI to a layer, returns the count of data added")
    public Stream<CountResult> importOSM(
            @Name("layerName") String name,
            @Name("uri") String uri) throws IOException, XMLStreamException {
        EditableLayerImpl layer = getEditableLayerOrThrow(name);
        return Stream.of(new CountResult(importOSMToLayer(uri, layer, 1000)));
    }

    @Procedure(value="spatial.importOSM", mode=WRITE)
    @Description("Imports the the provided osm-file from URI to a layer of the same name, returns the count of data added")
    public Stream<CountResult> importOSM(
            @Name("uri") String uri) throws IOException, XMLStreamException {
        return Stream.of(new CountResult(importOSMToLayer(uri, null, 1000)));
    }

    private long importOSMToLayer(String osmPath, EditableLayerImpl layer, int commitInterval) throws IOException, XMLStreamException {
        if (!osmPath.toLowerCase().endsWith(".osm")) {
            // add extension
            osmPath = osmPath + ".osm";
        }

        String layerName = (layer == null) ? osmPath.substring(osmPath.lastIndexOf(File.separator) + 1) : layer.getName();
        OSMImporter importer = new OSMImporter(layerName, new ProgressLoggingListener("Importing " + osmPath, log.debugLogger()));
        importer.importFile( db, osmPath, false, commitInterval, true );
        return importer.reIndex( db, commitInterval, false );
    }

    @Procedure(value="spatial.bbox", mode=WRITE)
    @Description("Finds all geometry nodes in the given layer within the lower left and upper right coordinates of a box")
    public Stream<NodeResult> findGeometriesInBBox(
            @Name("layerName") String name,
            @Name("min") Object min,
            @Name("max") Object max) {
        Layer layer = getLayerOrThrow(name);
        // TODO why a SearchWithin and not a SearchIntersectWindow?
        Envelope envelope = new Envelope(toCoordinate(min), toCoordinate(max));
        return GeoPipeline
                .startWithinSearch(layer, layer.getGeometryFactory().toGeometry(envelope))
                .stream().map(GeoPipeFlow::getGeomNode).map(NodeResult::new);
    }

    @Procedure(value="spatial.closest", mode=WRITE)
    @Description("Finds all geometry nodes in the layer within the distance to the given coordinate")
    public Stream<NodeResult> findClosestGeometries(
            @Name("layerName") String name,
            @Name("coordinate") Object coordinate,
            @Name("distanceInKm") double distanceInKm) {
        Layer layer = getLayerOrThrow(name);
        GeometryFactory factory = layer.getGeometryFactory();
        Point point = factory.createPoint(toCoordinate(coordinate));
        List<SpatialTopologyUtils.PointResult> edgeResults = SpatialTopologyUtils.findClosestEdges(point, layer, distanceInKm);
        return edgeResults.stream().map(e -> e.getValue().getGeomNode()).map(NodeResult::new);
    }

    @Procedure(value="spatial.withinDistance", mode=WRITE)
    @Description("Returns all geometry nodes and their ordered distance in the layer within the distance to the given coordinate")
    public Stream<NodeDistanceResult> findGeometriesWithinDistance(
            @Name("layerName") String name,
            @Name("coordinate") Object coordinate,
            @Name("distanceInKm") double distanceInKm) {

        Layer layer = getLayerOrThrow(name);
        return GeoPipeline
                .startNearestNeighborLatLonSearch(layer, toCoordinate(coordinate), distanceInKm)
                .sort(OrthodromicDistance.DISTANCE)
                .stream().map(r -> {
                    double distance = r.hasProperty(OrthodromicDistance.DISTANCE) ? ((Number) r.getProperty(OrthodromicDistance.DISTANCE)).doubleValue() : -1;
                    return new NodeDistanceResult(r.getGeomNode(), distance);
                });
    }

    @UserFunction("spatial.decodeGeometry")
    @Description("Returns a geometry of a layer node as the Neo4j geometry type, to be passed to other procedures or returned to a client")
    public Object decodeGeometry(
            @Name("layerName") String name,
            @Name("node") Node node) {

        Layer layer = getLayerOrThrow(name);
        GeometryResult result = new GeometryResult(toNeo4jGeometry(layer, layer.getGeometryEncoder().decodeGeometry(node)));
        return result.geometry;
    }

    @UserFunction("spatial.asMap")
    @Description("Returns a Map object representing the Geometry, to be passed to other procedures or returned to a client")
    public Object asMap(@Name("object") Object geometry) {
        return toGeometryMap(geometry);
    }

    @UserFunction("spatial.asGeometry")
    @Description("Returns a geometry object as the Neo4j geometry type, to be passed to other functions or procedures or returned to a client")
    public Object asGeometry(
            @Name("geometry") Object geometry) {

        return toNeo4jGeometry(null, geometry);
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

    @Procedure(value="spatial.intersects", mode=WRITE)
    @Description("Returns all geometry nodes that intersect the given geometry (shape, polygon) in the layer")
    public Stream<NodeResult> findGeometriesIntersecting(
            @Name("layerName") String name,
            @Name("geometry") Object geometry) {

        Layer layer = getLayerOrThrow(name);
        return GeoPipeline
                .startIntersectSearch(layer, toJTSGeometry(layer, geometry))
                .stream().map(GeoPipeFlow::getGeomNode).map(NodeResult::new);
    }

    private Geometry toJTSGeometry(Layer layer, Object value) {
        GeometryFactory factory = layer.getGeometryFactory();
        if (value instanceof org.neo4j.graphdb.spatial.Point) {
            org.neo4j.graphdb.spatial.Point point = (org.neo4j.graphdb.spatial.Point) value;
            List<Double> coord = point.getCoordinate().getCoordinate();
            return factory.createPoint(new Coordinate(coord.get(0), coord.get(1)));
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
        if (value instanceof PropertyContainer) {
            latLon = ((PropertyContainer) value).getProperties("latitude", "longitude", "lat", "lon");
        }
        if (value instanceof Map) latLon = (Map<String, Object>) value;
        Coordinate coord = toCoordinate(latLon);
        if (coord != null) return factory.createPoint(coord);
        throw new RuntimeException("Can't convert " + value + " to a geometry");
    }

    private static org.neo4j.graphdb.spatial.Coordinate toNeo4jCoordinate(Coordinate coordinate) {
        if (coordinate.z == Coordinate.NULL_ORDINATE) {
            return new org.neo4j.graphdb.spatial.Coordinate(coordinate.x, coordinate.y);
        } else {
            return new org.neo4j.graphdb.spatial.Coordinate(coordinate.x, coordinate.y, coordinate.z);
        }
    }

    private static List<org.neo4j.graphdb.spatial.Coordinate> toNeo4jCoordinates(Coordinate[] coordinates) {
        ArrayList<org.neo4j.graphdb.spatial.Coordinate> converted = new ArrayList<>();
        for (Coordinate coordinate : coordinates) {
            converted.add(toNeo4jCoordinate(coordinate));
        }
        return converted;
    }

    private static org.neo4j.graphdb.spatial.Geometry toNeo4jGeometry(Layer layer, Object value) {
        if (value instanceof org.neo4j.graphdb.spatial.Geometry) {
            return (org.neo4j.graphdb.spatial.Geometry) value;
        }
        Neo4jCRS crs = findCRS("Cartesian");
        if (layer != null) {
            CoordinateReferenceSystem layerCRS = layer.getCoordinateReferenceSystem();
            if (layerCRS != null) {
                ReferenceIdentifier crsRef = layer.getCoordinateReferenceSystem().getName();
                crs = findCRS(crsRef.toString());
            }
        }
        if (value instanceof Point) {
            Point point = (Point) value;
            return new Neo4jPoint(point, crs);
        }
        if (value instanceof Geometry) {
            Geometry geometry = (Geometry) value;
            return new Neo4jGeometry(geometry.getGeometryType(), toNeo4jCoordinates(geometry.getCoordinates()), crs);
        }
        if (value instanceof String && layer != null) {
            GeometryFactory factory = layer.getGeometryFactory();
            WKTReader reader = new WKTReader(factory);
            try {
                Geometry geometry = reader.read((String) value);
                return new Neo4jGeometry(geometry.getGeometryType(), toNeo4jCoordinates(geometry.getCoordinates()), crs);
            } catch (ParseException e) {
                throw new IllegalArgumentException("Invalid WKT: " + e.getMessage());
            }
        }
        Map<String, Object> latLon = null;
        if (value instanceof PropertyContainer) {
            latLon = ((PropertyContainer) value).getProperties("latitude", "longitude", "lat", "lon");
        }
        if (value instanceof Map) latLon = (Map<String, Object>) value;
        Coordinate coord = toCoordinate(latLon);
        if (coord != null) return new Neo4jPoint(coord, crs);
        throw new RuntimeException("Can't convert " + value + " to a geometry");
    }

    private static Object toPublic(Object obj) {
        if (obj instanceof Map) {
            return toPublic((Map) obj);
        } else if (obj instanceof PropertyContainer) {
            return toPublic(((PropertyContainer) obj).getProperties());
        } else if (obj instanceof Geometry) {
            return toMap((Geometry) obj);
        } else {
            return obj;
        }
    }

    private static Map<String, Object> toGeometryMap(Object geometry) {
        return toMap(toNeo4jGeometry(null, geometry));
    }

    private static Map<String, Object> toMap(Geometry geometry) {
        return toMap(toNeo4jGeometry(null, geometry));
    }

    private static double[] toCoordinateArrayFromDoubles(List<Double> coords) {
        double[] coordinates = new double[coords.size()];
        for (int i = 0; i < coordinates.length; i++) {
            coordinates[i] = coords.get(i);
        }
        return coordinates;
    }

    private static double[][] toCoordinateArrayFromCoordinates(List<org.neo4j.graphdb.spatial.Coordinate> coords) {
        List<double[]> coordinates = new ArrayList<>(coords.size());
        for (org.neo4j.graphdb.spatial.Coordinate coord : coords) {
            coordinates.add(toCoordinateArrayFromDoubles(coord.getCoordinate()));
        }
        return toCoordinateArray(coordinates);
    }

    private static double[][] toCoordinateArray(List<double[]> coords) {
        double[][] coordinates = new double[coords.size()][];
        for (int i = 0; i < coordinates.length; i++) {
            coordinates[i] = coords.get(i);
        }
        return coordinates;
    }

    private static Map<String, Object> toMap(org.neo4j.graphdb.spatial.Geometry geometry) {
        if (geometry instanceof org.neo4j.graphdb.spatial.Point) {
            org.neo4j.graphdb.spatial.Point point = (org.neo4j.graphdb.spatial.Point) geometry;
            return map("type", geometry.getGeometryType(), "coordinate", toCoordinateArrayFromDoubles(point.getCoordinate().getCoordinate()));
        } else {
            return map("type", geometry.getGeometryType(), "coordinates", toCoordinateArrayFromCoordinates(geometry.getCoordinates()));
        }
    }

    private static Map<String, Object> toPublic(Map incoming) {
        Map<String, Object> map = new HashMap<>(incoming.size());
        for (Object key : incoming.keySet()) {
            map.put(key.toString(), toPublic(incoming.get(key)));
        }
        return map;
    }

    private Coordinate toCoordinate(Object value) {
        if (value instanceof Coordinate) {
            return (Coordinate) value;
        }
        if (value instanceof org.neo4j.graphdb.spatial.Coordinate) {
            return toCoordinate((org.neo4j.graphdb.spatial.Coordinate) value);
        }
        if (value instanceof org.neo4j.graphdb.spatial.Point) {
            return toCoordinate(((org.neo4j.graphdb.spatial.Point) value).getCoordinate());
        }
        if (value instanceof PropertyContainer) {
            return toCoordinate(((PropertyContainer) value).getProperties("latitude", "longitude", "lat", "lon"));
        }
        if (value instanceof Map) {
            return toCoordinate((Map) value);
        }
        throw new RuntimeException("Can't convert " + value + " to a coordinate");
    }

    private static Coordinate toCoordinate(org.neo4j.graphdb.spatial.Coordinate point) {
        List<Double> coordinate = point.getCoordinate();
        return new Coordinate(coordinate.get(0), coordinate.get(1));
    }

    private static Coordinate toCoordinate(Map map) {
        if (map == null) return null;
        Coordinate coord = toCoordinate(map, "longitude", "latitude");
        if (coord == null) return toCoordinate(map, "lon", "lat");
        return coord;
    }

    private static Coordinate toCoordinate(Map map, String xName, String yName) {
        if (map.containsKey(xName) && map.containsKey(yName))
            return new Coordinate(((Number) map.get(xName)).doubleValue(), ((Number) map.get(yName)).doubleValue());
        return null;
    }

    private EditableLayerImpl getEditableLayerOrThrow(String name) {
        return (EditableLayerImpl) getLayerOrThrow(wrap(db), name);
    }

    private Layer getLayerOrThrow(String name) {
        return getLayerOrThrow(wrap(db), name);
    }

    private Layer getLayerOrThrow(SpatialDatabaseService spatialService, String name) {
        EditableLayer layer = (EditableLayer) spatialService.getLayer(name);
        if (layer != null) {
            return layer;
        } else {
            throw new IllegalArgumentException("No such layer '" + name + "'");
        }
    }

    private SpatialDatabaseService wrap(GraphDatabaseService db) {
        return new SpatialDatabaseService(db);
    }
}
