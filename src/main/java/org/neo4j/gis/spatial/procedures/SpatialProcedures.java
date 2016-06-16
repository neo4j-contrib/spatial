/**
 * Copyright (c) 2010-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j.
 * <p>
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.procedures;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.GeographicPoint;
import org.neo4j.gis.spatial.*;
import org.neo4j.gis.spatial.encoders.SimpleGraphEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.encoders.SimplePropertyEncoder;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.rtree.ProgressLoggingListener;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/*
TODO:
* don't pass raw coordinates, take an object which can be a property-container, geometry-point or a map
* optional default simplePointLayer should use the long form of "latitude and longitude" like the spatial functions do
*/

public class SpatialProcedures {

    public static final String DISTANCE = "OrthodromicDistance";
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

    public static class NameResult {
        public final String name;
        public final String signature;

        public NameResult(String name, String signature) {
            this.name = name;
            this.signature = signature;
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

    private static Map<String, Class> encoderClasses = new HashMap<>();

    static {
        populateEncoderClasses();
    }

    private static void populateEncoderClasses() {
        encoderClasses.clear();
        // TODO: Make this auto-find classes that implement GeometryEncoder
        for (Class cls : new Class[]{
                SimplePointEncoder.class, OSMGeometryEncoder.class, SimplePropertyEncoder.class,
                WKTGeometryEncoder.class, WKBGeometryEncoder.class, SimpleGraphEncoder.class
        }) {
            if (GeometryEncoder.class.isAssignableFrom(cls)) {
                String name = cls.getSimpleName();
                encoderClasses.put(name, cls);
            }
        }
    }

    @Procedure("spatial.procedures")
    public Stream<NameResult> listProcedures() {
        Procedures procedures = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( Procedures.class );
        Stream.Builder<NameResult> builder = Stream.builder();
        for (ProcedureSignature proc : procedures.getAll()) {
            if (proc.name().namespace()[0].equals("spatial")) {
                builder.accept(new NameResult(proc.name().toString(), proc.toString()));
            }
        }
        return builder.build();
    }

    @Procedure("spatial.layers")
    @PerformsWrites // TODO FIX - due to lazy evaluation of index count, updated during later reads, not during writes
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
    public Stream<NameResult> getAllLayerTypes() {
        Stream.Builder<NameResult> builder = Stream.builder();
        for (Map.Entry<String, String> entry : wrap(db).getRegisteredLayerTypes().entrySet()) {
            builder.accept(new NameResult(entry.getKey(), entry.getValue()));
        }
        return builder.build();
    }

    @Procedure("spatial.addPointLayer")
    @PerformsWrites
    public Stream<NodeResult> addSimplePointLayer(@Name("name") String name) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            return streamNode(sdb.createLayer(name, SimplePointEncoder.class, SimplePointLayer.class).getLayerNode());
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure("spatial.addPointLayerXY")
    @PerformsWrites
    public Stream<NodeResult> addSimplePointLayer(
            @Name("name") String name,
            @Name("xProperty") String xProperty,
            @Name("yProperty") String yProperty) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            if (xProperty != null && yProperty != null) {
                String encoderConfig = xProperty + ":" + yProperty;
                return streamNode(sdb.createLayer(name, SimplePointEncoder.class, SimplePointLayer.class, encoderConfig).getLayerNode());
            } else {
                throw new IllegalArgumentException("Cannot create layer '" + name + "': Missing encoder config values: xProperty[" + xProperty + "], yProperty[" + yProperty + "]");
            }
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure("spatial.addPointLayerWithConfig")
    @PerformsWrites
    public Stream<NodeResult> addSimplePointLayer(
            @Name("name") String name,
            @Name("encoderConfig") String encoderConfig) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            if (encoderConfig.indexOf(':') > 0) {
                return streamNode(sdb.createLayer(name, SimplePointEncoder.class, SimplePointLayer.class, encoderConfig).getLayerNode());
            } else {
                throw new IllegalArgumentException("Cannot create layer '" + name + "': invalid encoder config '" + encoderConfig + "'");
            }
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure("spatial.addLayerWithEncoder")
    @PerformsWrites
    public Stream<NodeResult> addLayer(
            @Name("name") String name,
            @Name("encoder") String encoderClassName,
            @Name("encoderConfig") String encoderConfig) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            Class encoderClass = encoderClasses.get(encoderClassName);
            Class layerClass = sdb.suggestLayerClassForEncoder(encoderClass);
            if (encoderClass != null) {
                return streamNode(sdb.createLayer(name, encoderClass, layerClass, encoderConfig).getLayerNode());
            } else {
                throw new IllegalArgumentException("Cannot create layer '" + name + "': invalid encoder class '" + encoderClassName + "'");
            }
        } else {
            throw new IllegalArgumentException("Cannot create existing layer: " + name);
        }
    }

    @Procedure("spatial.addLayer")
    @PerformsWrites
    public Stream<NodeResult> addLayerOfType(
            @Name("name") String name,
            @Name("type") String type,
            @Name("encoderConfig") String encoderConfig) {
        SpatialDatabaseService sdb = wrap(db);
        Layer layer = sdb.getLayer(name);
        if (layer == null) {
            Map<String,String> knownTypes = sdb.getRegisteredLayerTypes();
            if(knownTypes.containsKey(type)) {
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

    @Procedure("spatial.addWKTLayer")
    @PerformsWrites
    public Stream<NodeResult> addWKTLayer(@Name("name") String name,
                                               @Name("nodePropertyName") String nodePropertyName) {
        return addLayerOfType(name, "WKT", nodePropertyName);
    }

    // todo do we need this?
    @Procedure("spatial.layer")
    @PerformsWrites // TODO FIX - due to lazy evaluation of index count, updated during later reads, not during writes
    public Stream<NodeResult> getLayer(@Name("name") String name) {
        return streamNode(getLayerOrThrow(name).getLayerNode());
    }

    @Procedure("spatial.removeLayer")
    @PerformsWrites
    public void removeLayer(@Name("name") String name) {
        wrap(db).deleteLayer(name, new ProgressLoggingListener("Deleting layer '" + name + "'", log.infoLogger()));
    }

    // todo do we want to return anything ? or just a count?
    @Procedure("spatial.addNode")
    @PerformsWrites
    public Stream<NodeResult> addNodeToLayer(@Name("layerName") String name, @Name("node") Node node) {
        EditableLayer layer = getEditableLayerOrThrow(name);
        return streamNode(layer.add(node).getGeomNode());
    }

    // todo do we want to return anything ? or just a count?
    @Procedure("spatial.addNodes")
    @PerformsWrites
    public Stream<NodeResult> addNodesToLayer(@Name("layerName") String name, @Name("nodes") List<Node> nodes) {
        EditableLayer layer = getEditableLayerOrThrow(name);
        return nodes.stream().map(layer::add).map(SpatialDatabaseRecord::getGeomNode).map(NodeResult::new);
    }

    // todo do we want to return anything ? or just a count?
    @Procedure("spatial.addWKT")
    @PerformsWrites
    public Stream<NodeResult> addGeometryWKTToLayer(@Name("layerName") String name, @Name("geometry") String geometryWKT) throws ParseException {
        EditableLayer layer = getEditableLayerOrThrow(name);
        WKTReader reader = new WKTReader(layer.getGeometryFactory());
        return streamNode(addGeometryWkt(layer,reader,geometryWKT));
    }

    // todo do we want to return anything ? or just a count?
    @Procedure("spatial.addWKTs")
    @PerformsWrites
    public Stream<NodeResult> addGeometryWKTsToLayer(@Name("layerName") String name, @Name("geometry") List<String> geometryWKTs) throws ParseException {
        EditableLayer layer = getEditableLayerOrThrow(name);
        WKTReader reader = new WKTReader(layer.getGeometryFactory());
        return geometryWKTs.stream().map( geometryWKT -> addGeometryWkt(layer, reader, geometryWKT)).map(NodeResult::new);
    }

    private Node addGeometryWkt(EditableLayer layer, WKTReader reader, String geometryWKT) {
        try {
            Geometry geometry = reader.read(geometryWKT);
            return layer.add(geometry).getGeomNode();
        } catch (ParseException e) {
            throw new RuntimeException("Error parsing geometry: "+geometryWKT,e);
        }
    }

    // todo do we need this procedure??
    @Procedure("spatial.updateFromWKT")
    @PerformsWrites
    public Stream<NodeResult> updateGeometryFromWKT(@Name("layerName") String name, @Name("geometry") String geometryWKT,
                                                    @Name("geometryNodeId") long geometryNodeId) {
        try (Transaction tx = db.beginTx()) {
            EditableLayer layer = getEditableLayerOrThrow(name);
            WKTReader reader = new WKTReader(layer.getGeometryFactory());
            Geometry geometry = reader.read(geometryWKT);
            SpatialDatabaseRecord record = layer.getIndex().get(geometryNodeId);
            layer.getGeometryEncoder().encodeGeometry(geometry, record.getGeomNode());
            tx.success();
            return streamNode(record.getGeomNode());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Procedure("spatial.bbox")
    @PerformsWrites // TODO FIX
    public Stream<NodeResult> findGeometriesInBBox(
            @Name("layerName") String name,
            @Name("min") Object min,
            @Name("max") Object max) {
        Layer layer = getLayerOrThrow(name);
        // TODO why a SearchWithin and not a SearchIntersectWindow?
        Envelope envelope = new Envelope(toCoordinate(min),toCoordinate(max));
        return GeoPipeline
                .startWithinSearch(layer, layer.getGeometryFactory().toGeometry(envelope))
                .stream().map(GeoPipeFlow::getGeomNode).map(NodeResult::new);
    }


    @Procedure("spatial.closest")
    @PerformsWrites // TODO FIX
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


    @Procedure("spatial.withinDistance")
    @PerformsWrites // TODO FIX
    public Stream<NodeDistanceResult> findGeometriesWithinDistance(
            @Name("layerName") String name,
            @Name("coordinate") Object coordinate,
            @Name("distanceInKm") double distanceInKm) {

        Layer layer = getLayerOrThrow(name);
        return GeoPipeline
                .startNearestNeighborLatLonSearch(layer, toCoordinate(coordinate), distanceInKm)
                .sort(DISTANCE)
                .stream().map(r -> {
                    double distance = r.hasProperty(DISTANCE) ? ((Number) r.getProperty(DISTANCE)).doubleValue() : -1;
                    return new NodeDistanceResult(r.getGeomNode(), distance);
                });
    }


    @Procedure("spatial.intersects")
    @PerformsWrites // TODO FIX
    public Stream<NodeResult> findGeometriesIntersecting(
            @Name("layerName") String name,
            @Name("geometry") Object geometry) {

        Layer layer = getLayerOrThrow(name);
        return GeoPipeline
                .startIntersectSearch(layer, toGeometry(layer, geometry))
                .stream().map(GeoPipeFlow::getGeomNode).map(NodeResult::new);
    }

    private Geometry toGeometry(Layer layer, Object value) {
        GeometryFactory factory = layer.getGeometryFactory();
        if (value instanceof org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Point) {
            org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Point point = (org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Point) value;
            return factory.createPoint(new Coordinate(point.x(), point.y()));
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

    private Coordinate toCoordinate(Object value) {
        if (value instanceof GeographicPoint) {
            GeographicPoint point = (GeographicPoint) value;
            return new Coordinate(point.x(), point.y());
        }
        Map<String, Object> latLon = null;
        if (value instanceof PropertyContainer) {
            latLon = ((PropertyContainer) value).getProperties("latitude", "longitude","lat","lon");
        }
        if (value instanceof Map) latLon = (Map<String, Object>) value;
        Coordinate coord = toCoordinate(latLon);
        if (coord != null) return coord;
        throw new RuntimeException("Can't convert "+value+" to a coordinate");
    }

    private Coordinate toCoordinate(Map<String, Object> map) {
        if (map==null) return null;
        Coordinate coord = toCoordinate(map, "longitude", "latitude");
        if (coord == null) return toCoordinate(map, "lon", "lat");
        return coord;
    }

    private Coordinate toCoordinate(Map map, String xName, String yName) {
        if (map.containsKey(xName) && map.containsKey(yName))
            return new Coordinate(((Number) map.get(xName)).doubleValue(), ((Number) map.get(yName)).doubleValue());
        return null;
    }

    private EditableLayer getEditableLayerOrThrow(String name) {
        return (EditableLayer) getLayerOrThrow(wrap(db), name);
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
