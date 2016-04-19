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
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

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

    public static class NodeResult {
        public final Node node;

        public NodeResult(Node node) {
            this.node = node;
        }
    }
    public static class NameResult
    {
        public final String name;

        public NameResult(String name) {
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

    @Procedure("spatial.layers")
    @PerformsWrites
    public Stream<NameResult> getAllLayers()
    {
        Stream.Builder<NameResult> builder = Stream.builder();
        for ( String name : wrap( db ).getLayerNames() )
        {
            builder.accept( new NameResult(name) );
        }
        return builder.build();
    }

    @Procedure("spatial.addPointLayer")
    @PerformsWrites
    public Stream<NodeResult> addSimplePointLayer(@Name("layer") String layer) {
        return streamNode(wrap(db).getOrCreatePointLayer(layer, "longitude", "latitude").getLayerNode());
    }

    @Procedure("spatial.addPointLayerNamed")
    @PerformsWrites
    public Stream<NodeResult> addSimplePointLayer(
            @Name("layer") String layer,
            @Name("lat") String lat,
            @Name("lon") String lon) {
        return streamNode(wrap(db).getOrCreatePointLayer(layer, lon, lat).getLayerNode());
    }

    private Stream<NodeResult> streamNode(Node node) {
        return Stream.of(new NodeResult(node));
    }

    @Procedure("spatial.addWKTLayer")
    @PerformsWrites
    public Stream<NodeResult> addWKTLayer(@Name("layer") String layer,
                                               @Name("nodePropertyName") String nodePropertyName) {
        return streamNode(wrap(db).getOrCreateEditableLayer(layer, "WKT", nodePropertyName).getLayerNode());
    }

    @Procedure("spatial.addConfiguredLayer")
    @PerformsWrites
    public Stream<NodeResult> addEditableLayer(@Name("layer") String layer,
                                               @Name("format") String format,
                                               @Name("nodePropertyName") String nodePropertyName) {
        return streamNode(wrap(db).getOrCreateEditableLayer(layer, format, nodePropertyName).getLayerNode());
    }

    // todo do we need this?
    @Procedure("spatial.layer")
    @PerformsWrites // TODO FIX
    public Stream<NodeResult> getLayer(@Name("layer") String layer) {
        return streamNode(wrap(db).getLayer(layer).getLayerNode());
    }


    // todo do we want to return anything ? or just a count?
    @Procedure("spatial.addNode")
    @PerformsWrites
    public Stream<NodeResult> addNodeToLayer(@Name("layer") String layer, @Name("node") Node node) {
        SpatialDatabaseService spatialService = wrap(db);
        EditableLayer spatialLayer = (EditableLayer) spatialService.getLayer(layer);
        return streamNode(spatialLayer.add(node).getGeomNode());
    }

    // todo do we want to return anything ? or just a count?
    @Procedure("spatial.addNodes")
    @PerformsWrites
    public Stream<NodeResult> addNodesToLayer(@Name("layer") String layer, @Name("nodes") List<Node> nodes) {
        SpatialDatabaseService spatialService = wrap(db);
        EditableLayer spatialLayer = (EditableLayer) spatialService.getLayer(layer);
        return nodes.stream().map(spatialLayer::add).map(SpatialDatabaseRecord::getGeomNode).map(NodeResult::new);
    }

    // todo do we want to return anything ? or just a count?
    @Procedure("spatial.addWKT")
    @PerformsWrites
    public Stream<NodeResult> addGeometryWKTToLayer(@Name("layer") String layer, @Name("geometry") String geometryWKT) throws ParseException {
        EditableLayer spatialLayer = (EditableLayer) wrap(db).getLayer(layer);
        WKTReader reader = new WKTReader(spatialLayer.getGeometryFactory());
        return streamNode(addGeometryWkt(spatialLayer,reader,geometryWKT));
    }

    // todo do we want to return anything ? or just a count?
    @Procedure("spatial.addWKTs")
    @PerformsWrites
    public Stream<NodeResult> addGeometryWKTsToLayer(@Name("layer") String layer, @Name("geometry") List<String> geometryWKTs) throws ParseException {
        EditableLayer spatialLayer = (EditableLayer) wrap(db).getLayer(layer);
        WKTReader reader = new WKTReader(spatialLayer.getGeometryFactory());
        return geometryWKTs.stream().map( geometryWKT -> addGeometryWkt(spatialLayer, reader, geometryWKT)).map(NodeResult::new);
    }

    private Node addGeometryWkt(EditableLayer spatialLayer, WKTReader reader, String geometryWKT) {
        try {
            Geometry geometry = reader.read(geometryWKT);
            return spatialLayer.add(geometry).getGeomNode();
        } catch (ParseException e) {
            throw new RuntimeException("Error parsing geometry: "+geometryWKT,e);
        }
    }

    // todo do we need this procedure??
    @Procedure("spatial.updateFromWKT")
    @PerformsWrites
    public Stream<NodeResult> updateGeometryFromWKT(@Name("layer") String layer, @Name("geometry") String geometryWKT,
                                                    @Name("geometryNodeId") long geometryNodeId) {
        SpatialDatabaseService spatialService = wrap(db);
        try (Transaction tx = db.beginTx()) {
            EditableLayer spatialLayer = (EditableLayer) spatialService.getLayer(layer);
            WKTReader reader = new WKTReader(spatialLayer.getGeometryFactory());
            Geometry geometry = reader.read(geometryWKT);
            SpatialDatabaseRecord record = spatialLayer.getIndex().get(geometryNodeId);
            spatialLayer.getGeometryEncoder().encodeGeometry(geometry, record.getGeomNode());
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
            @Name("layer") String layerName,
            @Name("min") Object min,
            @Name("max") Object max) {
        Layer layer = getLayer(wrap(db), layerName);
        // TODO why a SearchWithin and not a SearchIntersectWindow?
        Envelope envelope = new Envelope(toCoordinate(min),toCoordinate(max));
        return GeoPipeline
                .startWithinSearch(layer, layer.getGeometryFactory().toGeometry(envelope))
                .stream().map(GeoPipeFlow::getGeomNode).map(NodeResult::new);
    }


    @Procedure("spatial.closest")
    @PerformsWrites // TODO FIX
    public Stream<NodeResult> findClosestGeometries(
            @Name("layer") String layerName,
            @Name("coordinate") Object coordinate,
            @Name("distanceInKm") double distanceInKm) {
        Layer layer = getLayer(wrap(db), layerName);
        GeometryFactory factory = layer.getGeometryFactory();
        Point point = factory.createPoint(toCoordinate(coordinate));
        List<SpatialTopologyUtils.PointResult> edgeResults = SpatialTopologyUtils.findClosestEdges(point, layer, distanceInKm);
        return edgeResults.stream().map(e -> e.getValue().getGeomNode()).map(NodeResult::new);
    }


    @Procedure("spatial.withinDistance")
    @PerformsWrites // TODO FIX
    public Stream<NodeDistanceResult> findGeometriesWithinDistance(
            @Name("layer") String layerName,
            @Name("coordinate") Object coordinate,
            @Name("distanceInKm") double distanceInKm) {

        Layer layer = getLayer(wrap(db), layerName);

        return GeoPipeline
                .startNearestNeighborLatLonSearch(layer, toCoordinate(coordinate), distanceInKm)
                .sort(DISTANCE)
                .stream().map(r -> {
                    double distance = r.hasProperty(DISTANCE) ? ((Number) r.getProperty(DISTANCE)).doubleValue() : -1;
                    return new NodeDistanceResult(r.getGeomNode(), distance);
                });
    }

    private Layer getLayer(SpatialDatabaseService spatialService, String layerName) {
        Layer layer = spatialService.getDynamicLayer(layerName);
        if (layer != null) return layer;
        layer = spatialService.getLayer(layerName);
        if (layer != null) return layer;
        throw new RuntimeException("Can't find layer named '"+layerName+"' please create with spatial.addPointLayer('"+layerName+"')");
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

    private SpatialDatabaseService wrap(GraphDatabaseService db) {
        return new SpatialDatabaseService(db);
    }
}
