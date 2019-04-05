/**
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
package org.neo4j.gis.spatial.server.plugin;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.gis.spatial.*;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.pipes.processing.OrthodromicDistance;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.plugins.*;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singleton;

@Description("a set of extensions that perform operations using the neo4j-spatial component")
public class SpatialPlugin extends ServerPlugin {

    private GraphDatabaseService db;
    private SpatialDatabaseService spatialDatabaseService;

    @PluginTarget(GraphDatabaseService.class)
    @Description("add a new layer specialized at storing simple point location data")
    public Iterable<Node> addSimplePointLayer(
            @Source GraphDatabaseService db,
            @Description("The layer to find or create.") @Parameter(name = "layer") String layer,
            @Description("The node property that contains the latitude. Default is 'lat'") @Parameter(name = "lat", optional = true) String lat,
            @Description("The node property that contains the longitude. Default is 'lon'") @Parameter(name = "lon", optional = true) String lon,
            @Description("The type of index to use. Default is 'rtree'") @Parameter(name = "index", optional = true) String index) {
        System.out.println("Creating new layer '" + layer + "' unless it already exists");
        SpatialDatabaseService spatialService = getSpatialDatabaseService(db);
        return singleton(spatialService.getOrCreateSimplePointLayer(layer, index, lon, lat).getLayerNode());
    }

    @PluginTarget(GraphDatabaseService.class)
    @Description("add a new layer specialized at storing generic geometry data in WKB")
    public Iterable<Node> addEditableLayer(@Source GraphDatabaseService db,
                                           @Description("The layer to find or create.") @Parameter(name = "layer") String layer,
                                           @Description("The format for internal representation, either WKB or WKT") @Parameter(name = "format") String format,
                                           @Description("The name of the node property carrying the spatial geometry info") @Parameter(name = "nodePropertyName") String nodePropertyName
    ) {
        System.out.println("Creating new layer '" + layer + "' unless it already exists");
        SpatialDatabaseService spatialService = getSpatialDatabaseService(db);
        return singleton(spatialService.getOrCreateEditableLayer(layer, format, nodePropertyName).getLayerNode());
    }

    @PluginTarget(GraphDatabaseService.class)
    @Description("add a new dynamic layer exposing a filtered view of an existing layer")
    public Iterable<Node> addCQLDynamicLayer(
            @Source GraphDatabaseService db,
            @Description("The master layer to find") @Parameter(name = "master_layer") String master_layer,
            @Description("The name for the new dynamic layer") @Parameter(name = "name") String name,
            @Description("The type of geometry to use for streaming data from the new view") @Parameter(name = "geometry", optional = true) String geometry,
            @Description("The CQL query to use for defining this dynamic layer") @Parameter(name = "layer") String query) {
        System.out.println("Creating new dynamic layer '" + name + "' from existing layer '" + master_layer + "'");
        SpatialDatabaseService spatialService = getSpatialDatabaseService(db);
        DynamicLayer dynamicLayer = spatialService.asDynamicLayer(spatialService.getLayer(master_layer));
        int gtype = SpatialDatabaseService.convertGeometryNameToType(geometry);

        Iterable<Node> node;
        Transaction tx = db.beginTx();
        try {
            node = singleton(dynamicLayer.addLayerConfig(name, gtype, query).getLayerNode());
            tx.success();
            return node;
        } catch (Exception e) {
            tx.failure();
            e.printStackTrace();
            throw new RuntimeException("Error adding dynamic layer: " + name, e);
        } finally {
            tx.close();
        }
    }

    @PluginTarget(GraphDatabaseService.class)
    @Description("find an existing layer")
    public Iterable<Node> getLayer(@Source GraphDatabaseService db,
                                   @Description("The layer to find.") @Parameter(name = "layer") String layer) {
//        System.out.println("Finding layer '" + layer + "'");
        SpatialDatabaseService spatialService = getSpatialDatabaseService(db);
        return singleton(spatialService.getLayer(layer).getLayerNode());
    }

    @PluginTarget(GraphDatabaseService.class)
    @Description("add a geometry node to a layer, as long as the node contains the geometry information appropriate to this layer.")
    public Iterable<Node> addNodeToLayer(@Source GraphDatabaseService db,
                                         @Description("The node representing a geometry to add to the layer") @Parameter(name = "node") Node node,
                                         @Description("The layer to add the node to.") @Parameter(name = "layer") String layer) {
        SpatialDatabaseService spatialService = getSpatialDatabaseService(db);
//        System.out.println("adding node " + node + " to layer '" + layer + "'");

        EditableLayer spatialLayer = (EditableLayer) spatialService.getLayer(layer);
        Transaction tx = db.beginTx();
        try {
            spatialLayer.add(node);
            tx.success();
            return singleton(node);
        } catch (Exception e) {
            tx.failure();
            e.printStackTrace();
            throw new RuntimeException("Error adding nodes to layer "+layer,e);
        } finally {
            tx.close();
        }
    }

    @PluginTarget(GraphDatabaseService.class)
    @Description("adds many geometry nodes (about 10k-50k) to a layer, as long as the nodes contain the geometry information appropriate to this layer.")
    public Iterable<Node> addNodesToLayer(@Source GraphDatabaseService db,
                                         @Description("The nodes representing geometries to add to the layer") @Parameter(name = "nodes") List<Node> nodes,
                                         @Description("The layer to add the nodes to.") @Parameter(name = "layer") String layer) {
        SpatialDatabaseService spatialService = getSpatialDatabaseService(db);
//        System.out.println("adding node " + node + " to layer '" + layer + "'");

        EditableLayer spatialLayer = (EditableLayer) spatialService.getLayer(layer);
        Transaction tx = db.beginTx();
        try {
            for (Node node : nodes) {
                spatialLayer.add(node);
            }
            tx.success();
            return nodes;
        } catch (Exception e) {
            tx.failure();
            e.printStackTrace();
            throw new RuntimeException("Error adding nodes to layer "+layer,e);
        } finally {
            tx.close();
        }
    }

    private SpatialDatabaseService getSpatialDatabaseService(GraphDatabaseService db) {
        if (this.db != db) {
            this.db = db;
            spatialDatabaseService = new SpatialDatabaseService(db);
        }
        return spatialDatabaseService;
    }

    @PluginTarget(GraphDatabaseService.class)
    @Description("add a geometry specified in WKT format to a layer, encoding in the specified layers encoding schemea.")
    public Iterable<Node> addGeometryWKTToLayer(@Source GraphDatabaseService db,
                                                @Description("The geometry in WKT to add to the layer") @Parameter(name = "geometry") String geometryWKT,
                                                @Description("The layer to add the node to.") @Parameter(name = "layer") String layer) {
//        System.out.println("Adding geometry to layer '" + layer + "': " + geometryWKT);
        SpatialDatabaseService spatialService = getSpatialDatabaseService(db);

        EditableLayer spatialLayer = (EditableLayer) spatialService.getLayer(layer);
        try (Transaction tx = db.beginTx()) {
            WKTReader reader = new WKTReader(spatialLayer.getGeometryFactory());
            Geometry geometry = reader.read(geometryWKT);
            SpatialDatabaseRecord record = spatialLayer.add(geometry);
            tx.success();
            return singleton(record.getGeomNode());
        } catch (ParseException e) {
            System.err.println("Invalid Geometry: " + e.getLocalizedMessage());
        }
        return null;
    }

    @PluginTarget(GraphDatabaseService.class)
    @Description("search a layer for geometries in a bounding box. To achieve more complex CQL searches, pre-define the dynamic layer with addCQLDynamicLayer.")
    public Iterable<Node> findGeometriesInBBox(
            @Source GraphDatabaseService db,
            @Description("The minimum x value of the bounding box") @Parameter(name = "minx") double minx,
            @Description("The maximum x value of the bounding box") @Parameter(name = "maxx") double maxx,
            @Description("The minimum y value of the bounding box") @Parameter(name = "miny") double miny,
            @Description("The maximum y value of the bounding box") @Parameter(name = "maxy") double maxy,
            @Description("The layer to search. Can be a dynamic layer with pre-defined CQL filter.") @Parameter(name = "layer") String layerName) {
//        System.out.println("Finding Geometries in layer '" + layerName + "'");
        SpatialDatabaseService spatialService = getSpatialDatabaseService(db);

        try (Transaction tx = db.beginTx()) {

            Layer layer = spatialService.getDynamicLayer(layerName);
            if (layer == null) {
                layer = spatialService.getLayer(layerName);
            }
            // TODO why a SearchWithin and not a SearchIntersectWindow?

            List<Node> result = GeoPipeline
                    .startWithinSearch(layer, layer.getGeometryFactory().toGeometry(new Envelope(minx, maxx, miny, maxy)))
                    .toNodeList();
            tx.success();
            return result;
        }
    }

    @PluginTarget(GraphDatabaseService.class)
    @Description("search a layer for geometries intersecting a bounding box. To achieve more complex CQL searches, pre-define the dynamic layer with addCQLDynamicLayer.")
    public Iterable<Node> findGeometriesIntersectingBBox(
            @Source GraphDatabaseService db,
            @Description("The minimum x value of the bounding box") @Parameter(name = "minx") double minx,
            @Description("The maximum x value of the bounding box") @Parameter(name = "maxx") double maxx,
            @Description("The minimum y value of the bounding box") @Parameter(name = "miny") double miny,
            @Description("The maximum y value of the bounding box") @Parameter(name = "maxy") double maxy,
            @Description("The layer to search. Can be a dynamic layer with pre-defined CQL filter.") @Parameter(name = "layer") String layerName) {
//        System.out.println("Finding Geometries intersecting layer '" + layerName + "'");
        SpatialDatabaseService spatialService = getSpatialDatabaseService(db);

        try (Transaction tx = db.beginTx()) {

            Layer layer = spatialService.getDynamicLayer(layerName);
            if (layer == null) {
                layer = spatialService.getLayer(layerName);
            }

            List<Node> result = GeoPipeline
                    .startIntersectWindowSearch(layer, new Envelope(minx, maxx, miny, maxy))
                    .toNodeList();
            tx.success();
            return result;
        }
    }

    @PluginTarget(GraphDatabaseService.class)
    @Description("search a layer for the closest geometries and return them.")
    public Iterable<Node> findClosestGeometries(
            @Source GraphDatabaseService db,
            @Description("The x value of a point") @Parameter(name = "pointX") double pointX,
            @Description("The y value of a point") @Parameter(name = "pointY") double pointY,
            @Description("The maximum distance in km") @Parameter(name = "distanceInKm") double distanceInKm,
            @Description("The layer to search. Can be a dynamic layer with pre-defined CQL filter.") @Parameter(name = "layer") String layerName) {
        SpatialDatabaseService spatialService = getSpatialDatabaseService(db);
        try (Transaction tx = db.beginTx()) {
            Layer layer = spatialService.getDynamicLayer(layerName);
            if (layer == null) {
                layer = spatialService.getLayer(layerName);
            }
            GeometryFactory factory = layer.getGeometryFactory();
            Point point = factory.createPoint(new Coordinate(pointX, pointY));
            List<SpatialTopologyUtils.PointResult> edgeResults = SpatialTopologyUtils.findClosestEdges(point, layer, distanceInKm);
            List<Node> results = new ArrayList<Node>();
            for (SpatialTopologyUtils.PointResult result : edgeResults) {
                results.add(result.getValue().getGeomNode());
            }
            tx.success();
            return results;
        }
    }

    @PluginTarget(GraphDatabaseService.class)
    @Description("search a layer for geometries within a distance of a point. To achieve more complex CQL searches, pre-define the dynamic layer with addCQLDynamicLayer.")
    public Iterable<Node> findGeometriesWithinDistance(
            @Source GraphDatabaseService db,
            @Description("The x value of a point") @Parameter(name = "pointX") double pointX,
            @Description("The y value of a point") @Parameter(name = "pointY") double pointY,
            @Description("The distance from the point to search") @Parameter(name = "distanceInKm") double distanceInKm,
            @Description("The layer to search. Can be a dynamic layer with pre-defined CQL filter.") @Parameter(name = "layer") String layerName) {
//        System.out.println("Finding Geometries in layer '" + layerName + "'");
        SpatialDatabaseService spatialService = getSpatialDatabaseService(db);

        try (Transaction tx = db.beginTx()) {
            Layer layer = spatialService.getDynamicLayer(layerName);
            if (layer == null) {
                layer = spatialService.getLayer(layerName);
            }

            List<Node> result = GeoPipeline
                    .startNearestNeighborLatLonSearch(layer, new Coordinate(pointX, pointY), distanceInKm)
                    .sort(OrthodromicDistance.DISTANCE).toNodeList();
            tx.success();
            return result;
        }
    }
}
