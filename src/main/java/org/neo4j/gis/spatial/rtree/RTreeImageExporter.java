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
package org.neo4j.gis.spatial.rtree;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.geotools.data.memory.MemoryFeatureCollection;
import org.geotools.data.neo4j.Neo4jFeatureBuilder;
import org.geotools.data.neo4j.StyledImageExporter;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.map.MapContent;
import org.geotools.renderer.lite.StreamingRenderer;
import org.geotools.styling.Style;
import org.neo4j.gis.spatial.*;
import org.neo4j.graphdb.Node;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

import static java.awt.RenderingHints.*;

public class RTreeImageExporter {
    private CoordinateReferenceSystem crs;
    private File exportDir;
    double zoom = 0.98;
    double[] offset = new double[]{0, 0};
    Rectangle displaySize = new Rectangle(2160, 2160);
    private Layer layer;
    private RTreeIndex index;
    private GeometryEncoder geometryEncoder;
    private final Color[] colors = new Color[]{Color.BLUE, Color.CYAN, Color.GREEN, Color.RED, Color.YELLOW, Color.PINK, Color.ORANGE};
    ReferencedEnvelope bounds;

    public RTreeImageExporter(Layer layer, RTreeIndex index, Coordinate min, Coordinate max) {
        initialize(layer, index);
        bounds.expandToInclude(new com.vividsolutions.jts.geom.Envelope(min.x, max.x, min.y, max.y));
        bounds = SpatialTopologyUtils.adjustBounds(bounds, 1.0 / zoom, offset);
    }

    public RTreeImageExporter(Layer layer, RTreeIndex index) {
        initialize(layer, index);
        bounds = SpatialTopologyUtils.adjustBounds(bounds, 1.0 / zoom, offset);
    }

    public void initialize(Layer layer, RTreeIndex index) {
        this.layer = layer;
        this.index = index;
        this.geometryEncoder = layer.getGeometryEncoder();
        this.crs = layer.getCoordinateReferenceSystem();
        this.bounds = new ReferencedEnvelope(crs);
        Envelope bbox = index.getBoundingBox();
        if (bbox != null) {
            bounds.expandToInclude(Utilities.fromNeo4jToJts(bbox));
        }
    }


    public void saveRTreeLayers(File imagefile, int levels) throws IOException {
        saveRTreeLayers(imagefile, levels, new EmptyMonitor(), new ArrayList<>(), null, null);
    }

    public void saveRTreeLayers(File imagefile, int levels, TreeMonitor monitor) throws IOException {
        saveRTreeLayers(imagefile, levels, monitor, new ArrayList<>(), null, null);
    }

    public void saveRTreeLayers(File imagefile, int levels, TreeMonitor monitor, List<Node> foundNodes, Coordinate min, Coordinate max) throws IOException {
        MapContent mapContent = new MapContent();
        drawBounds(mapContent, bounds, Color.WHITE);

        int indexHeight = index.getHeight(index.getIndexRoot(), 0);
        ArrayList<ArrayList<RTreeIndex.NodeWithEnvelope>> layers = new ArrayList<>(indexHeight);
        ArrayList<List<RTreeIndex.NodeWithEnvelope>> indexMatches = new ArrayList<>(indexHeight);
        for (int i = 0; i < indexHeight; i++) {
            indexMatches.add(monitor.getMatchedTreeNodes(indexHeight - i - 1).stream()
                    .map(n -> new RTreeIndex.NodeWithEnvelope(n, index.getLeafNodeEnvelope(n)))
                    .collect(Collectors.toList()));
            layers.add(new ArrayList<>());
            ArrayList<RTreeIndex.NodeWithEnvelope> nodes = layers.get(i);
            if (i == 0) {
                Node indexRoot = index.getIndexRoot();
                nodes.add(new RTreeIndex.NodeWithEnvelope(indexRoot, index.getIndexNodeEnvelope(indexRoot)));
            } else {
                for (RTreeIndex.NodeWithEnvelope parent : layers.get(i - 1)) {
                    for (RTreeIndex.NodeWithEnvelope child : index.getIndexChildren(parent.node)) {
                        layers.get(i).add(child);
                    }
                }
            }
        }
        ArrayList<Node> allIndexedNodes = new ArrayList<>();
        for(Node node: index.getAllIndexedNodes()) {
            allIndexedNodes.add(node);
        }
        drawGeometryNodes(mapContent, allIndexedNodes, Color.LIGHT_GRAY);
        for (int level = 0; level < Math.min(indexHeight, levels); level++) {
            ArrayList<RTreeIndex.NodeWithEnvelope> layer = layers.get(indexHeight - level - 1);
            System.out.println("Drawing index level " + level + " of " + layer.size() + " nodes");
            drawIndexNodes(level, mapContent, layer, colors[level % colors.length]);
            drawIndexNodes(2 + level * 2, mapContent, indexMatches.get(level), Color.MAGENTA);
        }
        drawGeometryNodes(mapContent, foundNodes, Color.RED);
        if (min != null && max != null) {
            drawEnvelope(mapContent, min, max, Color.RED);
        }
        saveMapContentToImageFile(mapContent, imagefile, bounds);
    }

    private void saveMapContentToImageFile(MapContent mapContent, File imagefile, ReferencedEnvelope bounds) throws IOException {
        RenderingHints hints = new RenderingHints(KEY_ANTIALIASING, VALUE_ANTIALIAS_ON);
        hints.put(KEY_TEXT_ANTIALIASING, VALUE_TEXT_ANTIALIAS_ON);

        BufferedImage image = new BufferedImage(displaySize.width, displaySize.height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D graphics = image.createGraphics();

        StreamingRenderer renderer = new StreamingRenderer();
        renderer.setJava2DHints(hints);
        renderer.setMapContent(mapContent);
        renderer.paint(graphics, displaySize, bounds);

        graphics.dispose();
        mapContent.dispose();

        imagefile = checkFile(imagefile);
        System.out.println("Writing image to disk: " + imagefile);
        ImageIO.write(image, "png", imagefile);
    }

    private File checkFile(File file) {
        if (!file.isAbsolute() && exportDir != null) {
            file = new File(exportDir, file.getPath());
        }
        file = file.getAbsoluteFile();
        file.getParentFile().mkdirs();
        if (file.exists()) {
            System.out.println("Deleting previous index image file: " + file);
            file.delete();
        }
        return file;
    }

    private void drawGeometryNodes(MapContent mapContent, List<Node> nodes, Color color) {
        SimpleFeatureType featureType = Neo4jFeatureBuilder.getTypeFromLayer(layer);
        Style style = StyledImageExporter.createStyleFromGeometry(featureType, color, color.GRAY);
        mapContent.addLayer(new org.geotools.map.FeatureLayer(makeGeometryNodeFeatures(nodes, featureType), style));
    }

    private void drawIndexNodes(int level, MapContent mapContent, List<RTreeIndex.NodeWithEnvelope> nodes, Color color) throws IOException {
        Style style = StyledImageExporter.createPolygonStyle(color, Color.WHITE, 0.8, 0.0, level + 1);
        mapContent.addLayer(new org.geotools.map.FeatureLayer(makeIndexNodeFeatures(nodes), style));
    }

    private void drawEnvelope(MapContent mapContent, Coordinate min, Coordinate max, Color color) throws IOException {
        Style style = StyledImageExporter.createPolygonStyle(color, Color.WHITE, 0.8, 0.0, 3);
        mapContent.addLayer(new org.geotools.map.FeatureLayer(makeEnvelopeFeatures(min, max), style));
    }

    private void drawBounds(MapContent mapContent, ReferencedEnvelope bounds, Color color) throws IOException {
        Style style = StyledImageExporter.createPolygonStyle(color, Color.WHITE, 0.8, 1.0, 6);
        double[] min = bounds.getLowerCorner().getCoordinate();
        double[] max = bounds.getUpperCorner().getCoordinate();
        mapContent.addLayer(new org.geotools.map.FeatureLayer(makeEnvelopeFeatures(new Coordinate(min[0], min[1]), new Coordinate(max[0], max[1])), style));
    }

    private MemoryFeatureCollection makeEnvelopeFeatures(Coordinate min, Coordinate max) {
        SimpleFeatureType featureType = Neo4jFeatureBuilder.getType("Polygon", Constants.GTYPE_POLYGON, crs, new String[]{});
        Neo4jFeatureBuilder featureBuilder = new Neo4jFeatureBuilder(featureType, new ArrayList<String>());
        GeometryFactory geometryFactory = layer.getGeometryFactory();
        MemoryFeatureCollection features = new MemoryFeatureCollection(featureType);
        Coordinate[] coordinates = new Coordinate[]{
                new Coordinate(min.x, min.y),
                new Coordinate(min.x, max.y),
                new Coordinate(max.x, max.y),
                new Coordinate(max.x, min.y),
                new Coordinate(min.x, min.y)
        };
        Geometry geometry = geometryFactory.createPolygon(coordinates);
        features.add(featureBuilder.buildFeature("envelope", geometry, new HashMap<>()));
        return features;
    }

    private MemoryFeatureCollection makeIndexNodeFeatures(List<RTreeIndex.NodeWithEnvelope> nodes) {
        SimpleFeatureType featureType = Neo4jFeatureBuilder.getType("Polygon", Constants.GTYPE_POLYGON, crs, new String[]{});
        Neo4jFeatureBuilder featureBuilder = new Neo4jFeatureBuilder(featureType, new ArrayList<String>());
        GeometryFactory geometryFactory = layer.getGeometryFactory();
        MemoryFeatureCollection features = new MemoryFeatureCollection(featureType);
        for (int i = 0; i < nodes.size(); i++) {
            RTreeIndex.NodeWithEnvelope node = nodes.get(i);
            Envelope envelope = node.envelope;
            Coordinate[] coordinates = new Coordinate[]{
                    new Coordinate(envelope.getMinX(), envelope.getMinY()),
                    new Coordinate(envelope.getMinX(), envelope.getMaxY()),
                    new Coordinate(envelope.getMaxX(), envelope.getMaxY()),
                    new Coordinate(envelope.getMaxX(), envelope.getMinY()),
                    new Coordinate(envelope.getMinX(), envelope.getMinY())
            };
            Geometry geometry = geometryFactory.createPolygon(coordinates);
            features.add(featureBuilder.buildFeature(node.toString(), geometry, new HashMap<>()));
        }
        return features;
    }

    private MemoryFeatureCollection makeGeometryNodeFeatures(List<Node> nodes, SimpleFeatureType featureType) {
        Neo4jFeatureBuilder featureBuilder = new Neo4jFeatureBuilder(featureType, new ArrayList<String>());
        MemoryFeatureCollection features = new MemoryFeatureCollection(featureType);
        for (Node node : nodes) {
            Geometry geometry = layer.getGeometryEncoder().decodeGeometry(node);
            features.add(featureBuilder.buildFeature(node.toString(), geometry, new HashMap<>()));
        }
        return features;
    }
}
