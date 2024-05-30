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
package org.neo4j.gis.spatial.rtree;

import static java.awt.RenderingHints.KEY_ANTIALIASING;
import static java.awt.RenderingHints.KEY_TEXT_ANTIALIASING;
import static java.awt.RenderingHints.VALUE_ANTIALIAS_ON;
import static java.awt.RenderingHints.VALUE_TEXT_ANTIALIAS_ON;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import javax.imageio.ImageIO;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.style.Style;
import org.geotools.data.memory.MemoryFeatureCollection;
import org.geotools.data.neo4j.Neo4jFeatureBuilder;
import org.geotools.data.neo4j.StyledImageExporter;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.map.MapContent;
import org.geotools.renderer.lite.StreamingRenderer;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.GeometryEncoder;
import org.neo4j.gis.spatial.SpatialTopologyUtils;
import org.neo4j.gis.spatial.Utilities;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class RTreeImageExporter {

	private final CoordinateReferenceSystem crs;
	private File exportDir;
	final double zoom = 0.98;
	final double[] offset = new double[]{0, 0};
	final Rectangle displaySize = new Rectangle(2160, 2160);
	private final RTreeIndex index;
	private final GeometryFactory geometryFactory;
	private final GeometryEncoder geometryEncoder;
	private final SimpleFeatureType featureType;
	private final Color[] colors = new Color[]{Color.BLUE, Color.CYAN, Color.GREEN, Color.RED, Color.YELLOW, Color.PINK,
			Color.ORANGE};
	ReferencedEnvelope bounds;

	public RTreeImageExporter(GeometryFactory geometryFactory, GeometryEncoder geometryEncoder,
			CoordinateReferenceSystem crs, SimpleFeatureType featureType, RTreeIndex index) {
		this.geometryFactory = geometryFactory;
		this.geometryEncoder = geometryEncoder;
		this.featureType = featureType;
		this.index = index;
		this.crs = crs;
		this.bounds = new ReferencedEnvelope(crs);
	}

	public void initialize(Transaction tx) {
		Envelope bbox = index.getBoundingBox(tx);
		if (bbox != null) {
			bounds.expandToInclude(Utilities.fromNeo4jToJts(bbox));
		}
		bounds = SpatialTopologyUtils.adjustBounds(bounds, 1.0 / zoom, offset);
	}

	public void initialize(Transaction tx, Coordinate min, Coordinate max) {
		Envelope bbox = index.getBoundingBox(tx);
		if (bbox != null) {
			bounds.expandToInclude(Utilities.fromNeo4jToJts(bbox));
		}
		bounds.expandToInclude(new org.locationtech.jts.geom.Envelope(min.x, max.x, min.y, max.y));
		bounds = SpatialTopologyUtils.adjustBounds(bounds, 1.0 / zoom, offset);
	}

	public void saveRTreeLayers(Transaction tx, File imagefile, int levels) throws IOException {
		saveRTreeLayers(tx, imagefile, levels, new EmptyMonitor(), new ArrayList<>(), null, null);
	}

	public void saveRTreeLayers(Transaction tx, File imagefile, Node rootNode, int levels) throws IOException {
		saveRTreeLayers(tx, imagefile, rootNode, levels, new EmptyMonitor(), new ArrayList<>(), new ArrayList<>(), null,
				null);
	}

	public void saveRTreeLayers(Transaction tx, File imagefile, Node rootNode, List<Envelope> envelopes, int levels)
			throws IOException {
		saveRTreeLayers(tx, imagefile, rootNode, levels, new EmptyMonitor(), new ArrayList<>(), envelopes, null, null);
	}

	public void saveRTreeLayers(Transaction tx, File imagefile, int levels, TreeMonitor monitor) throws IOException {
		saveRTreeLayers(tx, imagefile, levels, monitor, new ArrayList<>(), null, null);
	}

	public void saveRTreeLayers(Transaction tx, File imagefile, int levels, TreeMonitor monitor, List<Node> foundNodes,
			Coordinate min, Coordinate max) throws IOException {
		saveRTreeLayers(tx, imagefile, index.getIndexRoot(tx), levels, monitor, foundNodes, new ArrayList<>(), min,
				max);
	}

	public void saveRTreeLayers(Transaction tx, File imagefile, Node rootNode, int levels, TreeMonitor monitor,
			List<Node> foundNodes, List<Envelope> envelopes, Coordinate min, Coordinate max) throws IOException {
		MapContent mapContent = new MapContent();
		drawBounds(mapContent, bounds, Color.WHITE);

		int indexHeight = RTreeIndex.getHeight(rootNode, 0);
		ArrayList<ArrayList<RTreeIndex.NodeWithEnvelope>> layers = new ArrayList<>(indexHeight);
		ArrayList<List<RTreeIndex.NodeWithEnvelope>> indexMatches = new ArrayList<>(indexHeight);
		for (int i = 0; i < indexHeight; i++) {
			indexMatches.add(monitor.getMatchedTreeNodes(indexHeight - i - 1).stream()
					.map(n -> new RTreeIndex.NodeWithEnvelope(n, index.getLeafNodeEnvelope(n)))
					.collect(Collectors.toList()));
			layers.add(new ArrayList<>());
			ArrayList<RTreeIndex.NodeWithEnvelope> nodes = layers.get(i);
			if (i == 0) {
				nodes.add(new RTreeIndex.NodeWithEnvelope(rootNode, RTreeIndex.getIndexNodeEnvelope(rootNode)));
			} else {
				for (RTreeIndex.NodeWithEnvelope parent : layers.get(i - 1)) {
					for (RTreeIndex.NodeWithEnvelope child : RTreeIndex.getIndexChildren(parent.node)) {
						layers.get(i).add(child);
					}
				}
			}
		}
		ArrayList<Node> allIndexedNodes = new ArrayList<>();
		for (Node node : index.getAllIndexedNodes(tx)) {
			allIndexedNodes.add(node);
		}
		drawGeometryNodes(mapContent, allIndexedNodes, Color.LIGHT_GRAY);
		for (int level = 0; level < Math.min(indexHeight, levels); level++) {
			ArrayList<RTreeIndex.NodeWithEnvelope> layer = layers.get(indexHeight - level - 1);
			System.out.println("Drawing index level " + level + " of " + layer.size() + " nodes");
			drawIndexNodes(level, mapContent, layer, colors[level % colors.length]);
			drawIndexNodes(2 + level * 2, mapContent, indexMatches.get(level), Color.MAGENTA);
		}
		drawEnvelopes(mapContent, envelopes, Color.ORANGE);
		drawGeometryNodes(mapContent, foundNodes, Color.RED);
		if (min != null && max != null) {
			drawEnvelope(mapContent, min, max, Color.RED);
		}
		saveMapContentToImageFile(mapContent, imagefile, bounds);
	}

	private void saveMapContentToImageFile(MapContent mapContent, File imagefile, ReferencedEnvelope bounds)
			throws IOException {
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
		Style style = StyledImageExporter.createStyleFromGeometry(featureType, color, Color.GRAY);
		mapContent.addLayer(new org.geotools.map.FeatureLayer(makeGeometryNodeFeatures(nodes, featureType), style));
	}

	private void drawEnvelopes(MapContent mapContent, List<Envelope> envelopes, Color color) {
		Style style = StyledImageExporter.createPolygonStyle(color, Color.WHITE, 0.8, 0.0, 3);
		mapContent.addLayer(new org.geotools.map.FeatureLayer(makeEnvelopeFeatures(envelopes), style));
	}

	private void drawIndexNodes(int level, MapContent mapContent, List<RTreeIndex.NodeWithEnvelope> nodes,
			Color color) {
		Style style = StyledImageExporter.createPolygonStyle(color, Color.WHITE, 0.8, 0.0, level + 1);
		mapContent.addLayer(new org.geotools.map.FeatureLayer(makeIndexNodeFeatures(nodes), style));
	}

	private void drawEnvelope(MapContent mapContent, Coordinate min, Coordinate max, Color color) {
		Style style = StyledImageExporter.createPolygonStyle(color, Color.WHITE, 0.8, 0.0, 3);
		mapContent.addLayer(new org.geotools.map.FeatureLayer(makeEnvelopeFeatures(min, max), style));
	}

	private void drawBounds(MapContent mapContent, ReferencedEnvelope bounds, Color color) {
		Style style = StyledImageExporter.createPolygonStyle(color, Color.WHITE, 0.8, 1.0, 6);
		double[] min = bounds.getLowerCorner().getCoordinate();
		double[] max = bounds.getUpperCorner().getCoordinate();
		mapContent.addLayer(new org.geotools.map.FeatureLayer(
				makeEnvelopeFeatures(new Coordinate(min[0], min[1]), new Coordinate(max[0], max[1])), style));
	}

	private MemoryFeatureCollection makeEnvelopeFeatures(List<Envelope> envelopes) {
		SimpleFeatureType featureType = Neo4jFeatureBuilder.getType("Polygon", Constants.GTYPE_POLYGON, crs,
				new String[]{});
		Neo4jFeatureBuilder featureBuilder = new Neo4jFeatureBuilder(featureType, new ArrayList<>());
		MemoryFeatureCollection features = new MemoryFeatureCollection(featureType);
		for (Envelope envelope : envelopes) {

			Coordinate[] coordinates = new Coordinate[]{
					new Coordinate(envelope.getMinX(), envelope.getMinY()),
					new Coordinate(envelope.getMinX(), envelope.getMaxY()),
					new Coordinate(envelope.getMaxX(), envelope.getMaxY()),
					new Coordinate(envelope.getMaxX(), envelope.getMinY()),
					new Coordinate(envelope.getMinX(), envelope.getMinY())
			};
			Geometry geometry = geometryFactory.createPolygon(coordinates);
			features.add(featureBuilder.buildFeature("envelope", geometry, new HashMap<>()));
		}
		return features;
	}

	private MemoryFeatureCollection makeEnvelopeFeatures(Coordinate min, Coordinate max) {
		SimpleFeatureType featureType = Neo4jFeatureBuilder.getType("Polygon", Constants.GTYPE_POLYGON, crs,
				new String[]{});
		Neo4jFeatureBuilder featureBuilder = new Neo4jFeatureBuilder(featureType, new ArrayList<>());
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
		SimpleFeatureType featureType = Neo4jFeatureBuilder.getType("Polygon", Constants.GTYPE_POLYGON, crs,
				new String[]{});
		Neo4jFeatureBuilder featureBuilder = new Neo4jFeatureBuilder(featureType, new ArrayList<>());
		MemoryFeatureCollection features = new MemoryFeatureCollection(featureType);
		for (RTreeIndex.NodeWithEnvelope node : nodes) {
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
		Neo4jFeatureBuilder featureBuilder = new Neo4jFeatureBuilder(featureType, new ArrayList<>());
		MemoryFeatureCollection features = new MemoryFeatureCollection(featureType);
		for (Node node : nodes) {
			Geometry geometry = geometryEncoder.decodeGeometry(node);
			features.add(featureBuilder.buildFeature(node.toString(), geometry, new HashMap<>()));
		}
		return features;
	}
}
