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
package org.geotools.data.neo4j;

import static java.awt.RenderingHints.KEY_ANTIALIASING;
import static java.awt.RenderingHints.KEY_TEXT_ANTIALIASING;
import static java.awt.RenderingHints.VALUE_ANTIALIAS_ON;
import static java.awt.RenderingHints.VALUE_TEXT_ANTIALIAS_ON;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.ImageIO;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.SimpleFeatureStore;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.FeatureType;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.style.FeatureTypeStyle;
import org.geotools.api.style.Fill;
import org.geotools.api.style.Graphic;
import org.geotools.api.style.LineSymbolizer;
import org.geotools.api.style.Mark;
import org.geotools.api.style.PointSymbolizer;
import org.geotools.api.style.PolygonSymbolizer;
import org.geotools.api.style.Rule;
import org.geotools.api.style.Stroke;
import org.geotools.api.style.Style;
import org.geotools.api.style.StyleFactory;
import org.geotools.data.DefaultTransaction;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.map.FeatureLayer;
import org.geotools.map.MapContent;
import org.geotools.renderer.lite.StreamingRenderer;
import org.geotools.xml.styling.SLDParser;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.gis.spatial.SpatialTopologyUtils;

public class StyledImageExporter {

	private static final Logger LOGGER = Logger.getLogger(StyledImageExporter.class.getName());
	private final Driver driver;
	private final String database;
	private File exportDir;
	double zoom = 1.0;
	final double[] offset = new double[]{0, 0};
	Rectangle displaySize = new Rectangle(400, 300);
	private String[] styleFiles;
	static final StyleFactory styleFactory = CommonFactoryFinder.getStyleFactory(null);
	static final FilterFactory filterFactory = CommonFactoryFinder.getFilterFactory(null);

	public StyledImageExporter(Driver driver, String database) {
		this.driver = driver;
		this.database = database;
	}

	public void setExportDir(String dir) {
		exportDir = (dir == null || dir.isEmpty()) ? null : (new File(dir)).getAbsoluteFile();
	}

	public void setZoom(double zoom) {
		this.zoom = zoom;
	}

	public void setSize(int width, int height) {
		this.displaySize = new Rectangle(width, height);
	}

	public void setStyleFiles(String[] files) {
		styleFiles = files;
	}

	public Style getStyle(int i) {
		if (styleFiles != null && i < styleFiles.length) {
			return getStyleFromSLDFile(styleFiles[i]);
		}
		return null;
	}

	/**
	 * When zooming in, it is useful to also control the location of the visable
	 * window using offsets from the center, in fractions of the bounding box
	 * dimensions. Use negative values to adjust left or down.
	 *
	 * @param fractionWidth  fraction of the width to shift right/left
	 * @param fractionHeight fraction of the height to shift up/down
	 */
	public void setOffset(double fractionWidth, double fractionHeight) {
		this.offset[0] = fractionWidth;
		this.offset[1] = fractionHeight;
	}

	private File checkFile(File file) {
		if (!file.isAbsolute() && exportDir != null) {
			file = new File(exportDir, file.getPath());
		}
		file = file.getAbsoluteFile();
		file.getParentFile().mkdirs();
		if (file.exists()) {
			LOGGER.info("Deleting previous file: " + file);
			file.delete();
		}
		return file;
	}

	public void saveLayerImage(String[] layerNames) throws IOException {
		saveLayerImage(layerNames, null, new File(layerNames[0] + ".png"), null);
	}

	public void saveLayerImage(String[] layerNames, File imageFile) throws IOException {
		saveLayerImage(layerNames, null, imageFile, null);
	}

	public void saveLayerImage(String layerName) throws IOException {
		saveLayerImage(layerName, null, new File(layerName + ".png"), null);
	}

	public void saveLayerImage(String layerName, String sldFile) throws IOException {
		saveLayerImage(layerName, sldFile, new File(layerName + ".png"), null);
	}

	public void saveLayerImage(String layerName, String sldFile, String imageFile) throws IOException {
		saveLayerImage(layerName, sldFile, new File(imageFile), null);
	}

	public void saveLayerImage(String layerName, String sldFile, File imagefile, ReferencedEnvelope bounds, int width,
			int height,
			double zoom) throws IOException {
		setZoom(zoom);
		setSize(width, height);
		saveLayerImage(layerName, sldFile, imagefile, bounds);
	}

	public void saveLayerImage(String layerName, String sldFile, File imagefile, ReferencedEnvelope bounds)
			throws IOException {
		String[] layerNames = new String[]{layerName};
		saveLayerImage(layerNames, sldFile, imagefile, bounds);
	}

	public void saveImage(FeatureCollection<SimpleFeatureType, SimpleFeature> features, String sldFile, File imagefile)
			throws IOException {
		saveImage(features, getStyleFromSLDFile(sldFile), imagefile);
	}

	public void saveImage(FeatureCollection<SimpleFeatureType, SimpleFeature> features, Style style, File imagefile)
			throws IOException {
		MapContent mapContent = new MapContent();
		mapContent.addLayer(new FeatureLayer(features, style));
		saveMapContentToImageFile(mapContent, imagefile, features.getBounds());
	}

	public void saveImage(FeatureCollection<SimpleFeatureType, SimpleFeature>[] features, Style[] styles,
			File imagefile, ReferencedEnvelope bounds) throws IOException {
		MapContent mapContent = new MapContent();
		for (int i = 0; i < features.length; i++) {
			mapContent.addLayer(new FeatureLayer(features[i], styles[i]));
		}
		saveMapContentToImageFile(mapContent, imagefile, bounds);
	}

	public void saveLayerImage(String[] layerNames, String sldFile, File imagefile, ReferencedEnvelope bounds)
			throws IOException {
		DataStore store = new Neo4jSpatialDataStore(driver, database);

		try (org.geotools.api.data.Transaction transaction = new DefaultTransaction("saveLayerImage")) {

			// debugStore(store, layerNames);
			StringBuilder names = new StringBuilder();
			for (String name : layerNames) {
				if (!names.isEmpty()) {
					names.append(", ");
				}
				names.append(name);
			}
			LOGGER.info("Exporting layers '" + names + "' to styled image " + imagefile.getPath());

			Style style = getStyleFromSLDFile(sldFile);

			MapContent mapContent = new MapContent();
			for (int i = 0; i < layerNames.length; i++) {
				SimpleFeatureStore featureSource = (SimpleFeatureStore) store.getFeatureSource(layerNames[i]);
				featureSource.setTransaction(transaction);
				Style featureStyle = style;
				if (featureStyle == null) {
					featureStyle = getStyle(i);
				}

				if (featureStyle == null) {
					featureStyle = createStyleFromGeometry(featureSource.getSchema(), Color.BLUE,
							Color.CYAN);
					LOGGER.info("Created style from geometry '" +
							featureSource.getSchema().getGeometryDescriptor()
									.getType() + "': " + featureStyle);
				}

				mapContent.addLayer(new FeatureLayer(featureSource, featureStyle));

				if (bounds == null) {
					bounds = featureSource.getBounds();
				} else {
					bounds.expandToInclude(featureSource.getBounds());
				}
			}

			saveMapContentToImageFile(mapContent, imagefile, bounds);
			transaction.commit();
		}
	}

	private static Style getStyleFromSLDFile(String sldFile) {
		Style style = null;
		if (sldFile != null) {
			style = createStyleFromSLD(sldFile);
			if (style != null) {
				LOGGER.info("Created style from sldFile '" + sldFile + "': " + style);
			}
		}
		return style;
	}

	private void saveMapContentToImageFile(MapContent mapContent, File imagefile, ReferencedEnvelope bounds)
			throws IOException {
		bounds = SpatialTopologyUtils.adjustBounds(bounds, 1.0 / zoom, offset);

		if (displaySize == null) {
			displaySize = new Rectangle(0, 0, 800, 600);
		}

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

		ImageIO.write(image, "png", checkFile(imagefile));
	}

	/**
	 * Create a Style object from a definition in a SLD document
	 */
	private static Style createStyleFromSLD(String sldFile) {
		try {
			SLDParser stylereader = new SLDParser(styleFactory, new File(sldFile).toURI().toURL());
			Style[] style = stylereader.readXML();
			return style[0];
		} catch (Exception e) {
			LOGGER.warning("Failed to read style from '" + sldFile + "': " + e.getMessage());
		}
		return null;
	}

	public static Style createDefaultStyle(Color strokeColor, Color fillColor) {
		return createStyleFromGeometry(null, strokeColor, fillColor);
	}

	/**
	 * Here is a programmatic alternative to using JSimpleStyleDialog to
	 * get a Style. These methods works out what sort of feature geometry
	 * we have in the shapefile and then delegates to an appropriate style
	 * creating method.
	 * <a href="https://docs.geotools.org/stable/userguide/examples/stylefunctionlab.html">stylefunctionlab</a>
	 */
	// TODO: Consider adding support for attribute based color schemes like in
	public static Style createStyleFromGeometry(FeatureType schema, Color strokeColor, Color fillColor) {
		if (schema != null) {
			Class<?> geomType = schema.getGeometryDescriptor().getType().getBinding();
			if (Polygon.class.isAssignableFrom(geomType)
					|| MultiPolygon.class.isAssignableFrom(geomType)) {
				return createPolygonStyle(strokeColor, fillColor);
			}
			if (LineString.class.isAssignableFrom(geomType)
					|| LinearRing.class.isAssignableFrom(geomType)
					|| MultiLineString.class.isAssignableFrom(geomType)) {
				return createLineStyle(strokeColor);
			}
			if (Point.class.isAssignableFrom(geomType)
					|| MultiPoint.class.isAssignableFrom(geomType)) {
				return createPointStyle(strokeColor, fillColor);
			}
		}

		Style style = styleFactory.createStyle();
		style.featureTypeStyles().addAll(createPolygonStyle(strokeColor, fillColor).featureTypeStyles());
		style.featureTypeStyles().addAll(createLineStyle(strokeColor).featureTypeStyles());
		style.featureTypeStyles().addAll(createPointStyle(strokeColor, fillColor).featureTypeStyles());
		return style;
	}

	/**
	 * Create a Style to draw polygon features
	 */
	public static Style createPolygonStyle(Color strokeColor, Color fillColor) {
		return createPolygonStyle(strokeColor, fillColor, 0.5, 0.5, 1);
	}

	/**
	 * Create a Style to draw polygon features
	 */
	public static Style createPolygonStyle(Color strokeColor, Color fillColor, double stokeGamma, double fillGamma,
			int strokeWidth) {
		// create a partially opaque outline stroke
		Stroke stroke = styleFactory.createStroke(
				filterFactory.literal(strokeColor),
				filterFactory.literal(strokeWidth),
				filterFactory.literal(stokeGamma));

		// create a partial opaque fill
		Fill fill = styleFactory.createFill(
				filterFactory.literal(fillColor),
				filterFactory.literal(fillGamma));

		/*
		 * Setting the geometryPropertyName arg to null signals that we want to
		 * draw the default geomettry of features
		 */
		PolygonSymbolizer sym = styleFactory.createPolygonSymbolizer(stroke, fill, null);

		Rule rule = styleFactory.createRule();
		rule.symbolizers().add(sym);
		try {
			rule.setFilter(ECQL.toFilter("geometryType(the_geom)='Polygon' or geometryType(the_geom)='MultiPoligon'"));
		} catch (CQLException e) {
			LOGGER.log(Level.WARNING, "", e);
		}

		FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle(rule);
		Style style = styleFactory.createStyle();
		style.featureTypeStyles().add(fts);

		return style;
	}

	/**
	 * Create a Style to draw line features
	 */
	private static Style createLineStyle(Color strokeColor) {
		Stroke stroke = styleFactory.createStroke(
				filterFactory.literal(strokeColor),
				filterFactory.literal(1));

		/*
		 * Setting the geometryPropertyName arg to null signals that we want to
		 * draw the default geomettry of features
		 */
		LineSymbolizer sym = styleFactory.createLineSymbolizer(stroke, null);

		Rule rule = styleFactory.createRule();
		rule.symbolizers().add(sym);
		try {
			rule.setFilter(ECQL.toFilter(
					"geometryType(the_geom)='LineString' or geometryType(the_geom)='LinearRing' or geometryType(the_geom)='MultiLineString'"));
		} catch (CQLException e) {
			LOGGER.log(Level.WARNING, "", e);
		}

		FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle(rule);
		Style style = styleFactory.createStyle();
		style.featureTypeStyles().add(fts);

		return style;
	}

	/**
	 * Create a Style to draw point features as circles with blue outlines
	 * and cyan fill
	 */
	private static Style createPointStyle(Color strokeColor, Color fillColor) {
		Mark mark = styleFactory.getCircleMark();
		mark.setStroke(styleFactory.createStroke(filterFactory.literal(strokeColor), filterFactory.literal(2)));
		mark.setFill(styleFactory.createFill(filterFactory.literal(fillColor)));

		Graphic gr = styleFactory.createDefaultGraphic();
		gr.graphicalSymbols().clear();
		gr.graphicalSymbols().add(mark);
		gr.setSize(filterFactory.literal(5));

		/*
		 * Setting the geometryPropertyName arg to null signals that we want to
		 * draw the default geomettry of features
		 */
		PointSymbolizer sym = styleFactory.createPointSymbolizer(gr, null);

		Rule rule = styleFactory.createRule();
		rule.symbolizers().add(sym);
		try {
			rule.setFilter(ECQL.toFilter("geometryType(the_geom)='Point' or geometryType(the_geom)='MultiPoint'"));
		} catch (CQLException e) {
			LOGGER.log(Level.WARNING, "", e);
		}

		FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle(rule);
		Style style = styleFactory.createStyle();
		style.featureTypeStyles().add(fts);

		return style;
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 6) {
			LOGGER.warning(
					"Too few arguments. Provide: <uri> <database> <user> <password> <exportdir> <stylefile> <zoom layer> <layers..>");
			System.exit(1);
		}
		String uri = args[0];
		String database = args[1];
		String user = args[2];
		String password = args[3];
		String exportdir = args[4];
		String stylefile = args[5];
		double zoom = Double.parseDouble(args[6]);
		var driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));

		StyledImageExporter imageExporter = new StyledImageExporter(driver, database);
		imageExporter.setExportDir(exportdir);
		imageExporter.setZoom(zoom);
		imageExporter.setSize(800, 600);
		for (int i = 6; i < args.length; i++) {
			imageExporter.saveLayerImage(args[i], stylefile);
		}
	}
}
