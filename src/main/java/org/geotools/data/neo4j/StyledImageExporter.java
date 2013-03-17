/**
 * Copyright (c) 2010-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.geotools.data.neo4j;

import static java.awt.RenderingHints.KEY_ANTIALIASING;
import static java.awt.RenderingHints.KEY_TEXT_ANTIALIASING;
import static java.awt.RenderingHints.VALUE_ANTIALIAS_ON;
import static java.awt.RenderingHints.VALUE_TEXT_ANTIALIAS_ON;
import static java.util.Arrays.asList;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.map.FeatureLayer;
import org.geotools.map.MapContent;
import org.geotools.renderer.lite.StreamingRenderer;
import org.geotools.styling.FeatureTypeStyle;
import org.geotools.styling.Fill;
import org.geotools.styling.Graphic;
import org.geotools.styling.LineSymbolizer;
import org.geotools.styling.Mark;
import org.geotools.styling.PointSymbolizer;
import org.geotools.styling.PolygonSymbolizer;
import org.geotools.styling.Rule;
import org.geotools.styling.SLDParser;
import org.geotools.styling.Stroke;
import org.geotools.styling.Style;
import org.geotools.styling.StyleFactory;
import org.neo4j.gis.spatial.SpatialTopologyUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.FilterFactory;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class StyledImageExporter {
	private GraphDatabaseService db;
	private File exportDir;
	double zoom = 1.0;
	double[] offset = new double[] { 0, 0 };
	Rectangle displaySize = new Rectangle(400, 300);
	private String[] styleFiles;
	static StyleFactory styleFactory = CommonFactoryFinder.getStyleFactory(null);
    static FilterFactory filterFactory = CommonFactoryFinder.getFilterFactory(null);

	public StyledImageExporter(GraphDatabaseService db) {
		this.db = db;
	}

	public void setExportDir(String dir) {
		exportDir = (dir == null || dir.length() == 0) ? null : (new File(dir)).getAbsoluteFile();
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
		} else {
			return null;
		}
	}
	
	/**
	 * When zooming in, it is useful to also control the location of the visable
	 * window using offsets from the center, in fractions of the bounding box
	 * dimensions. Use negative values to adjust left or down.
	 * 
	 * @param fractionWidth
	 *            fraction of the width to shift right/left
	 * @param fractionHeigh
	 *            fraction of the height to shift up/down
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
			System.out.println("Deleting previous file: " + file);
			file.delete();
		}
		return file;
	}

	@SuppressWarnings({ "unchecked", "unused" })
	private void debugStore(DataStore store, String[] layerNames) throws IOException {
		for (int i = 0; i < layerNames.length; i++) {
			System.out.println(asList(store.getTypeNames()));
			System.out.println(asList(store.getSchema(layerNames[i]).getAttributeDescriptors()));
		}
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

	public void saveLayerImage(String layerName, String sldFile, File imagefile, ReferencedEnvelope bounds, int width, int height,
			double zoom) throws IOException {
		setZoom(zoom);
		setSize(width, height);
		saveLayerImage(layerName, sldFile, imagefile, bounds);
	}

	public void saveLayerImage(String layerName, String sldFile, File imagefile, ReferencedEnvelope bounds) throws IOException {
		String[] layerNames = new String[] { layerName };
		saveLayerImage(layerNames, sldFile, imagefile, bounds);
	}
	
	public void saveImage(FeatureCollection<SimpleFeatureType,SimpleFeature> features, String sldFile, File imagefile) throws IOException {
		saveImage(features, getStyleFromSLDFile(sldFile), imagefile);
	}

	public void saveImage(FeatureCollection<SimpleFeatureType,SimpleFeature> features, Style style, File imagefile) throws IOException {
		MapContent mapContent = new MapContent();
		mapContent.addLayer(new FeatureLayer(features, style));
		saveMapContentToImageFile(mapContent, imagefile, features.getBounds());
	}
	
	public void saveImage(FeatureCollection<SimpleFeatureType,SimpleFeature>[] features, Style[] styles, File imagefile, ReferencedEnvelope bounds) throws IOException {
		MapContent mapContent = new MapContent();
		for (int i = 0; i < features.length; i++) {
			mapContent.addLayer(new FeatureLayer(features[i], styles[i]));
		}
		saveMapContentToImageFile(mapContent, imagefile, bounds);
	}
	
	public void saveLayerImage(String[] layerNames, String sldFile, File imagefile, ReferencedEnvelope bounds) throws IOException {
		DataStore store = new Neo4jSpatialDataStore(db);
		// debugStore(store, layerNames);
		StringBuffer names = new StringBuffer();
		for (String name : layerNames) {
			if (names.length() > 0)
				names.append(", ");
			names.append(name);
		}
		System.out.println("Exporting layers '" + names + "' to styled image " + imagefile.getPath());

		Style style = getStyleFromSLDFile(sldFile);

		MapContent mapContent = new MapContent();
		for (int i = 0; i < layerNames.length; i++) {
			SimpleFeatureSource featureSource = store.getFeatureSource(layerNames[i]);
			Style featureStyle = style;
			if (featureStyle == null) {
				featureStyle = getStyle(i);
			}
			
			if (featureStyle == null) {
				featureStyle = createStyleFromGeometry((SimpleFeatureType) featureSource.getSchema(), Color.BLUE, Color.CYAN);
				System.out.println("Created style from geometry '" + featureSource.getSchema().getGeometryDescriptor().getType() + "': " + featureStyle);
			}
			
			mapContent.addLayer(new org.geotools.map.FeatureLayer(featureSource, featureStyle));
			
			if (bounds == null) {
				bounds = featureSource.getBounds();
			} else {
				bounds.expandToInclude(featureSource.getBounds());
			}
		}
		
		saveMapContentToImageFile(mapContent, imagefile, bounds);
	}

	private Style getStyleFromSLDFile(String sldFile) {
		Style style = null;
		if (sldFile != null) {
			style = createStyleFromSLD(sldFile);
			if (style != null)
				System.out.println("Created style from sldFile '" + sldFile + "': " + style);
		}
		return style;
	}

	private void saveMapContentToImageFile(MapContent mapContent, File imagefile, ReferencedEnvelope bounds) throws IOException {
		bounds = SpatialTopologyUtils.adjustBounds(bounds, 1.0 / zoom, offset);
		
		if (displaySize == null)
			displaySize = new Rectangle(0, 0, 800, 600);

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
    private Style createStyleFromSLD(String sldFile) {
        try {
            SLDParser stylereader = new SLDParser(styleFactory, new File(sldFile).toURI().toURL());
            Style[] style = stylereader.readXML();
            return style[0];
        } catch (Exception e) {
			System.err.println("Failed to read style from '" + sldFile + "': " + e.getMessage());
        }
        return null;
    }

    public static Style createDefaultStyle(Color strokeColor, Color fillColor) {    
    	return createStyleFromGeometry(null, strokeColor, fillColor);
    }
    
    /**
     * Here is a programmatic alternative to using JSimpleStyleDialog to
     * get a Style. This methods works out what sort of feature geometry
     * we have in the shapefile and then delegates to an appropriate style
     * creating method.
     * TODO: Consider adding support for attribute based color schemes like in
     * http://docs.geotools.org/stable/userguide/examples/stylefunctionlab.html
     */
    public static Style createStyleFromGeometry(FeatureType schema, Color strokeColor, Color fillColor) {
    	if (schema != null) {
	        Class<?> geomType = schema.getGeometryDescriptor().getType().getBinding();
	        if (Polygon.class.isAssignableFrom(geomType)
	                || MultiPolygon.class.isAssignableFrom(geomType)) {
	            return createPolygonStyle(strokeColor, fillColor);
	        } else if (LineString.class.isAssignableFrom(geomType)
	        		|| LinearRing.class.isAssignableFrom(geomType)
	                || MultiLineString.class.isAssignableFrom(geomType)) {
	            return createLineStyle(strokeColor);
	        } else if (Point.class.isAssignableFrom(geomType)
	                || MultiPoint.class.isAssignableFrom(geomType)) {
	            return createPointStyle(strokeColor, fillColor);
	        }
    	}
    	
        Style style = styleFactory.createStyle();
        style.featureTypeStyles().addAll(createPolygonStyle(strokeColor, fillColor).featureTypeStyles());
        style.featureTypeStyles().addAll(createLineStyle(strokeColor).featureTypeStyles());
        style.featureTypeStyles().addAll(createPointStyle(strokeColor, fillColor).featureTypeStyles());
        System.out.println("Created Geometry Style: "+style);
        return style;
    }
    
    /**
     * Create a Style to draw polygon features
     */
    private static Style createPolygonStyle(Color strokeColor, Color fillColor) {
        // create a partially opaque outline stroke
        Stroke stroke = styleFactory.createStroke(
                filterFactory.literal(strokeColor),
                filterFactory.literal(1),
                filterFactory.literal(0.5));

        // create a partial opaque fill
        Fill fill = styleFactory.createFill(
                filterFactory.literal(fillColor),
                filterFactory.literal(0.5));

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
			// TODO
			e.printStackTrace();
		}
        
        FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle(new Rule[]{ rule });
        Style style = styleFactory.createStyle();
        style.featureTypeStyles().add(fts);
        System.out.println("Created Polygon Style: " + style);

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
			rule.setFilter(ECQL.toFilter("geometryType(the_geom)='LineString' or geometryType(the_geom)='LinearRing' or geometryType(the_geom)='MultiLineString'"));
		} catch (CQLException e) {
			// TODO
			e.printStackTrace();
		}        
        
        FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle(new Rule[]{rule});
        Style style = styleFactory.createStyle();
        style.featureTypeStyles().add(fts);
        System.out.println("Created Line Style: "+style);

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
			// TODO
			e.printStackTrace();
		}      
                
        FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle(new Rule[]{rule});
        Style style = styleFactory.createStyle();
        style.featureTypeStyles().add(fts);
        System.out.println("Created Point Style: " + style);

        return style;
    }

    public static void main(String[] args) {
		if (args.length < 4) {
			System.err.println("Too few arguments. Provide: 'database' 'exportdir' 'stylefile' zoom layer <layers..>");
			return;
		}
		String database = args[0];
		String exportdir = args[1];
		String stylefile = args[2];
		double zoom = new Double(args[3]);
		GraphDatabaseService db = new EmbeddedGraphDatabase(database);
		try {
			StyledImageExporter imageExporter = new StyledImageExporter(db);
			imageExporter.setExportDir(exportdir);
			imageExporter.setZoom(zoom);
			imageExporter.setSize(800, 600);
			for (int i = 4; i < args.length; i++) {
				imageExporter.saveLayerImage(args[i], stylefile);
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			db.shutdown();
		}
	}
}