package org.neo4j.gis.spatial.geotools.data;

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
import org.geotools.data.FeatureSource;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.map.DefaultMapContext;
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
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.FilterFactory;

import com.vividsolutions.jts.geom.LineString;
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

	@SuppressWarnings("unchecked")
	private void debugStore(DataStore store, String[] layerNames) throws IOException {
		for (int i = 0; i < layerNames.length; i++) {
			System.out.println(asList(store.getTypeNames()));
			System.out.println(asList(store.getSchema(layerNames[i]).getAttributeDescriptors()));
		}
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

	public void saveLayerImage(String[] layerNames, String sldFile, File imagefile, ReferencedEnvelope bounds) throws IOException {
		imagefile = checkFile(imagefile);
		DataStore store = new Neo4jSpatialDataStore(db);
		//debugStore(store, layerNames);

		Style style = null;
		if (sldFile != null) {
			style = createStyleFromSLD(sldFile);
			if (style != null)
				System.out.println("Created style from sldFile '" + sldFile + "': " + style);
		}
		StreamingRenderer renderer = new StreamingRenderer();
		RenderingHints hints = new RenderingHints(KEY_ANTIALIASING, VALUE_ANTIALIAS_ON);
		hints.put(KEY_TEXT_ANTIALIASING, VALUE_TEXT_ANTIALIAS_ON);
		renderer.setJava2DHints(hints);

		DefaultMapContext context = new DefaultMapContext();
		renderer.setContext(context);
		for (int i = 0; i < layerNames.length; i++) {
			SimpleFeatureSource featureSource = store.getFeatureSource(layerNames[i]);
			Style featureStyle = style;
			if (featureStyle == null) {
				featureStyle = createStyleFromGeometry(featureSource);
				System.out.println("Created style from geometry '" + featureSource.getSchema().getGeometryDescriptor().getType() + "': " + featureStyle);
			}
			context.addLayer(new org.geotools.map.FeatureLayer(featureSource, featureStyle));
			if (bounds == null) {
				bounds = featureSource.getBounds();
			} else {
				bounds.expandToInclude(featureSource.getBounds());
			}
		}
		bounds = SpatialTopologyUtils.adjustBounds(bounds, 1.0 / zoom, offset);
		if (displaySize == null)
			displaySize = new Rectangle(0, 0, 800, 600);

		BufferedImage image = new BufferedImage(displaySize.width, displaySize.height, BufferedImage.TYPE_INT_ARGB);

		Graphics2D graphics = image.createGraphics();
		renderer.paint(graphics, displaySize, bounds);
		graphics.dispose();

		StringBuffer names = new StringBuffer();
		for (String name : layerNames) {
			names.append(name);
			names.append(",");
		}
		System.out.println("Exporting layers '" + names + "' to styled image " + imagefile.getPath());
		ImageIO.write(image, "png", imagefile);
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

    /**
     * Here is a programmatic alternative to using JSimpleStyleDialog to
     * get a Style. This methods works out what sort of feature geometry
     * we have in the shapefile and then delegates to an appropriate style
     * creating method.
     */
    private Style createStyleFromGeometry(FeatureSource<?,?> featureSource) {
        SimpleFeatureType schema = (SimpleFeatureType)featureSource.getSchema();
        Class<?> geomType = schema.getGeometryDescriptor().getType().getBinding();

        if (Polygon.class.isAssignableFrom(geomType)
                || MultiPolygon.class.isAssignableFrom(geomType)) {
            return createPolygonStyle();

        } else if (LineString.class.isAssignableFrom(geomType)
                || MultiLineString.class.isAssignableFrom(geomType)) {
            return createLineStyle();

        } else if (Point.class.isAssignableFrom(geomType)
                || MultiPoint.class.isAssignableFrom(geomType)) {
            return createPointStyle();

        } else {
        	Style style = styleFactory.createStyle();
        	style.featureTypeStyles().addAll(createPolygonStyle().featureTypeStyles());
        	style.featureTypeStyles().addAll(createLineStyle().featureTypeStyles());
        	style.featureTypeStyles().addAll(createPointStyle().featureTypeStyles());
            System.out.println("Created Geometry Style: "+style);
            return style;
        }
    }

    /**
     * Create a Style to draw polygon features with a thin blue outline and
     * a cyan fill
     */
    private Style createPolygonStyle() {

        // create a partially opaque outline stroke
        Stroke stroke = styleFactory.createStroke(
                filterFactory.literal(Color.BLUE),
                filterFactory.literal(1),
                filterFactory.literal(0.5));

        // create a partial opaque fill
        Fill fill = styleFactory.createFill(
                filterFactory.literal(Color.CYAN),
                filterFactory.literal(0.5));

        /*
         * Setting the geometryPropertyName arg to null signals that we want to
         * draw the default geomettry of features
         */
        PolygonSymbolizer sym = styleFactory.createPolygonSymbolizer(stroke, fill, null);

        Rule rule = styleFactory.createRule();
        rule.symbolizers().add(sym);
        FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle(new Rule[]{rule});
        Style style = styleFactory.createStyle();
        style.featureTypeStyles().add(fts);
        System.out.println("Created Polygon Style: "+style);

        return style;
    }
    
    /**
     * Create a Style to draw line features as thin blue lines
     */
    private Style createLineStyle() {
        Stroke stroke = styleFactory.createStroke(
                filterFactory.literal(Color.BLUE),
                filterFactory.literal(1));

        /*
         * Setting the geometryPropertyName arg to null signals that we want to
         * draw the default geomettry of features
         */
        LineSymbolizer sym = styleFactory.createLineSymbolizer(stroke, null);

        Rule rule = styleFactory.createRule();
        rule.symbolizers().add(sym);
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
    private Style createPointStyle() {
        Graphic gr = styleFactory.createDefaultGraphic();

        Mark mark = styleFactory.getCircleMark();

        mark.setStroke(styleFactory.createStroke(
                filterFactory.literal(Color.BLUE), filterFactory.literal(1)));

        mark.setFill(styleFactory.createFill(filterFactory.literal(Color.CYAN)));

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
        FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle(new Rule[]{rule});
        Style style = styleFactory.createStyle();
        style.featureTypeStyles().add(fts);
        System.out.println("Created Point Style: "+style);

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