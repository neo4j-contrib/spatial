package org.neo4j.gis.spatial.geotools.data;

import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;

import javax.imageio.ImageIO;

import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.map.DefaultMapContext;
import org.geotools.renderer.lite.StreamingRenderer;
import org.geotools.styling.SLDParser;
import org.geotools.styling.Style;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import static java.awt.RenderingHints.*;
import static java.util.Arrays.asList;
import static org.geotools.factory.CommonFactoryFinder.getStyleFactory;

public class StyledImageExporter {
	private GraphDatabaseService db;
	private File exportDir;
	double zoom = 1.0;
	Rectangle displaySize = new Rectangle(400,300);
	
	public StyledImageExporter(GraphDatabaseService db) {
		this.db = db;
	}

	public void setExportDir(String dir) {
		exportDir = (dir == null || dir.length() == 0) ? null : (new File(dir)).getAbsoluteFile();
	}

	public void setZoom(double zoom){
		this.zoom = zoom;
	}

	public void setSize(int width, int height){
		this.displaySize = new Rectangle(width,height);
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
    private void debugStore(DataStore store, String layerName) throws IOException {
        System.out.println(asList(store.getTypeNames()));
        System.out.println(asList(store.getSchema(layerName).getAttributeDescriptors()));
	}

	public void saveLayerImage(String layerName, String sldFile) throws IOException {
		saveLayerImage(layerName, sldFile, new File(layerName + ".png"), null);
	}

	public void saveLayerImage(String layerName, String sldFile, String imageFile)
	        throws IOException {
		saveLayerImage(layerName, sldFile, new File(imageFile), null);
	}

	public void saveLayerImage(String layerName, String sldFile, File imagefile, ReferencedEnvelope bounds, int width, int height,
	        double zoom) throws IOException {
		setZoom(zoom);
		setSize(width, height);
		saveLayerImage(layerName, sldFile, imagefile, bounds);
	}

	public void saveLayerImage(String layerName, String sldFile, File imagefile, ReferencedEnvelope bounds) throws IOException {
		imagefile = checkFile(imagefile);
        DataStore store = new Neo4jSpatialDataStore(db);
        debugStore(store,layerName);

        SimpleFeatureSource featureSource = store.getFeatureSource(layerName);

        URL styleURL = new File(sldFile).toURI().toURL();
        Style style = new SLDParser(getStyleFactory(null), styleURL).readXML()[0];
        StreamingRenderer renderer = new StreamingRenderer();
        RenderingHints hints = new RenderingHints(KEY_ANTIALIASING, VALUE_ANTIALIAS_ON);
        hints.put(KEY_TEXT_ANTIALIASING, VALUE_TEXT_ANTIALIAS_ON);
        renderer.setJava2DHints(hints);

        DefaultMapContext context = new DefaultMapContext();
        renderer.setContext(context);
        context.addLayer(new org.geotools.map.FeatureLayer(featureSource, style));

		if (bounds == null)
			bounds = featureSource.getBounds();
		bounds = scaleBounds(bounds, 1.0 / zoom);
		if (displaySize == null)
			displaySize = new Rectangle(0, 0, 800, 600);

		BufferedImage image = new BufferedImage(displaySize.width, displaySize.height, BufferedImage.TYPE_INT_ARGB);

        Graphics2D graphics = image.createGraphics();
        renderer.paint(graphics, displaySize, bounds);
        graphics.dispose();

		System.out.println("Exporting layer '" + layerName + "' to styled image " + imagefile.getPath());
        ImageIO.write(image, "png", imagefile);
	}

	private ReferencedEnvelope scaleBounds(ReferencedEnvelope bounds, double factor) {
		ReferencedEnvelope scaled = new ReferencedEnvelope(bounds);
		if (Math.abs(factor - 1.0) > 0.01) {
			double[] min = scaled.getLowerCorner().getCoordinate();
			double[] max = scaled.getUpperCorner().getCoordinate();
			for(int i=0;i<scaled.getDimension();i++){
				double span = scaled.getSpan(i);
				double delta = (span - span * factor) / 2.0;
				min[i] += delta;
				max[i] -= delta;
			}
			scaled = new ReferencedEnvelope(min[0],max[0],min[1],max[1],scaled.getCoordinateReferenceSystem());
		}
		return scaled;
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