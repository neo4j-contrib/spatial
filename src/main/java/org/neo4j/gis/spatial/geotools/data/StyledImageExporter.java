package org.neo4j.gis.spatial.geotools.data;

import static java.awt.RenderingHints.KEY_ANTIALIASING;
import static java.awt.RenderingHints.KEY_TEXT_ANTIALIASING;
import static java.awt.RenderingHints.VALUE_ANTIALIAS_ON;
import static java.awt.RenderingHints.VALUE_TEXT_ANTIALIAS_ON;
import static java.util.Arrays.asList;
import static org.geotools.factory.CommonFactoryFinder.getStyleFactory;

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
import org.neo4j.gis.spatial.SpatialTopologyUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class StyledImageExporter {
	private GraphDatabaseService db;
	private File exportDir;
	double zoom = 1.0;
	double[] offset = new double[]{0,0};
	Rectangle displaySize = new Rectangle(400, 300);

	public StyledImageExporter(GraphDatabaseService db) {
		this.db = db;
	}

	public void setExportDir(String dir) {
		exportDir = (dir == null || dir.length() == 0) ? null : (new File(dir))
				.getAbsoluteFile();
	}

	public void setZoom(double zoom) {
		this.zoom = zoom;
	}

	public void setSize(int width, int height) {
		this.displaySize = new Rectangle(width, height);
	}

	public void setOffsetFractions(double boundaryFractionX, double boundaryFractionY) {
		this.offset[0] = boundaryFractionX;
		this.offset[1] = boundaryFractionY;
		
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
	private void debugStore(DataStore store, String[] layerNames)
			throws IOException {
		for (int i = 0; i < layerNames.length; i++) {
			System.out.println(asList(store.getTypeNames()));
			System.out.println(asList(store.getSchema(layerNames[i])
					.getAttributeDescriptors()));
		}
	}

	public void saveLayerImage(String layerName, String sldFile)
			throws IOException {
		saveLayerImage(layerName, sldFile, new File(layerName + ".png"), null);
	}

	public void saveLayerImage(String layerName, String sldFile,
			String imageFile) throws IOException {
		saveLayerImage(layerName, sldFile, new File(imageFile), null);
	}

	public void saveLayerImage(String layerName, String sldFile,
			File imagefile, ReferencedEnvelope bounds, int width, int height,
			double zoom) throws IOException {
		setZoom(zoom);
		setSize(width, height);
		saveLayerImage(layerName, sldFile, imagefile, bounds);
	}

	public void saveLayerImage(String layerName, String sldFile,
			File imagefile, ReferencedEnvelope bounds) throws IOException {
		String[] layerNames = new String[] { layerName };
		saveLayerImage(layerNames, sldFile, imagefile, bounds);
	}

	public void saveLayerImage(String[] layerNames, String sldFile,
			File imagefile, ReferencedEnvelope bounds) throws IOException {
		imagefile = checkFile(imagefile);
		DataStore store = new Neo4jSpatialDataStore(db);
		debugStore(store, layerNames);

		URL styleURL = new File(sldFile).toURI().toURL();
		Style style = new SLDParser(getStyleFactory(null), styleURL).readXML()[0];
		StreamingRenderer renderer = new StreamingRenderer();
		RenderingHints hints = new RenderingHints(KEY_ANTIALIASING,
				VALUE_ANTIALIAS_ON);
		hints.put(KEY_TEXT_ANTIALIASING, VALUE_TEXT_ANTIALIAS_ON);
		renderer.setJava2DHints(hints);

		DefaultMapContext context = new DefaultMapContext();
		renderer.setContext(context);
		for (int i = 0; i < layerNames.length; i++) {
			SimpleFeatureSource featureSource = store
					.getFeatureSource(layerNames[i]);
			context.addLayer(new org.geotools.map.FeatureLayer(featureSource,
					style));
			if (bounds == null) {
				bounds = featureSource.getBounds();
			} else {
				bounds.expandToInclude(featureSource.getBounds());
			}
		}
		bounds = SpatialTopologyUtils.scaleBounds(bounds, 1.0 / zoom);
		if (displaySize == null)
			displaySize = new Rectangle(0, 0, 800, 600);

		BufferedImage image = new BufferedImage(displaySize.width,
				displaySize.height, BufferedImage.TYPE_INT_ARGB);

		Graphics2D graphics = image.createGraphics();
		renderer.paint(graphics, displaySize, bounds);
		graphics.dispose();

		StringBuffer names = new StringBuffer();
		for(String name : layerNames) {
			names.append(name);
			names.append(",");
		}
		System.out.println("Exporting layers '" + names 
				+ "' to styled image " + imagefile.getPath());
		ImageIO.write(image, "png", imagefile);
	}

	public static void main(String[] args) {
		if (args.length < 4) {
			System.err
					.println("Too few arguments. Provide: 'database' 'exportdir' 'stylefile' zoom layer <layers..>");
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