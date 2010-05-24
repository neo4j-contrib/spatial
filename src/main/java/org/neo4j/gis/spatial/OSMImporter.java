package org.neo4j.gis.spatial;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;

import javax.xml.stream.XMLStreamException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import com.vividsolutions.jts.geom.GeometryFactory;

public class OSMImporter implements Constants {
    private GraphDatabaseService database;
    private SpatialDatabaseService spatialDatabase;
    private int commitInterval;

    public OSMImporter(GraphDatabaseService database) {   
        this(database, 1000);
    }
    
    public OSMImporter(GraphDatabaseService database, int commitInterval) {
        if (commitInterval < 1) throw new IllegalArgumentException("commitInterval must be >= 1");
        
        this.database = database;
        this.spatialDatabase = new SpatialDatabaseService(database);
        this.commitInterval = commitInterval;
    }
    

    // Public methods
    
    public void importFile(String dataset, String layerName) throws IOException, XMLStreamException {
        
        Layer layer = getOrCreateLayer(layerName);
        GeometryFactory geomFactory = layer.getGeometryFactory();
        
        long startTime = System.currentTimeMillis();
        javax.xml.stream.XMLInputFactory factory = javax.xml.stream.XMLInputFactory.newInstance();
        javax.xml.stream.XMLStreamReader parser = factory.createXMLStreamReader(new FileReader(dataset));
        Transaction tx = database.beginTx();
        int count = 0;
        try {
            ArrayList<String> currentTags = new ArrayList<String>();
            int depth = 0;
            while (true) {
                int event = parser.next();
                if (event == javax.xml.stream.XMLStreamConstants.END_DOCUMENT) {
                   break;
                }
                switch(event) {
                case javax.xml.stream.XMLStreamConstants.START_ELEMENT:
                    currentTags.add(depth, parser.getLocalName());
//                    System.out.println("Starting tag at depth "+depth+": "+currentTags.get(depth));
                    for(int i=0;i<parser.getAttributeCount();i++) {
//                        System.out.println("\t"+currentTags.get(depth)+": "+parser.getAttributeLocalName(i)+"["+parser.getAttributeNamespace(i)+","+parser.getAttributePrefix(i)+","+parser.getAttributeType(i)+","+"] = "+parser.getAttributeValue(i));
                    }
                    depth++;
                    break;
                case javax.xml.stream.XMLStreamConstants.END_ELEMENT:
                    depth--;
//                    System.out.println("Ending tag at depth "+depth+": "+currentTags.get(depth));
                    break;
                default:
                    break;
                }
//                    try {
//                        geometry = (Geometry) record.shape();
//                        fields = dbfReader.readEntry();
//                        
//                        if (geometry.isEmpty()) {
//                            log("warn | found empty geometry in record " + recordCounter);
//                        } else {
//                            // TODO check geometry.isValid() ?
//                            layer.add(geometry, fieldsName, fields);
//                        }
//                    } catch (IllegalArgumentException e) {
//                        // org.geotools.data.shapefile.shp.ShapefileReader.Record.shape() can throw this exception
//                        log("warn | found invalid geometry: index=" + recordCounter, e);                    
//                    }
                if(++count % commitInterval == 0) {
                    tx.success();
                    tx.finish();
                    tx = database.beginTx();
                }
            }            // TODO ask charset to user?
            tx.success();
        } finally {
            tx.finish();
            parser.close();
        }

        long stopTime = System.currentTimeMillis();
        log("info | Elapsed time in seconds: " + (1.0 * (stopTime - startTime) / 1000.0));
    }
    
    
    // Private methods
    
    private Layer getOrCreateLayer(String layerName) {
        Layer layer;
        Transaction tx = database.beginTx();
        try {
            if (spatialDatabase.containsLayer(layerName)) {
                layer = spatialDatabase.getLayer(layerName);                
            } else {
                layer = spatialDatabase.createLayer(layerName);
            }
            tx.success();
        } finally {
            tx.finish();
        }
        return layer;
    }
    
    private void log(String message) {
        System.out.println(message);
    }

    private void log(String message, Exception e) {
        System.out.println(message);
        e.printStackTrace();
    }

}
