/*
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
package org.neo4j.gis.spatial;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Date;

import org.geotools.data.PrjFileReader;
import org.geotools.data.shapefile.files.ShpFileType;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.DbaseFileReader;
import org.geotools.data.shapefile.shp.JTSUtilities;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.geotools.data.shapefile.shp.ShapefileReader.Record;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.rtree.NullListener;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;


/**
 * @author Davide Savazzi
 */
public class ShapefileImporter implements Constants {
	
	private int commitInterval;
	private boolean maintainGeometryOrder = false;

	public ShapefileImporter(GraphDatabaseService database, Listener monitor, int commitInterval, boolean maintainGeometryOrder) {	
		this.maintainGeometryOrder = maintainGeometryOrder;
        if (commitInterval < 1) {
            throw new IllegalArgumentException("commitInterval must be > 0");
        }
        this.commitInterval = commitInterval;
		this.database = database;
		this.spatialDatabase = new SpatialDatabaseService(database);
		
		if (monitor == null) monitor = new NullListener();
		this.monitor = monitor;
	}
	
	public ShapefileImporter(GraphDatabaseService database, Listener monitor, int commitInterval) {
		this(database, monitor, commitInterval, false);
	}

	public ShapefileImporter(GraphDatabaseService database, Listener monitor) {
		this(database, monitor, 1000, false);
	}

	public ShapefileImporter(GraphDatabaseService database) {	
		this(database, null, 1000, false);
	}
	
	// Main
	
	public static void main(String[] args) throws Exception {
		String neoPath;
		String shpPath;
		String layerName;
		int commitInterval = 1000;

		if (args.length < 2 || args.length > 4) {
			throw new IllegalArgumentException("Parameters: neo4jDirectory shapefile [layerName commitInterval]");
		}
		
		neoPath = args[0];
		
		shpPath = args[1];
		// remove extension
		shpPath = shpPath.substring(0, shpPath.lastIndexOf("."));
		
		if (args.length == 2) {
			layerName = shpPath.substring(shpPath.lastIndexOf(File.separator) + 1);
		} else if (args.length == 3) {
			layerName = args[2];
		} else {
			layerName = args[2];
			commitInterval = Integer.parseInt(args[3]);
		}
		
		GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabase(new File(neoPath));
		try {
	        ShapefileImporter importer = new ShapefileImporter(database, new NullListener(), commitInterval);
	        importer.importFile(shpPath, layerName);
	    } finally {
			database.shutdown();
		}
	}

	
	// Public methods

    public List<Node> importFile(String dataset, String layerName) throws IOException {
        return importFile(dataset, layerName, Charset.defaultCharset());
    }

    public List<Node> importFile(String dataset, String layerName, Charset charset) throws IOException {
        Class<? extends Layer> layerClass = maintainGeometryOrder ? OrderedEditableLayer.class : EditableLayerImpl.class;
        EditableLayerImpl layer = (EditableLayerImpl) spatialDatabase.getOrCreateLayer(layerName, WKBGeometryEncoder.class, layerClass);
        return importFile(dataset, layer, charset);
    }

    public List<Node> importFile(String dataset, EditableLayerImpl layer, Charset charset) throws IOException {
        GeometryFactory geomFactory = layer.getGeometryFactory();
		ArrayList<Node> added = new ArrayList<>();
		
		long startTime = System.currentTimeMillis();
		
		ShpFiles shpFiles;
		try {
			shpFiles = new ShpFiles(new File(dataset));
		} catch (Exception e) {
			try {
				shpFiles = new ShpFiles(new File(dataset + ".shp"));
			} catch (Exception e2) {
				throw new IllegalArgumentException("Failed to access the shapefile at either '" + dataset + "' or '" + dataset + ".shp'", e);
			}
		}
		
		ShapefileReader shpReader = new ShapefileReader(shpFiles, false, true, geomFactory);
		try {
            Class geometryClass = JTSUtilities.findBestGeometryClass(shpReader.getHeader().getShapeType());
            int geometryType = SpatialDatabaseService.convertJtsClassToGeometryType(geometryClass);
			
			// TODO ask charset to user?
			DbaseFileReader dbfReader = new DbaseFileReader(shpFiles, true, charset);
			try {
				DbaseFileHeader dbaseFileHeader = dbfReader.getHeader();
	            
				String[] fieldsName = new String[dbaseFileHeader.getNumFields()+1];
				fieldsName[0] = "ID";
				for (int i = 1; i < fieldsName.length; i++) {
					fieldsName[i] = dbaseFileHeader.getFieldName(i-1);
				}
				
				Transaction tx = database.beginTx();
				try {
                    CoordinateReferenceSystem crs = readCRS(shpFiles, shpReader);
                    if (crs != null) {
						layer.setCoordinateReferenceSystem(crs);
					}

					layer.setGeometryType(geometryType);

					layer.mergeExtraPropertyNames(fieldsName);
					tx.success();
				} finally {
					tx.close();
				}
				
				monitor.begin(dbaseFileHeader.getNumRecords());
				try {
					Record record;
					Geometry geometry;
					Object[] values;
                    ArrayList<Object> fields = new ArrayList<>();
					int recordCounter = 0;
					int filterCounter = 0;
					while (shpReader.hasNext() && dbfReader.hasNext()) {
						tx = database.beginTx();
						try {
							int committedSinceLastNotification = 0;
							for (int i = 0; i < commitInterval; i++) {
								if (shpReader.hasNext() && dbfReader.hasNext()) {
									record = shpReader.nextRecord();
									recordCounter++;
									committedSinceLastNotification++;
									try {
                                        fields.clear();
										geometry = (Geometry) record.shape();
										if (filterEnvelope == null || filterEnvelope.intersects(geometry.getEnvelopeInternal())) {
											values = dbfReader.readEntry();
                                                                                        
                                                                                        //convert Date to String 
                                                                                        //necessary because Neo4j doesn't support Date properties on nodes
                                                                                        for (int k = 0; k < fieldsName.length - 1; k++){
                                                                                            if (values[k] instanceof Date){
                                                                                                Date aux = (Date) values[k];
                                                                                                values[k] = aux.toString();
                                                                                            }
                                                                                        }
                                                                                        
											fields.add(recordCounter);
											Collections.addAll(fields, values);
											if (geometry.isEmpty()) {
												log("warn | found empty geometry in record " + recordCounter);
											} else {
												// TODO check geometry.isValid()
												// ?
												SpatialDatabaseRecord spatial_record = layer.add(geometry, fieldsName, fields.toArray(values));
												added.add(spatial_record.getGeomNode());
											}
										} else {
											filterCounter ++;
										}
									} catch (IllegalArgumentException e) {
										// org.geotools.data.shapefile.shp.ShapefileReader.Record.shape() can throw this exception
										log("warn | found invalid geometry: index=" + recordCounter, e);
									}
								}
							}
							monitor.worked(committedSinceLastNotification);
							tx.success();

							log("info | inserted geometries: " + (recordCounter-filterCounter));
							if (filterCounter > 0) {
								log("info | ignored " + filterCounter + "/" + recordCounter
										+ " geometries outside filter envelope: " + filterEnvelope);
							}

						} finally {
							tx.close();
						}
					}
				} finally {
					monitor.done();
				}
			} finally {
				dbfReader.close();
			}			
		} finally {
			shpReader.close();
		}

		long stopTime = System.currentTimeMillis();
		log("info | elapsed time in seconds: " + (1.0 * (stopTime - startTime) / 1000));
		return added;
	}
	
	
	// Private methods
	
	private CoordinateReferenceSystem readCRS(ShpFiles shpFiles, ShapefileReader shpReader) {
		try {
            PrjFileReader prjReader = new PrjFileReader(shpFiles.getReadChannel(ShpFileType.PRJ, shpReader));
            try {
            	return prjReader.getCoordinateReferenceSystem();
			} finally {
				prjReader.close();
			}
		} catch (IOException | FactoryException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private void log(String message) {
		System.out.println(message);
	}

	private void log(String message, Exception e) {
		System.out.println(message);
		e.printStackTrace();
	}
	
	
	// Attributes
	
	private Listener monitor;
	private GraphDatabaseService database;
	private SpatialDatabaseService spatialDatabase;
	private Envelope filterEnvelope;

	public void setFilterEnvelope(Envelope filterEnvelope) {
		this.filterEnvelope = filterEnvelope;
	}
}
