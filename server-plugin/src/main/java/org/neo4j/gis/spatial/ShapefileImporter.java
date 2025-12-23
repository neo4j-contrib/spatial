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
package org.neo4j.gis.spatial;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.data.PrjFileReader;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.DbaseFileReader;
import org.geotools.data.shapefile.files.ShpFileType;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.JTSUtilities;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.geotools.data.shapefile.shp.ShapefileReader.Record;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.gis.spatial.encoders.WKBGeometryEncoder;
import org.neo4j.gis.spatial.index.IndexManagerImpl;
import org.neo4j.gis.spatial.rtree.NullListener;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.api.monitoring.ProgressListener;

public class ShapefileImporter {

	private static final Logger LOGGER = Logger.getLogger(ShapefileImporter.class.getName());
	private final int commitInterval;
	private final boolean maintainGeometryOrder;
	private final ProgressListener monitor;
	private final GraphDatabaseService database;
	private final SpatialDatabaseService spatialDatabase;
	private Envelope filterEnvelope;

	public ShapefileImporter(GraphDatabaseService database, ProgressListener monitor, int commitInterval,
			boolean maintainGeometryOrder) {
		this.maintainGeometryOrder = maintainGeometryOrder;
		if (commitInterval < 1) {
			throw new IllegalArgumentException("commitInterval must be > 0");
		}
		this.commitInterval = commitInterval;
		this.database = database;
		this.spatialDatabase = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) database, SecurityContext.AUTH_DISABLED));

		if (monitor == null) {
			monitor = new NullListener();
		}
		this.monitor = monitor;
	}

	public ShapefileImporter(GraphDatabaseService database, ProgressListener monitor, int commitInterval) {
		this(database, monitor, commitInterval, false);
	}

	public ShapefileImporter(GraphDatabaseService database, ProgressListener monitor) {
		this(database, monitor, 1000, false);
	}

	public ShapefileImporter(GraphDatabaseService database) {
		this(database, null, 1000, false);
	}

	public void setFilterEnvelope(Envelope filterEnvelope) {
		this.filterEnvelope = filterEnvelope;
	}

	public List<Node> importFile(String dataset, String layerName) throws IOException {
		return importFile(dataset, layerName, Charset.defaultCharset());
	}

	public List<Node> importFile(String dataset, String layerName, Charset charset) throws IOException {
		Class<? extends Layer> layerClass =
				maintainGeometryOrder ? OrderedEditableLayer.class : EditableLayerImpl.class;
		EditableLayerImpl layer;
		try (Transaction tx = database.beginTx()) {
			layer = (EditableLayerImpl) spatialDatabase.getOrCreateLayer(tx, layerName, WKBGeometryEncoder.class,
					layerClass, null, false);
			tx.commit();
		}
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
				throw new IllegalArgumentException(
						"Failed to access the shapefile at either '" + dataset + "' or '" + dataset + ".shp'", e);
			}
		}

		try (ShapefileReader shpReader = new ShapefileReader(shpFiles, false, true, geomFactory)) {
			Class<? extends Geometry> geometryClass = JTSUtilities.findBestGeometryClass(
					shpReader.getHeader().getShapeType());
			int geometryType = SpatialDatabaseService.convertJtsClassToGeometryType(geometryClass);

			// TODO ask charset to user?
			try (DbaseFileReader dbfReader = new DbaseFileReader(shpFiles, true, charset)) {
				DbaseFileHeader dbaseFileHeader = dbfReader.getHeader();

				String[] fieldsName = new String[dbaseFileHeader.getNumFields() + 1];
				fieldsName[0] = "ID";
				for (int i = 1; i < fieldsName.length; i++) {
					fieldsName[i] = dbaseFileHeader.getFieldName(i - 1);
				}

				try (var tx = database.beginTx()) {
					CoordinateReferenceSystem crs = readCRS(shpFiles, shpReader);
					if (crs != null) {
						layer.setCoordinateReferenceSystem(tx, crs);
					}

					layer.setGeometryType(tx, geometryType);
					tx.commit();
				}

				monitor.begin(dbaseFileHeader.getNumRecords());
				try {
					Record record;
					Geometry geometry;
					int recordCounter = 0;
					int filterCounter = 0;
					while (shpReader.hasNext() && dbfReader.hasNext()) {
						try (var tx = database.beginTx()) {
							int committedSinceLastNotification = 0;
							for (int i = 0; i < commitInterval; i++) {
								if (shpReader.hasNext() && dbfReader.hasNext()) {
									record = shpReader.nextRecord();
									recordCounter++;
									committedSinceLastNotification++;
									try {
										geometry = (Geometry) record.shape();
										if (filterEnvelope == null || filterEnvelope.intersects(
												geometry.getEnvelopeInternal())) {
											Object[] values = dbfReader.readEntry();

											var properties = new HashMap<String, Object>();
											for (int k = 0; k < fieldsName.length; k++) {
												String field = fieldsName[k];
												Object value = k == 0 ? recordCounter : values[k - 1];
												if (value instanceof Date aux) {
													//convert Date to String
													//necessary because Neo4j doesn't support Date properties on nodes
													value = aux.toString();
												}
												properties.put(field, value);
											}

											if (geometry.isEmpty()) {
												LOGGER.warning("found empty geometry in record " + recordCounter);
											} else {
												// TODO check geometry.isValid()
												// ?
												SpatialDatabaseRecord spatial_record = layer.add(tx, geometry,
														properties);
												added.add(spatial_record.getGeomNode());
											}
										} else {
											filterCounter++;
										}
									} catch (IllegalArgumentException e) {
										// org.geotools.data.shapefile.shp.ShapefileReader.Record.shape() can throw this exception
										LOGGER.log(java.util.logging.Level.WARNING,
												"found invalid geometry: index=" + recordCounter, e);
									}
								}
							}
							monitor.worked(committedSinceLastNotification);
							tx.commit();

							LOGGER.info("inserted geometries: " + (recordCounter - filterCounter));
							if (filterCounter > 0) {
								LOGGER.info("ignored " + filterCounter + "/" + recordCounter
										+ " geometries outside filter envelope: " + filterEnvelope);
							}
						}
					}
				} finally {
					monitor.done();
				}
			}
		}
		try (Transaction tx = database.beginTx()) {
			layer.finalizeTransaction(tx);
			tx.commit();
		}

		long stopTime = System.currentTimeMillis();
		LOGGER.info("elapsed time in seconds: " + 1.0 * (stopTime - startTime) / 1000);
		return added;
	}

	private static CoordinateReferenceSystem readCRS(ShpFiles shpFiles, ShapefileReader shpReader) {
		try (PrjFileReader prjReader = new PrjFileReader(shpFiles.getReadChannel(ShpFileType.PRJ, shpReader))) {
			return prjReader.getCoordinateReferenceSystem();
		} catch (IOException | FactoryException e) {
			LOGGER.log(Level.WARNING, "", e);
			return null;
		}
	}

	public static void main(String[] args) throws Exception {
		String neoPath;
		String database;
		String shpPath;
		String layerName;
		int commitInterval = 1000;

		if (args.length < 3 || args.length > 5) {
			LOGGER.warning("Parameters: neo4jDirectory database shapefile [layerName commitInterval]");
			LOGGER.warning(
					"\tNote: 'database' can only be something other than 'neo4j' in Neo4j Enterprise Edition.");
			System.exit(1);
		}

		neoPath = args[0];
		database = args[1];
		shpPath = args[2];
		shpPath = shpPath.substring(0, shpPath.lastIndexOf("."));

		if (args.length == 3) {
			layerName = shpPath.substring(shpPath.lastIndexOf(File.separator) + 1);
		} else if (args.length == 4) {
			layerName = args[3];
		} else {
			layerName = args[3];
			commitInterval = Integer.parseInt(args[4]);
		}

		DatabaseManagementService databases = new DatabaseManagementServiceBuilder(Path.of(neoPath)).build();
		GraphDatabaseService db = databases.database(database);
		try {
			ShapefileImporter importer = new ShapefileImporter(db, new NullListener(), commitInterval);
			importer.importFile(shpPath, layerName);
		} finally {
			databases.shutdown();
		}
	}
}
