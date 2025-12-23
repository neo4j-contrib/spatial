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
package org.neo4j.spatial.osm.server.plugin.procedures;

import static org.neo4j.gis.spatial.Constants.DOC_LAYER_NAME;
import static org.neo4j.gis.spatial.Constants.DOC_URI;
import static org.neo4j.procedure.Mode.WRITE;

import java.io.File;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.neo4j.gis.spatial.EditableLayerImpl;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.procedures.SpatialProcedures.CountResult;
import org.neo4j.gis.spatial.rtree.ProgressLoggingListener;
import org.neo4j.gis.spatial.utilities.SpatialApiBase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.spatial.osm.server.plugin.OSMGeometryEncoder;
import org.neo4j.spatial.osm.server.plugin.OSMImporter;
import org.neo4j.spatial.osm.server.plugin.OSMLayer;

public class OsmSpatialProcedures extends SpatialApiBase {

	@Context
	public GraphDatabaseService db;

	@Context
	public Log log;

	@Procedure(value = "spatial.importOSMToLayer", mode = WRITE)
	@Description("Imports the the provided osm-file from URI to a layer, returns the count of data added")
	public Stream<CountResult> importOSM(
			@Name(value = "layerName", description = DOC_LAYER_NAME) String layerName,
			@Name(value = "uri", description = DOC_URI) String uri)
			throws InterruptedException {
		// Delegate finding the layer to the inner thread, so we do not pollute the procedure transaction with anything that might conflict.
		// Since the procedure transaction starts before, and ends after, all inner transactions.
		BiFunction<Transaction, String, OSMLayer> layerFinder = (tx, name) -> (OSMLayer) getEditableLayerOrThrow(tx,
				spatial(), name);
		return Stream.of(new CountResult(importOSMToLayer(uri, layerName, layerFinder)));
	}

	@Procedure(value = "spatial.importOSM", mode = WRITE)
	@Description("Imports the the provided osm-file from URI to a layer of the same name, returns the count of data added")
	public Stream<CountResult> importOSM(
			@Name(value = "uri", description = DOC_URI) String uri)
			throws InterruptedException {
		String layerName = uri.substring(uri.lastIndexOf(File.separator) + 1);
		assertLayerDoesNotExist(spatial(), layerName);
		// Delegate creating the layer to the inner thread, so we do not pollute the procedure transaction with anything that might conflict.
		// Since the procedure transaction starts before, and ends after, all inner transactions.
		BiFunction<Transaction, String, OSMLayer> layerMaker = (tx, name) -> (OSMLayer) spatial().getOrCreateLayer(tx,
				name, OSMGeometryEncoder.class, OSMLayer.class, "", false);
		return Stream.of(new CountResult(importOSMToLayer(uri, layerName, layerMaker)));
	}

	private long importOSMToLayer(String osmPath, String layerName,
			BiFunction<Transaction, String, OSMLayer> layerMaker) throws InterruptedException {
		// add extension
		if (!osmPath.toLowerCase().endsWith(".osm")) {
			osmPath = osmPath + ".osm";
		}
		OSMImportRunner runner = new OSMImportRunner(api, ktx.securityContext(), osmPath, layerName, layerMaker, log,
				Level.DEBUG);
		Thread importerThread = new Thread(runner);
		importerThread.start();
		importerThread.join();
		return runner.getResult();
	}

	private static class OSMImportRunner implements Runnable {

		private final GraphDatabaseAPI db;
		private final String osmPath;
		private final String layerName;
		private final BiFunction<Transaction, String, OSMLayer> layerMaker;
		private final Log log;
		private final Level level;
		private final SecurityContext securityContext;
		private Exception e;
		private long rc = -1;

		OSMImportRunner(GraphDatabaseAPI db, SecurityContext securityContext, String osmPath, String layerName,
				BiFunction<Transaction, String, OSMLayer> layerMaker, Log log, Level level) {
			this.db = db;
			this.osmPath = osmPath;
			this.layerName = layerName;
			this.layerMaker = layerMaker;
			this.log = log;
			this.level = level;
			this.securityContext = securityContext;
		}

		long getResult() {
			if (e == null) {
				return rc;
			}
			throw new RuntimeException(
					"Failed to import " + osmPath + " to layer '" + layerName + "': " + e.getMessage(), e);
		}

		@Override
		public void run() {
			// Create the layer in the same thread as doing the import, otherwise we have an outer thread doing a creation,
			// and the inner thread repeating it, resulting in duplicates
			try (Transaction tx = db.beginTransaction(KernelTransaction.Type.EXPLICIT, securityContext)) {
				layerMaker.apply(tx, layerName);
				tx.commit();
			}
			OSMImporter importer = new OSMImporter(layerName,
					new ProgressLoggingListener("Importing " + osmPath, log, level));
			try {
				// Provide the security context for all inner transactions that will be made during import
				importer.setSecurityContext(securityContext);
				// import using multiple, serial inner transactions (using the security context of the outer thread)
				importer.importFile(db, osmPath, false, 10000);
				// Re-index using inner transactions (using the security context of the outer thread)
				rc = importer.reIndex(db, 10000, false);
			} catch (Exception e) {
				log.error("Error running OSMImporter: " + e.getMessage());
				this.e = e;
			}
		}
	}

	// TODO this is a copy
	private void assertLayerDoesNotExist(SpatialDatabaseService sdb, String name) {
		if (sdb.getLayer(tx, name, true) != null) {
			throw new IllegalArgumentException("Layer already exists: '" + name + "'");
		}
	}

	// TODO this is a copy
	private static EditableLayerImpl getEditableLayerOrThrow(Transaction tx, SpatialDatabaseService spatial,
			String name) {
		return (EditableLayerImpl) getLayerOrThrow(tx, spatial, name, false);
	}
}
