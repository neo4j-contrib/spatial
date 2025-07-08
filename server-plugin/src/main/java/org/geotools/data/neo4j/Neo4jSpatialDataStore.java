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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.Name;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Envelope;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.Utilities;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.rtree.filter.SearchAll;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * Geotools DataStore implementation.
 */
public class Neo4jSpatialDataStore extends ContentDataStore implements Constants {

	private final Map<String, SimpleFeatureType> simpleFeatureTypeIndex = Collections.synchronizedMap(new HashMap<>());
	private final Map<String, CoordinateReferenceSystem> crsIndex = Collections.synchronizedMap(new HashMap<>());
	private final Map<String, ReferencedEnvelope> boundsIndex = Collections.synchronizedMap(new HashMap<>());
	private final GraphDatabaseService database;
	private final SpatialDatabaseService spatialDatabase;
	private List<Name> typeNames;

	public Neo4jSpatialDataStore(GraphDatabaseService database) {
		this.database = database;
		this.spatialDatabase = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) database, SecurityContext.AUTH_DISABLED));
	}

	/**
	 * Return list of not-empty Layer names.
	 * The list is cached in memory.
	 *
	 * @return layer names
	 */
	@Override
	protected List<Name> createTypeNames() {
		if (typeNames == null) {
			try (Transaction tx = database.beginTx()) {
				List<Name> notEmptyTypes = new ArrayList<>();
				String[] allTypeNames = spatialDatabase.getLayerNames(tx);
				for (String allTypeName : allTypeNames) {
					// discard empty layers
					System.out.print("loading layer " + allTypeName);
					Layer layer = spatialDatabase.getLayer(tx, allTypeName, true);
					if (!layer.getIndex().isEmpty(tx)) {
						notEmptyTypes.add(new NameImpl(allTypeName));
					}
				}
				typeNames = notEmptyTypes;
				tx.commit();
			}
		}
		return typeNames;
	}

	/**
	 * Return FeatureType of the given Layer.
	 * FeatureTypes are cached in memory.
	 */
	public SimpleFeatureType buildFeatureType(String typeName) throws IOException {
		SimpleFeatureType result = simpleFeatureTypeIndex.get(typeName);
		if (result == null) {
			try (Transaction tx = database.beginTx()) {
				Layer layer = spatialDatabase.getLayer(tx, typeName, true);
				if (layer == null) {
					throw new IOException("Layer not found: " + typeName);
				}

				result = Neo4jFeatureBuilder.getTypeFromLayer(tx, layer);
				simpleFeatureTypeIndex.put(typeName, result);
				tx.commit();
			}
		}

		return result;
	}

	public ReferencedEnvelope getBounds(String typeName) {
		ReferencedEnvelope result = boundsIndex.get(typeName);
		if (result == null) {
			try (Transaction tx = database.beginTx()) {
				Layer layer = spatialDatabase.getLayer(tx, typeName, true);
				if (layer != null) {
					Envelope bbox = Utilities.fromNeo4jToJts(layer.getIndex().getBoundingBox(tx));
					result = new ReferencedEnvelope(bbox, getCRS(tx, layer));
					boundsIndex.put(typeName, result);
				}
				tx.commit();
			}
		}
		return result;
	}

	public Transaction beginTx() {
		return database.beginTx();
	}

	@Override
	protected ContentFeatureSource createFeatureSource(ContentEntry contentEntry) throws IOException {
		Layer layer;
		ArrayList<SpatialDatabaseRecord> records = new ArrayList<>();
		Map<String, Class<?>> extraProperties;
		try (Transaction tx = database.beginTx()) {
			layer = spatialDatabase.getLayer(tx, contentEntry.getTypeName(), false);
			SearchRecords results = layer.getIndex().search(tx, new SearchAll());
			// We need to pull all records during this transaction, so that later readers do not have a transaction violation
			// TODO: See if there is a more memory efficient way of doing this, perhaps create a transaction at read time in the reader?
			for (SpatialDatabaseRecord record : results) {
				records.add(record);
			}
			extraProperties = layer.getExtraProperties(tx);
			tx.commit();
		}
		Neo4jSpatialFeatureSource source = new Neo4jSpatialFeatureSource(contentEntry, database, layer,
				buildFeatureType(contentEntry.getTypeName()), records, extraProperties.keySet());
		if (layer instanceof EditableLayer) {
			return new Neo4jSpatialFeatureStore(contentEntry, database, (EditableLayer) layer, source);
		}
		return source;
	}

	private CoordinateReferenceSystem getCRS(Transaction tx, Layer layer) {
		CoordinateReferenceSystem result = crsIndex.get(layer.getName());
		if (result == null) {
			result = layer.getCoordinateReferenceSystem(tx);
			crsIndex.put(layer.getName(), result);
		}

		return result;
	}
}
