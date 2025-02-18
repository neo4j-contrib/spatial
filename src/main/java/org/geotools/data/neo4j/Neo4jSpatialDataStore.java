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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.Name;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.style.Style;
import org.geotools.api.style.StyleFactory;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.styling.StyleFactoryImpl;
import org.geotools.xml.styling.SLDParser;
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

	private List<Name> typeNames;
	private final Map<String, SimpleFeatureType> simpleFeatureTypeIndex = Collections.synchronizedMap(new HashMap<>());
	private final Map<String, CoordinateReferenceSystem> crsIndex = Collections.synchronizedMap(new HashMap<>());
	private final Map<String, Style> styleIndex = Collections.synchronizedMap(new HashMap<>());
	private final Map<String, ReferencedEnvelope> boundsIndex = Collections.synchronizedMap(new HashMap<>());
	private final Map<String, SimpleFeatureSource> featureSourceIndex = Collections.synchronizedMap(new HashMap<>());
	private final GraphDatabaseService database;
	private final SpatialDatabaseService spatialDatabase;

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
					Layer layer = spatialDatabase.getLayer(tx, allTypeName);
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
				Layer layer = spatialDatabase.getLayer(tx, typeName);
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
				Layer layer = spatialDatabase.getLayer(tx, typeName);
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

	public SpatialDatabaseService getSpatialDatabaseService() {
		return spatialDatabase;
	}

	public Transaction beginTx() {
		return database.beginTx();
	}

	public void clearCache() {
		typeNames = null;
		simpleFeatureTypeIndex.clear();
		crsIndex.clear();
		styleIndex.clear();
		boundsIndex.clear();
		featureSourceIndex.clear();
	}

	@Override
	protected ContentFeatureSource createFeatureSource(ContentEntry contentEntry) throws IOException {
		Layer layer;
		ArrayList<SpatialDatabaseRecord> records = new ArrayList<>();
		String[] extraPropertyNames;
		try (Transaction tx = database.beginTx()) {
			layer = spatialDatabase.getLayer(tx, contentEntry.getTypeName());
			SearchRecords results = layer.getIndex().search(tx, new SearchAll());
			// We need to pull all records during this transaction, so that later readers do not have a transaction violation
			// TODO: See if there is a more memory efficient way of doing this, perhaps create a transaction at read time in the reader?
			for (SpatialDatabaseRecord record : results) {
				records.add(record);
			}
			extraPropertyNames = layer.getExtraPropertyNames(tx);
			tx.commit();
		}
		Neo4jSpatialFeatureSource source = new Neo4jSpatialFeatureSource(contentEntry, database, layer,
				buildFeatureType(contentEntry.getTypeName()), records, extraPropertyNames);
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

	private Object getLayerStyle(String typeName) {
		try (Transaction tx = database.beginTx()) {
			Layer layer = spatialDatabase.getLayer(tx, typeName);
			tx.commit();
			if (layer == null) {
				return null;
			}
			return layer.getStyle();
		}
	}

	public Style getStyle(String typeName) {
		Style result = styleIndex.get(typeName);
		if (result == null) {
			Object obj = getLayerStyle(typeName);
			if (obj instanceof Style) {
				result = (Style) obj;
			} else if (obj instanceof File || obj instanceof String) {
				StyleFactory styleFactory = new StyleFactoryImpl();
				SLDParser parser = new SLDParser(styleFactory);
				try {
					if (obj instanceof File) {
						parser.setInput(new FileReader((File) obj));
					} else {
						parser.setInput(new StringReader(obj.toString()));
					}
					Style[] styles = parser.readXML();
					result = styles[0];
				} catch (Exception e) {
					System.err.println("Error loading style '" + obj + "': " + e.getMessage());
					e.printStackTrace(System.err);
				}
			}
			styleIndex.put(typeName, result);
		}
		return result;
	}
}
