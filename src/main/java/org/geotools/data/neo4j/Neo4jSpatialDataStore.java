/**
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
package org.geotools.data.neo4j;

import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.styling.Style;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.IOException;
import java.util.*;


/**
 * Geotools DataStore implementation.
 * 
 * @author Davide Savazzi
 */
public class Neo4jSpatialDataStore extends ContentDataStore implements Constants {

	// Constructor
	
	public Neo4jSpatialDataStore(GraphDatabaseService database) {
		super();
		
		this.database = database;
        this.spatialDatabase = new SpatialDatabaseService(database);
	}
	
	
	// Public methods
	/**
	 * Return list of not-empty Layer names.
	 * The list is cached in memory.
	 *
	 * @return layer names
	 */
	@Override
	protected List<Name> createTypeNames() throws IOException {
		if (typeNames == null) {
			typeNames = new ArrayList<>();
			try (Transaction tx = database.beginTx()) {
				String[] allTypeNames = spatialDatabase.getLayerNames();
				for (int i = 0; i < allTypeNames.length; i++) {
					// discard empty layers
					System.out.print("loading layer " + allTypeNames[i]);
					Layer layer = spatialDatabase.getLayer(allTypeNames[i]);
					if (!layer.getIndex().isEmpty()) {
						Name name = new NameImpl(layer.getName());
						typeNames.add(name);
					}
				}
				tx.success();
			}
		}
		return typeNames;
	}

	@Override
	protected Neo4jState createContentState(ContentEntry entry) {
		return new Neo4jState(entry);
	}

	@Override
	protected ContentFeatureSource createFeatureSource(ContentEntry contentEntry) throws IOException {
		String typeName = contentEntry.getTypeName();
		ContentFeatureSource result;
		if (!contentFeatureSourceIndex.containsKey(typeName)) {
			Layer layer = spatialDatabase.getLayer(typeName);
			if (layer == null) {
				throw new IOException("Layer not found: " + typeName);
			}

			try (Transaction tx = database.beginTx()) {
				result = new Neo4jFeatureSource(database, contentEntry, Query.ALL);
				contentFeatureSourceIndex.put(typeName, result);
				tx.success();
			}
		} else {
			result = contentFeatureSourceIndex.get(contentEntry.getTypeName());
		}

		return result;
	}

	@Override
	public ContentFeatureSource getFeatureSource(Name typeName, org.geotools.data.Transaction tx) throws IOException {
		ContentEntry entry = this.getNeo4jEntry(typeName);
		if (entry == null) {
			throw new SpatialDatabaseException("Cannot query database");
		}
		ContentFeatureSource featureSource = this.createFeatureSource(entry);
		featureSource.setTransaction(tx);
		return featureSource;
	}


	protected final ContentEntry getNeo4jEntry(Name name) throws IOException {
		ContentEntry entry;
		boolean found = this.entries.containsKey(name);
		boolean unqualifiedSearch = name.getNamespaceURI() == null;
		if (!found && unqualifiedSearch && this.namespaceURI != null) {
			Name defaultNsName = new NameImpl(this.namespaceURI, ((Name)name).getLocalPart());
			if (this.entries.containsKey(defaultNsName)) {
				name = defaultNsName;
				found = true;
			}
		}

		if (!found) {
			List<Name> typeNames = this.createTypeNames();
			Iterator<Name> var6 = typeNames.iterator();

			while(true) {
				Name tn;
				do {
					do {
						if (!var6.hasNext()) {
							return this.entries.get(name);
						}

						tn = var6.next();
						synchronized(this) {
							if (!this.entries.containsKey(tn)) {
								entry = new Neo4jEntry(this, getSchema(tn));
								this.entries.put(tn, entry);
							}
						}
					} while(found);
				} while(!tn.equals(name) && (!unqualifiedSearch || !tn.getLocalPart().equals(((Name)name).getLocalPart())));

				name = tn;
				found = true;
			}
		} else {
			return this.entries.get(name);
		}
	}

	/**
	 * Return FeatureType of the given Layer.
	 * FeatureTypes are cached in memory.
	 */
	public SimpleFeatureType getSchema(Name name) throws IOException {
		String typeName = name.getLocalPart();
		SimpleFeatureType result = simpleFeatureTypeIndex.get(typeName);
		if (result == null) {
			Layer layer = spatialDatabase.getLayer(typeName);
			if (layer == null) {
				throw new IOException("Layer not found: " + typeName);
			}

			try (Transaction tx = database.beginTx()) {
				result = Neo4jFeatureBuilder.getTypeFromLayer(layer);
				simpleFeatureTypeIndex.put(typeName, result);
				tx.success();
			}
		}

		return result;
	}

	public Transaction beginTx() {
		return database.beginTx();
	}
	
    public void clearCache() {
    	typeNames = null;
    	contentFeatureSourceIndex.clear();
    	crsIndex.clear();
    	styleIndex.clear();
    	boundsIndex.clear();
    	featureSourceIndex.clear();
    }	
		
	public void dispose() {
		database.shutdown();
		
		super.dispose();
	}
	
	// Attributes
	
	private List<Name> typeNames;
	private Map<String,ContentFeatureSource> contentFeatureSourceIndex = Collections.synchronizedMap(new HashMap<String,ContentFeatureSource>());
	private Map<String,SimpleFeatureType> simpleFeatureTypeIndex = Collections.synchronizedMap(new HashMap<String,SimpleFeatureType>());
	private Map<String,CoordinateReferenceSystem> crsIndex = Collections.synchronizedMap(new HashMap<String,CoordinateReferenceSystem>());
	private Map<String, Style> styleIndex = Collections.synchronizedMap(new HashMap<String,Style>());
	private Map<String,ReferencedEnvelope> boundsIndex = Collections.synchronizedMap(new HashMap<String,ReferencedEnvelope>());
	private Map<String,SimpleFeatureSource> featureSourceIndex = Collections.synchronizedMap(new HashMap<String,SimpleFeatureSource>());
	private GraphDatabaseService database;
	private SpatialDatabaseService spatialDatabase;
}