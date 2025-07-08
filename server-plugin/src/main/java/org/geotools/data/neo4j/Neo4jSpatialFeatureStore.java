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

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.FeatureWriter;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * FeatureWriter implementation. Instances of this class are created by
 * Neo4jSpatialDataStore.
 */
public class Neo4jSpatialFeatureStore extends ContentFeatureStore {

	private final GraphDatabaseService database;
	private final SimpleFeatureType featureType;
	private final Neo4jSpatialFeatureSource featureSource;
	private final EditableLayer layer;

	protected Neo4jSpatialFeatureStore(ContentEntry contentEntry, GraphDatabaseService database, EditableLayer layer,
			Neo4jSpatialFeatureSource featureSource) {
		super(contentEntry, Query.ALL);
		this.database = database;
		this.featureSource = featureSource;
		this.layer = layer;
		this.featureType = featureSource.buildFeatureType();
	}

	@Override
	protected FeatureWriter<SimpleFeatureType, SimpleFeature> getWriterInternal(Query query, int flags) {
		return new Neo4jSpatialFeatureWriter(
				this,
				database,
				layer,
				featureSource.getReaderInternal(query)
		);
	}

	@Override
	protected ReferencedEnvelope getBoundsInternal(Query query) {
		return featureSource.getBoundsInternal(query);
	}

	@Override
	protected int getCountInternal(Query query) {
		return featureSource.getCountInternal(query);
	}

	@Override
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) {
		return featureSource.getReaderInternal(query);
	}

	@Override
	protected SimpleFeatureType buildFeatureType() {
		return featureType;
	}

}
