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

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.Utilities;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;


/**
 * FeatureReader implementation.
 * Instances of this class are created by Neo4jSpatialDataStore.
 */
public class Neo4jSpatialFeatureSource extends ContentFeatureSource {

	protected static final String FEATURE_PROP_GEOM = "the_geom";

	private final GraphDatabaseService database;
	private final Layer layer;
	private final SimpleFeatureType featureType;
	private final SimpleFeatureBuilder builder;
	private final Iterable<SpatialDatabaseRecord> results;
	private final String[] extraPropertyNames;

	public Neo4jSpatialFeatureSource(ContentEntry contentEntry, GraphDatabaseService database, Layer layer,
			SimpleFeatureType featureType, Iterable<SpatialDatabaseRecord> results, String[] extraPropertyNames) {
		super(contentEntry, Query.ALL);
		this.database = database;
		this.layer = layer;
		this.extraPropertyNames = extraPropertyNames;
		this.featureType = featureType;
		this.builder = new SimpleFeatureBuilder(featureType);
		this.results = results;
	}

	protected Layer getLayer() {
		return layer;
	}

	@Override
	protected ReferencedEnvelope getBoundsInternal(Query query) {
		try (Transaction tx = database.beginTx()) {
			Envelope envelope = this.layer.getIndex().getBoundingBox(tx);
			CoordinateReferenceSystem crs = this.layer.getCoordinateReferenceSystem(tx);
			tx.commit();
			return new ReferencedEnvelope(Utilities.fromNeo4jToJts(envelope), crs);
		}
	}

	@Override
	protected int getCountInternal(Query query) {
		try (Transaction tx = database.beginTx()) {
			int count = this.layer.getIndex().count(tx);
			tx.commit();
			return count;
		}
	}

	@Override
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) {
		return new Reader(results.iterator());
	}

	@Override
	protected SimpleFeatureType buildFeatureType() {
		return featureType;
	}

	public class Reader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

		private final Iterator<SpatialDatabaseRecord> results;

		Reader(Iterator<SpatialDatabaseRecord> results) {
			this.results = results;
		}

		@Override
		public SimpleFeatureType getFeatureType() {
			return Neo4jSpatialFeatureSource.this.buildFeatureType();
		}

		@Override
		public SimpleFeature next() throws IllegalArgumentException, NoSuchElementException {
			if (results == null) {
				return null;
			}

			try (Transaction tx = database.beginTx()) {
				SpatialDatabaseRecord record = results.next();
				if (record == null) {
					return null;
				}

				record.refreshGeomNode(tx);

				builder.reset();

				builder.set(FEATURE_PROP_GEOM, record.getGeometry());

				if (extraPropertyNames != null) {
					for (String extraPropertyName : extraPropertyNames) {
						if (record.hasProperty(tx, extraPropertyName)) {
							builder.set(extraPropertyName, record.getProperty(tx, extraPropertyName));
						}
					}
				}
				tx.commit();

				return builder.buildFeature(record.getId());
			}
		}

		@Override
		public boolean hasNext() {
			if (results == null) {
				return false;
			}
			try (Transaction tx = database.beginTx()) {
				boolean ans = results.hasNext();
				tx.commit();
				return ans;
			}
		}

		@Override
		public void close() {
		}
	}
}
