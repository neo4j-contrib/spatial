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
import java.util.logging.Logger;
import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.FeatureWriter;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
 * FeatureWriter implementation. Instances of this class are created by
 * Neo4jSpatialDataStore.
 */
public class Neo4jSpatialFeatureStore extends ContentFeatureStore {

	private final GraphDatabaseService database;
	private final SimpleFeatureType featureType;
	private final Neo4jSpatialFeatureSource reader;
	private final EditableLayer layer;

	private static final Logger LOGGER = org.geotools.util.logging.Logging.getLogger("org.neo4j.gis.spatial");

	protected Neo4jSpatialFeatureStore(ContentEntry contentEntry, GraphDatabaseService database, EditableLayer layer,
			Neo4jSpatialFeatureSource reader) {
		super(contentEntry, Query.ALL);
		this.database = database;
		this.reader = reader;
		this.layer = layer;
		this.featureType = reader.buildFeatureType();
	}

	public SimpleFeatureType getFeatureType() {
		return featureType;
	}

	@Override
	protected FeatureWriter<SimpleFeatureType, SimpleFeature> getWriterInternal(Query query, int flags) {
		return new Writer(reader.getReaderInternal(query));
	}

	@Override
	protected ReferencedEnvelope getBoundsInternal(Query query) {
		return reader.getBoundsInternal(query);
	}

	@Override
	protected int getCountInternal(Query query) {
		return reader.getCountInternal(query);
	}

	@Override
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) {
		return reader.getReaderInternal(query);
	}

	@Override
	protected SimpleFeatureType buildFeatureType() {
		return featureType;
	}

	class Writer implements FeatureWriter<SimpleFeatureType, SimpleFeature> {

		private SimpleFeature live;    // current for FeatureWriter
		private SimpleFeature current; // copy of live returned to user
		private boolean closed;
		private final FeatureReader<SimpleFeatureType, SimpleFeature> reader;

		public Writer(FeatureReader<SimpleFeatureType, SimpleFeature> reader) {
			this.reader = reader;
		}

		@Override
		public SimpleFeatureType getFeatureType() {
			return reader.getFeatureType();
		}

		@Override
		public SimpleFeature next() throws IOException {
			if (closed) {
				throw new IOException("FeatureWriter has been closed");
			}

			SimpleFeatureType featureType = getFeatureType();

			if (hasNext()) {
				live = reader.next();
				current = SimpleFeatureBuilder.copy(live);
				LOGGER.finer("Calling next on writer");
			} else {
				// new content
				live = null;
				current = SimpleFeatureBuilder.template(featureType, null);
			}

			return current;
		}

		@Override
		public void remove() throws IOException {
			if (closed) {
				throw new IOException("FeatureWriter has been closed");
			}

			if (current == null) {
				throw new IOException("No feature available to remove");
			}

			if (live != null) {
				LOGGER.fine("Removing " + live);

				try (Transaction tx = database.beginTx()) {
					layer.delete(tx, live.getID());
					tx.commit();
				}

				Neo4jSpatialFeatureStore.this.getState().fireFeatureRemoved(Neo4jSpatialFeatureStore.this, live);
			}

			live = null;
			current = null;
		}

		@Override
		public void write() throws IOException {
			if (closed) {
				throw new IOException("FeatureWriter has been closed");
			}

			if (current == null) {
				throw new IOException("No feature available to write");
			}

			LOGGER.fine("Write called, live is " + live + " and cur is " + current);

			if (live != null) {
				if (!live.equals(current)) {
					LOGGER.fine("Updating " + current);
					try (Transaction tx = database.beginTx()) {
						layer.update(tx, current.getID(), (Geometry) current.getDefaultGeometry());
						tx.commit();
					}

					Neo4jSpatialFeatureStore.this.getState().fireFeatureUpdated(
							Neo4jSpatialFeatureStore.this, live,
							new ReferencedEnvelope(current.getBounds()));

				}
			} else {
				LOGGER.fine("Inserting " + current);
				try (Transaction tx = database.beginTx()) {
					layer.add(tx, (Geometry) current.getDefaultGeometry());
					tx.commit();
				}

				Neo4jSpatialFeatureStore.this.getState().fireFeatureAdded(Neo4jSpatialFeatureStore.this, current);
			}

			live = null;
			current = null;
		}

		@Override
		public boolean hasNext() throws IOException {
			if (closed) {
				throw new IOException("Feature writer is closed");
			}
			return reader != null && reader.hasNext();
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}
	}
}
