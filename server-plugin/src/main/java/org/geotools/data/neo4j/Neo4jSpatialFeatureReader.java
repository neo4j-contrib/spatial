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
import java.util.Set;
import org.geotools.api.data.SimpleFeatureReader;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

public class Neo4jSpatialFeatureReader implements SimpleFeatureReader {

	private final GraphDatabaseService database;
	private final SimpleFeatureType featureType;
	private final SimpleFeatureBuilder builder;
	private final Set<String> extraPropertyNames;
	private final Iterator<SpatialDatabaseRecord> results;

	Neo4jSpatialFeatureReader(
			GraphDatabaseService database,
			SimpleFeatureType featureType,
			Set<String> extraPropertyNames,
			Iterator<SpatialDatabaseRecord> results
	) {
		this.database = database;
		this.featureType = featureType;
		this.extraPropertyNames = extraPropertyNames;
		this.results = results;
		this.builder = new SimpleFeatureBuilder(featureType);
	}

	@Override
	public SimpleFeatureType getFeatureType() {
		return featureType;
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

			builder.set(Neo4jSpatialFeatureSource.FEATURE_PROP_GEOM, record.getGeometry());

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
