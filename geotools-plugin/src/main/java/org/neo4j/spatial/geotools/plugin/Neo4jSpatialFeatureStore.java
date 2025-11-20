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
package org.neo4j.spatial.geotools.plugin;

import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.driver.Value;
import org.neo4j.spatial.geotools.common.utilities.GeotoolsAdapter;

/**
 * FeatureWriter implementation. Instances of this class are created by
 * Neo4jSpatialDataStore.
 */
public class Neo4jSpatialFeatureStore extends ContentFeatureStore {

	private final Neo4jSpatialDataStore dataStore;
	private final SimpleFeatureType featureType;

	protected Neo4jSpatialFeatureStore(
			Neo4jSpatialDataStore dataStore,
			SimpleFeatureType featureType,
			ContentEntry entry,
			Query query
	) {
		super(entry, query);
		this.dataStore = dataStore;
		this.featureType = featureType;
	}

	@Override
	protected boolean canTransact() {
		return true;
	}

	@Override
	protected ReferencedEnvelope getBoundsInternal(Query query) {
		var cql = getCQLFromQuery(query);

		Statement statement;
		if ("INCLUDE".equals(cql)) {
			statement = Cypher.call("spatial.getLayerBoundingBox")
					.withArgs(Cypher.parameter("layer", getLayerName(query)))
					.build();
		} else {
			throw new IllegalStateException("Determine BBOX with filter is not yet implemented for " + cql);
		}

		return dataStore.executeQuery(statement, transaction)
				.map(record -> {
					Value crs = record.get("crs");
					return new ReferencedEnvelope(
							record.get("minX").asDouble(),
							record.get("maxX").asDouble(),
							record.get("minY").asDouble(),
							record.get("maxY").asDouble(),
							crs.isNull() ? featureType.getCoordinateReferenceSystem()
									: GeotoolsAdapter.getCRS(crs.asString()));
				})
				.findFirst()
				.orElseThrow();

	}

	@Override
	protected int getCountInternal(Query query) {
		var cql = getCQLFromQuery(query);

		var node = Cypher.name("node");
		Statement statement;
		if ("INCLUDE".equals(cql)) {
			statement = Cypher.call("spatial.getFeatureCount")
					.withArgs(
							Cypher.parameter("layer", getLayerName(query))
					)
					.build();
		} else {
			statement = Cypher.call("spatial.cql")
					.withArgs(
							Cypher.parameter("layer", getLayerName(query)),
							Cypher.parameter("cql", CQL.toCQL(query.getFilter()))
					)
					.yield(node)
					.returning(Cypher.count(node).as("count"))
					.build();
		}

		return dataStore.executeQuery(statement, transaction)
				.mapToInt(record -> record.get("count").asInt())
				.findFirst()
				.orElseThrow();
	}

	@Override
	protected Neo4jSpatialFeatureWriter getWriterInternal(Query query, int flags) {
		return new Neo4jSpatialFeatureWriter(dataStore, this, getReaderInternal(query));
	}

	@Override
	protected Neo4jSpatialFeatureReader getReaderInternal(Query query) {
		return new Neo4jSpatialFeatureReader(dataStore, featureType, transaction, query);
	}

	@Override
	protected SimpleFeatureType buildFeatureType() {
		return featureType;
	}

	private String getLayerName(Query query) {
		String layerName = query.getTypeName();
		if (layerName == null) {
			layerName = featureType.getTypeName();
		}
		return layerName;
	}

	private static String getCQLFromQuery(Query query) {
		var filter = query.getFilter() == null ? Filter.INCLUDE : query.getFilter();
		return CQL.toCQL(filter);
	}
}
