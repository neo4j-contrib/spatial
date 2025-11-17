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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.Name;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.feature.NameImpl;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.cypherdsl.core.renderer.Configuration;
import org.neo4j.cypherdsl.core.renderer.Dialect;
import org.neo4j.cypherdsl.core.renderer.Renderer;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.utilities.GeotoolsAdapter;

/**
 * Geotools DataStore implementation.
 */
public class Neo4jSpatialDataStore extends ContentDataStore implements Constants {

	private static final Renderer cypherRenderer = Renderer.getRenderer(
			Configuration.newConfig()
					.withDialect(Dialect.NEO4J_5_DEFAULT_CYPHER)
					.build()
	);

	private final Driver driver;
	private final SessionConfig sessionConfig;

	private final Map<String, SimpleFeatureType> simpleFeatureTypeCache = Collections.synchronizedMap(new HashMap<>());

	public Neo4jSpatialDataStore(Driver driver, String database) {
		this.driver = driver;
		this.sessionConfig = SessionConfig.forDatabase(database);
	}

	public Session getSession(org.geotools.api.data.Transaction t) {
		if (t == org.geotools.api.data.Transaction.AUTO_COMMIT) {
			return driver.session(sessionConfig);
		} else {
			Neo4jTransactionState state = (Neo4jTransactionState) t.getState(this);
			if (state == null) {
				Session session = driver.session(sessionConfig);
				state = new Neo4jTransactionState(session);
				t.putState(this, state);
				state.setTransaction(t);
			}
			return state.getSession();
		}
	}

	public Stream<org.neo4j.driver.Record> executeQuery(Statement statement, org.geotools.api.data.Transaction tx) {
		return executeQuery(cypherRenderer.render(statement), statement.getCatalog().getParameters(), tx);
	}

	public Stream<Record> executeQuery(String cypher, Map<String, Object> parameters,
			org.geotools.api.data.Transaction tx) {
		Session session = getSession(tx);

		if (tx == org.geotools.api.data.Transaction.AUTO_COMMIT) {
			return session.run(cypher, parameters)
					.stream()
					.onClose(session::close);
		} else {
			Neo4jTransactionState state = (Neo4jTransactionState) tx.getState(this);
			org.neo4j.driver.Transaction neo4jTx = state.getNeo4jTransaction();
			return neo4jTx.run(cypher, parameters).stream();
		}
	}

	/**
	 * Return list of not-empty Layer names.
	 * The list is cached in memory.
	 *
	 * @return layer names
	 */
	@Override
	protected List<Name> createTypeNames() {
		Statement statement = Cypher.call("spatial.layers").build();
		var result = executeQuery(statement, org.geotools.api.data.Transaction.AUTO_COMMIT);
		return result.<Name>map(record -> {
					String layerName = record.get("name").asString();
					return new NameImpl(layerName);
				})
				.toList();
	}

	/**
	 * Return FeatureType of the given Layer.
	 * FeatureTypes are cached in memory.
	 */
	public SimpleFeatureType buildFeatureType(String layerName) throws IOException {
		return simpleFeatureTypeCache.computeIfAbsent(layerName, s -> {

			Statement statement = Cypher.call("spatial.layerMeta")
					.withArgs(Cypher.parameter("layerName", layerName))
					.build();

			var result = executeQuery(statement, org.geotools.api.data.Transaction.AUTO_COMMIT).findFirst()
					.orElseThrow();

			String geometryType = result.get("geometryType").asString();
			Class<? extends Geometry> geometryTypeClass;
			try {
				//noinspection unchecked
				geometryTypeClass = (Class<? extends Geometry>) Class.forName(geometryType);
			} catch (ClassNotFoundException e) {
				throw new IllegalStateException("Unable to load geometry type " + geometryType, e);
			}

			boolean hasComplexAttributes = result.get("hasComplexAttributes").asBoolean();
			Value crsString = result.get("crs");
			var crs = crsString.isNull() ? null : GeotoolsAdapter.getCRS(crsString.asString());
			var extraProperties = result.get("extraAttributes").asMap(value -> {
				String className = value.asString();
				if (className.equals("org.neo4j.values.storable.PointValue")) {
					return Point.class;
				}
				if (className.equals("[org.neo4j.values.storable.PointValue;")) {
					return LineString.class;
				}
				try {
					return Class.forName(className);
				} catch (ClassNotFoundException e) {
					getLogger().warning("Class not found: " + className + ", falling back to java.lang.String");
					return String.class;
				}
			});
			return Neo4jFeatureBuilder.getType(layerName, geometryTypeClass, crs, hasComplexAttributes,
					extraProperties);
		});
	}

	@Override
	protected Neo4jSpatialFeatureStore createFeatureSource(ContentEntry contentEntry) throws IOException {
		var featureType = buildFeatureType(contentEntry.getTypeName());
		return new Neo4jSpatialFeatureStore(this, featureType, contentEntry, Query.ALL);
	}
}
