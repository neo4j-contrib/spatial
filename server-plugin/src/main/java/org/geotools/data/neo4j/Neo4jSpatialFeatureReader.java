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

import static org.geotools.data.neo4j.Neo4jFeatureBuilder.HAS_COMPLEX_ATTRIBUTES;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureReader;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Parameter;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Node;

public class Neo4jSpatialFeatureReader implements SimpleFeatureReader {

	private static final String GEOMETRY = "geom";
	private static final String ATTRIBUTES = "attributes";
	private static final String NODE = "node";


	private final Neo4jSpatialDataStore dataStore;
	private final SimpleFeatureType featureType;
	private final SimpleFeatureBuilder featureBuilder;
	private final Transaction transaction;
	private final Query query;

	protected Iterator<org.neo4j.driver.Record> result;
	protected Record currentRecord;
	protected SimpleFeature currentFeature;

	protected boolean closed = false;
	private final WKTReader wktReader = new WKTReader();
	private final boolean hasComplexAttributes;

	Neo4jSpatialFeatureReader(
			Neo4jSpatialDataStore dataStore,
			SimpleFeatureType featureType,
			Transaction transaction,
			Query query
	) {
		this.dataStore = dataStore;
		this.featureType = featureType;
		this.hasComplexAttributes = Optional.ofNullable(featureType.getUserData())
				.map(m -> m.get(HAS_COMPLEX_ATTRIBUTES))
				.filter(Boolean.class::isInstance)
				.map(Boolean.class::cast)
				.orElse(false);
		this.featureBuilder = new SimpleFeatureBuilder(featureType);
		this.transaction = transaction;
		this.query = query;

		initialize();
	}

	protected void initialize() {
		try {
			String layer = query.getTypeName();
			if (layer == null) {
				layer = featureType.getTypeName();
			}
			var node = Cypher.anyNode("node").as(NODE);

			Parameter<String> layerParam = Cypher.parameter("layer", layer);
			Statement statement = Cypher.call("spatial.cql")
					.withArgs(
							layerParam,
							Cypher.parameter("cql", ECQL.toCQL(query.getFilter()))
					)
					.yield(node)
					.returning(
							node,
							hasComplexAttributes ? Cypher
									.call("spatial.extractAttributes")
									.withArgs(layerParam, node)
									.asFunction()
									.as(ATTRIBUTES)
									: Cypher.mapOf().as(ATTRIBUTES),
							Cypher
									.call("spatial.nodeAsWKT")
									.withArgs(layerParam, node)
									.asFunction()
									.as(GEOMETRY)
					)
					.build();
			this.result = dataStore.executeQuery(statement, transaction).iterator();

		} catch (RuntimeException e) {
			close();
			throw new IllegalStateException("Error initializing feature reader", e);
		}
	}

	@Override
	public SimpleFeatureType getFeatureType() {
		return featureType;
	}

	@Override
	public SimpleFeature next() throws IllegalArgumentException, NoSuchElementException, IOException {
		if (closed) {
			throw new IllegalStateException("FeatureReader is closed");
		}

		if (!hasNext()) {
			throw new NoSuchElementException("No more features available");
		}

		try {
			currentRecord = result.next();
			currentFeature = buildFeature(currentRecord);
			return currentFeature;

		} catch (Exception e) {
			throw new IOException("Error reading next feature", e);
		}
	}

	@Override
	public boolean hasNext() throws IOException {
		if (closed) {
			return false;
		}

		try {
			return result.hasNext();
		} catch (Exception e) {
			throw new IOException("Error checking for next record", e);
		}
	}

	@Override
	public void close() {
		if (!closed) {
			result = null;
			closed = true;
		}
	}

	protected SimpleFeature buildFeature(Record record) throws IOException {
		try {
			featureBuilder.reset();

			// Node aus dem Record extrahieren
			Node node = record.get(NODE).asNode();
			Map<String, Object> attributes = record.get(ATTRIBUTES).asMap();

			// Feature ID generieren
			String fid = generateFeatureId(node);

			// Attribute setzen
			for (AttributeDescriptor descriptor : featureType.getAttributeDescriptors()) {
				if (descriptor.getLocalName().equals(featureType.getGeometryDescriptor().getLocalName())) {
					featureBuilder.set(descriptor.getName(), wktReader.read(record.get(GEOMETRY).asString()));
					continue;
				}
				String attributeName = descriptor.getLocalName();
				Class<?> binding = descriptor.getType().getBinding();

				Object value = null;
				try {
					if (hasComplexAttributes) {
						value = extractAttributeValue(binding, Values.value(attributes.get(attributeName)));
					} else if (node.containsKey(attributeName)) {
						value = extractAttributeValue(binding, node.get(attributeName));
					}
				} catch (Exception e) {
					throw new IOException("Error extracting attribute '" + attributeName + "'", e);
				}
				featureBuilder.set(attributeName, value);
			}

			return featureBuilder.buildFeature(fid);

		} catch (Exception e) {
			throw new IOException("Error building feature from Neo4j record", e);
		}
	}

	protected String generateFeatureId(Node node) {
		// Verwende Neo4j Node ID oder property-basierte ID
		if (node.containsKey("id")) {
			return String.valueOf(node.get("id").asString());
		} else if (node.containsKey("fid")) {
			return node.get("fid").asString();
		} else {
			return node.elementId();
		}
	}


	private Object extractAttributeValue(Class<?> binding, Value value) throws IOException {
		if (value.isNull()) {
			return null;
		}

		// Typ-spezifische Konvertierung
		if (Geometry.class.isAssignableFrom(binding)) {
			return convertToGeometry(value, binding);
		} else if (String.class.equals(binding)) {
			return value.asString();
		} else if (Integer.class.equals(binding)) {
			return value.asInt();
		} else if (Long.class.equals(binding)) {
			return value.asLong();
		} else if (Double.class.equals(binding)) {
			return value.asDouble();
		} else if (Boolean.class.equals(binding)) {
			return value.asBoolean();
		} else if (java.util.Date.class.equals(binding)) {
			return convertToDate(value);
		} else if (double[].class.equals(binding)) {
			return value.asList().stream()
					.map(Double.class::cast)
					.mapToDouble(Double::doubleValue)
					.toArray();
		} else {
			// Fallback: als String zurückgeben
			return value.asString();
		}
	}

	protected Geometry convertToGeometry(Value value, Class<?> binding) throws IOException {
		try {
			if (value.hasType(InternalTypeSystem.TYPE_SYSTEM.POINT())) {
				// Neo4j Point zu JTS Geometry
				org.neo4j.driver.types.Point neoPoint = value.asPoint();
				return convertNeo4jPointToJTS(neoPoint);
			} else if (value.hasType(InternalTypeSystem.TYPE_SYSTEM.STRING())) {
				// WKT String zu Geometry
				String wkt = value.asString();
				return wktReader.read(wkt);
			} else {
				throw new IOException("Unsupported geometry type: " + value.type());
			}
		} catch (Exception e) {
			throw new IOException("Error converting geometry", e);
		}
	}

	protected org.locationtech.jts.geom.Point convertNeo4jPointToJTS(org.neo4j.driver.types.Point neoPoint) {
		GeometryFactory factory = new GeometryFactory();
		Coordinate coord = new Coordinate(neoPoint.x(), neoPoint.y());
		if (!Double.isNaN(neoPoint.z())) {
			coord = new Coordinate(neoPoint.x(), neoPoint.y(), neoPoint.z());
		}
		return factory.createPoint(coord);
	}

	protected java.util.Date convertToDate(Value value) {
		if (value.hasType(InternalTypeSystem.TYPE_SYSTEM.DATE_TIME())) {
			return new java.util.Date(value.asZonedDateTime().toInstant().toEpochMilli());
		} else if (value.hasType(InternalTypeSystem.TYPE_SYSTEM.DATE())) {
			return java.sql.Date.valueOf(value.asLocalDate());
		} else {
			// Fallback: String als ISO Date parsen
			return java.sql.Timestamp.valueOf(value.asString());
		}
	}

}
