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
import java.util.HashMap;
import java.util.logging.Logger;
import org.geotools.api.data.FeatureWriter;
import org.geotools.api.feature.GeometryAttribute;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.ExposesReturning;
import org.neo4j.cypherdsl.core.ExposesWith;
import org.neo4j.cypherdsl.core.Parameter;

public class Neo4jSpatialFeatureWriter implements FeatureWriter<SimpleFeatureType, SimpleFeature> {

	private static final Logger LOGGER = org.geotools.util.logging.Logging.getLogger(Neo4jSpatialFeatureWriter.class);

	private final Neo4jSpatialDataStore dataStore;
	private final Neo4jSpatialFeatureStore featureStore;
	private final Neo4jSpatialFeatureReader reader;

	private SimpleFeature live; // copy of live returned to user
	private SimpleFeature current;

	private boolean closed;

	public Neo4jSpatialFeatureWriter(
			Neo4jSpatialDataStore dataStore,
			Neo4jSpatialFeatureStore featureStore,
			Neo4jSpatialFeatureReader reader
	) {
		this.dataStore = dataStore;
		this.featureStore = featureStore;
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

			Parameter<String> layerName = Cypher.parameter("layerName", current.getType().getTypeName());
			var exitingNode = Cypher.anyNode("exitingNode");
			dataStore.executeQuery(Cypher
							.match(exitingNode)
							.where(Cypher.elementId(exitingNode).isEqualTo(Cypher.parameter("nodeToDelete", current.getID())))
							.call("spatial.removeNode")
							.withArgs(layerName, exitingNode.asExpression())
							.yield(Cypher.anyNode("nodeId"))
							.with(exitingNode)
							.delete(exitingNode).build(),
					featureStore.getTransaction());
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
				writeCurrent(true);
			}
		} else {
			LOGGER.fine("Inserting " + current);
			writeCurrent(false);
		}

		live = null;
		current = null;
	}

	private void writeCurrent(boolean updateExiting) {
		Geometry geometry = (Geometry) current.getDefaultGeometry();

		var node = Cypher.anyNode("node").as("node");

		// extract additional attributes
		GeometryAttribute defaultGeometryProperty = current.getDefaultGeometryProperty();
		var extraAttributes = new HashMap<String, Object>();
		current.getProperties().forEach(property -> {
			if (property.getName().equals(defaultGeometryProperty.getName())) {
				return;
			}
			extraAttributes.put(property.getName().getLocalPart(), property.getValue());
		});

		Parameter<String> layerName = Cypher.parameter("layerName", current.getType().getTypeName());

		ExposesWith updateCall;
		if (updateExiting) {
			var exitingNode = Cypher.anyNode("exitingNode");
			Parameter<String> nodeToUpdate = Cypher.parameter("nodeToUpdate", current.getID());
			updateCall = Cypher
					.match(exitingNode)
					.where(Cypher.elementId(exitingNode).isEqualTo(nodeToUpdate))
					.call("spatial.updateWKT")
					.withArgs(layerName, exitingNode.asExpression(), Cypher.parameter("geometry", geometry.toText()))
					.yield(node);
		} else {
			updateCall = Cypher
					.call("spatial.addWKT")
					.withArgs(layerName, Cypher.parameter("geometry", geometry.toText()))
					.yield(node);
		}

		var result = extraAttributes.isEmpty() ? (ExposesReturning) updateCall : updateCall.with(node)
				.mutate(node, Cypher.parameter("attributes", extraAttributes));

		var statement = result.returning(node).build();
		dataStore.executeQuery(statement, featureStore.getTransaction());
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
		closed = true;
		reader.close();
	}
}
