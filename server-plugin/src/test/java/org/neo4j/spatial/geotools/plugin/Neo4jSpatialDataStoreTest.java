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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.geotools.api.data.ResourceInfo;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.harness.Neo4jBuilder;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.spatial.testutils.Neo4jTestCase;

public class Neo4jSpatialDataStoreTest extends Neo4jTestCase {

	@Override
	protected List<Class<?>> loadProceduresAndFunctions() {
		return List.of(SpatialFunctions.class, SpatialProcedures.class);
	}

	@BeforeEach
	public void setup() throws Exception {
		try (Session session = driver.session()) {
			// Create a test layer
			session.run("CALL spatial.addPointLayer('testlayer')");

			// Add test geometries with properties
			session.run("""
					UNWIND [
						{name: 'Test Point 1', latitude: 56.0, longitude: 13.0},
						{name: 'Test Point 2', latitude: 56.1, longitude: 13.1},
						{name: 'Nybrodalsvägen', latitude: 56.05, longitude: 13.05}
					] AS point
					CREATE (n:TestGeometry {name: point.name, latitude: point.latitude, longitude: point.longitude})
					WITH n
					CALL spatial.addNode('testlayer', n) YIELD node
					RETURN node
					""");
		}
	}


	@Test
	public void shouldFailOnNonSpatialDatabase() {
		Neo4jBuilder neo4jBuilder = Neo4jBuilders
				.newInProcessBuilder();
		try (
				var neo4jWithoutSpatial = neo4jBuilder.build();
				var driver = GraphDatabase.driver(neo4jWithoutSpatial.boltURI().toString(),
						AuthTokens.basic("neo4j", ""));
		) {
			Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(driver, DEFAULT_DATABASE_NAME);
			var exception = Assertions.catchThrowable(store::getTypeNames);
			assertThat(exception)
					.isInstanceOf(ClientException.class)
					.hasMessageContaining(
							"There is no procedure with the name `spatial.layers` registered for this database instance.");
		}
	}

	@Test
	public void shouldBeAbleToListLayers() throws IOException {
		Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(driver, DEFAULT_DATABASE_NAME);
		String[] layers = store.getTypeNames();
		MatcherAssert.assertThat("Expected one layer", layers.length, equalTo(1));
		MatcherAssert.assertThat(layers[0], equalTo("testlayer"));
	}

	@Test
	public void shouldBeAbleToGetSchemaForLayer() throws IOException {
		Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(driver, DEFAULT_DATABASE_NAME);
		SimpleFeatureType schema = store.getSchema("testlayer");
		MatcherAssert.assertThat("Expected geometry attribute to be called 'the_geom'",
				schema.getAttributeDescriptors().getFirst().getLocalName(), equalTo("the_geom"));
		// Test should have at least geometry and name attributes
		MatcherAssert.assertThat("Expected at least 2 attributes", schema.getAttributeCount() >= 2);
	}

	@Test
	public void shouldBeAbleToGetFeatureSourceForLayer() throws IOException {
		Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(driver, DEFAULT_DATABASE_NAME);
		SimpleFeatureSource source = store.getFeatureSource("testlayer");
		SimpleFeatureCollection features = source.getFeatures();
		MatcherAssert.assertThat("Expected 3 features", features.size(), equalTo(3));
		MatcherAssert.assertThat("Expected there to be a feature with name 'Nybrodalsvägen'", featureNames(features),
				hasItem("Nybrodalsvägen"));
	}

	@Test
	public void shouldBeAbleToGetInfoForLayer() throws IOException {
		Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(driver, DEFAULT_DATABASE_NAME);
		SimpleFeatureSource source = store.getFeatureSource("testlayer");
		ResourceInfo info = source.getInfo();
		ReferencedEnvelope bounds = info.getBounds();
		// Updated bounds to match our test data (approximately 13.0-13.1 lon, 56.0-56.1 lat)
		MatcherAssert.assertThat("Bounds should cover our test data",
				bounds.getMinX() <= 13.0 && bounds.getMaxX() >= 13.1 &&
						bounds.getMinY() <= 56.0 && bounds.getMaxY() >= 56.1);
		SimpleFeatureCollection features = source.getFeatures();
		MatcherAssert.assertThat("Expected 3 features", features.size(), equalTo(3));
		MatcherAssert.assertThat("Expected there to be a feature with name 'Nybrodalsvägen'", featureNames(features),
				hasItem("Nybrodalsvägen"));
	}

	private static Set<String> featureNames(SimpleFeatureCollection features) {
		HashSet<String> names = new HashSet<>();
		try (SimpleFeatureIterator featureIterator = features.features()) {
			while (featureIterator.hasNext()) {
				SimpleFeature feature = featureIterator.next();
				Object name = feature.getAttribute("name");
				if (name != null) {
					names.add(name.toString());
				}
			}
		}
		return names;
	}
}
