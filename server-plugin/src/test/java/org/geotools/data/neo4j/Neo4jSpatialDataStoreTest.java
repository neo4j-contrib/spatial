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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import org.assertj.core.api.Assertions;
import org.geotools.api.data.ResourceInfo;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.gis.spatial.LogListener;
import org.neo4j.gis.spatial.Neo4jTestCase;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.harness.Neo4jBuilder;
import org.neo4j.harness.Neo4jBuilders;

public class Neo4jSpatialDataStoreTest extends Neo4jTestCase {

	private static final Logger LOGGER = Logger.getLogger(Neo4jSpatialDataStoreTest.class.getName());


	@BeforeEach
	public void setup() throws Exception {
		OSMImporter importer = new OSMImporter("map", new LogListener(LOGGER));
		importer.setCharset(StandardCharsets.UTF_8);
		importer.importFile(graphDb(), "map.osm");
		importer.reIndex(graphDb());
	}


	@Test
	public void shouldFailOnNonSpatialDatabase() throws IOException {
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
		MatcherAssert.assertThat(layers[0], equalTo("map"));
	}

	@Test
	public void shouldBeAbleToGetSchemaForLayer() throws IOException {
		Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(driver, DEFAULT_DATABASE_NAME);
		SimpleFeatureType schema = store.getSchema("map");
		MatcherAssert.assertThat("Expected 25 attributes", schema.getAttributeCount(), equalTo(25));
		MatcherAssert.assertThat("Expected geometry attribute to be called 'the_geom'",
				schema.getAttributeDescriptors().get(0).getLocalName(), equalTo("the_geom"));
	}

	@Test
	public void shouldBeAbleToGetFeatureSourceForLayer() throws IOException {
		Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(driver, DEFAULT_DATABASE_NAME);
		SimpleFeatureSource source = store.getFeatureSource("map");
		SimpleFeatureCollection features = source.getFeatures();
		MatcherAssert.assertThat("Expected 217 features", features.size(), equalTo(217));
		MatcherAssert.assertThat("Expected there to be a feature with name 'Nybrodalsvägen'", featureNames(features),
				hasItem("Nybrodalsvägen"));
	}

	@Test
	public void shouldBeAbleToGetInfoForLayer() throws IOException {
		Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(driver, DEFAULT_DATABASE_NAME);
		SimpleFeatureSource source = store.getFeatureSource("map");
		ResourceInfo info = source.getInfo();
		ReferencedEnvelope bounds = info.getBounds();
		MatcherAssert.assertThat(bounds, equalTo(new ReferencedEnvelope(12.7856667, 13.2873561, 55.9254241, 56.2179056,
				DefaultGeographicCRS.WGS84)));
		SimpleFeatureCollection features = source.getFeatures();
		MatcherAssert.assertThat("Expected 217 features", features.size(), equalTo(217));
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
