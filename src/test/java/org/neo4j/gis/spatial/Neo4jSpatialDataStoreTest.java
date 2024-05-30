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

package org.neo4j.gis.spatial;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.geotools.api.data.ResourceInfo;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.neo4j.Neo4jSpatialDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class Neo4jSpatialDataStoreTest {

	private DatabaseManagementService databases;
	public GraphDatabaseService graph;

	@BeforeEach
	public void setup() throws Exception {
		this.databases = new TestDatabaseManagementServiceBuilder(Path.of("target", "test")).impermanent().build();
		this.graph = databases.database(DEFAULT_DATABASE_NAME);
		OSMImporter importer = new OSMImporter("map", new ConsoleListener());
		importer.setCharset(StandardCharsets.UTF_8);
		importer.setVerbose(false);
		importer.importFile(graph, "map.osm");
		importer.reIndex(graph);
	}

	@AfterEach
	public void teardown() {
		if (this.databases != null) {
			this.databases.shutdown();
			this.databases = null;
			this.graph = null;
		}
	}

	@Test
	public void shouldOpenDataStore() {
		Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(graph);
		ReferencedEnvelope bounds = store.getBounds("map");
		MatcherAssert.assertThat(bounds, equalTo(new ReferencedEnvelope(12.7856667, 13.2873561, 55.9254241, 56.2179056,
				DefaultGeographicCRS.WGS84)));
	}

	@Test
	public void shouldOpenDataStoreOnNonSpatialDatabase() {
		DatabaseManagementService otherDatabases = null;
		try {
			otherDatabases = new TestDatabaseManagementServiceBuilder(Path.of("target", "other-db")).impermanent()
					.build();
			GraphDatabaseService otherGraph = otherDatabases.database(DEFAULT_DATABASE_NAME);
			Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(otherGraph);
			ReferencedEnvelope bounds = store.getBounds("map");
			// TODO: rather should throw a descriptive exception
			MatcherAssert.assertThat(bounds, equalTo(null));
		} finally {
			if (otherDatabases != null) {
				otherDatabases.shutdown();
			}
		}
	}

	@Test
	public void shouldBeAbleToListLayers() throws IOException {
		Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(graph);
		String[] layers = store.getTypeNames();
		MatcherAssert.assertThat("Expected one layer", layers.length, equalTo(1));
		MatcherAssert.assertThat(layers[0], equalTo("map"));
	}

	@Test
	public void shouldBeAbleToGetSchemaForLayer() throws IOException {
		Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(graph);
		SimpleFeatureType schema = store.getSchema("map");
		MatcherAssert.assertThat("Expected 25 attributes", schema.getAttributeCount(), equalTo(25));
		MatcherAssert.assertThat("Expected geometry attribute to be called 'the_geom'",
				schema.getAttributeDescriptors().get(0).getLocalName(), equalTo("the_geom"));
	}

	@Test
	public void shouldBeAbleToGetFeatureSourceForLayer() throws IOException {
		Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(graph);
		SimpleFeatureSource source = store.getFeatureSource("map");
		SimpleFeatureCollection features = source.getFeatures();
		MatcherAssert.assertThat("Expected 217 features", features.size(), equalTo(217));
		MatcherAssert.assertThat("Expected there to be a feature with name 'Nybrodalsvägen'", featureNames(features),
				hasItem("Nybrodalsvägen"));
	}

	@Test
	public void shouldBeAbleToGetInfoForLayer() throws IOException {
		Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(graph);
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
