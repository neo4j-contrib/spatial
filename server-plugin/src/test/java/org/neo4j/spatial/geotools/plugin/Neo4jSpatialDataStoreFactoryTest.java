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
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.geotools.api.data.FeatureWriter;
import org.geotools.api.data.Parameter;
import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.data.SimpleFeatureStore;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.collection.BridgeIterator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;


public class Neo4jSpatialDataStoreFactoryTest {

	private Neo4jSpatialDataStoreFactory factory;
	private Neo4jSpatialDataStore dataStore;
	private static Neo4j neo4j;


	@BeforeAll
	public static void init() {
		neo4j = Neo4jBuilders
				.newInProcessBuilder()
				.withConfig(GraphDatabaseSettings.procedure_unrestricted, List.of("spatial.*"))
				.withProcedure(SpatialProcedures.class)
				.withFunction(SpatialFunctions.class)
				.build();

	}

	@AfterAll
	public static void close() {
		neo4j.close();
	}

	@BeforeEach
	public void setup() throws Exception {
		var db = neo4j.defaultDatabaseService();
		db.executeTransactionally("MATCH (n) DETACH DELETE n");
		db.executeTransactionally("CALL spatial.addWKTLayer('map','geom')");
		db.executeTransactionally("CALL spatial.addWKT('map',\"POINT(13.0 56.0)\")");
		db.executeTransactionally("CALL spatial.addWKT('map',\"POINT(13.1 56.1)\")");
		factory = new Neo4jSpatialDataStoreFactory();
		Map<String, Object> params = new HashMap<>();
		params.put("dbtype", "neo4j-driver");
		params.put("uri", neo4j.boltURI().toString());
		params.put("username", "neo4j");
		params.put("password", "");
		params.put("database", "neo4j");

		dataStore = factory.createDataStore(params);
	}

	@AfterEach
	public void tearDown() {
		if (dataStore != null) {
			dataStore.dispose();
			dataStore = null;
		}
	}

	// Factory Tests
	@Test
	public void testGetDisplayName() {
		assertThat(factory.getDisplayName()).isEqualTo("Neo4j");
	}

	@Test
	public void testGetDescription() {
		assertThat(factory.getDescription()).isEqualTo(
				"A datasource connecting a neo4j server that has the neo4j-spatial plugin installed");
	}

	@Test
	public void testIsAvailable() {
		assertThat(factory.isAvailable()).isTrue();
	}

	@Test
	public void testGetParametersInfo() {
		assertThat(factory.getParametersInfo())
				.extracting(Parameter::getName, param -> param.getDescription().toString(), Parameter::getType,
						Parameter::isPassword,
						Parameter::isRequired)
				.containsExactlyInAnyOrder(
						tuple("dbtype", "must be 'neo4j-driver'", String.class, false, true),
						tuple("uri", "URI for the Neo4j server", String.class, false, true),
						tuple("database", "Neo4j database name", String.class, false, false),
						tuple("username", "Username for Neo4j authentication", String.class, false, true),
						tuple("password", "Password for Neo4j authentication", String.class, true, true)
				);
	}

	@Test
	public void testCanProcess() {
		Map<String, Object> params = new HashMap<>();
		params.put("dbtype", "neo4j-driver");
		assertThat(factory.canProcess(params)).isTrue();

		params.put("dbtype", "postgres");
		assertThat(factory.canProcess(params)).isFalse();

		params.remove("dbtype");
		assertThat(factory.canProcess(params)).isFalse();
	}

	@Test
	public void testCreateNewDataStore() {
		Map<String, Object> params = new HashMap<>();
		var ex = assertThrows(IOException.class, () -> factory.createDataStore(params));
		assertThat(ex).hasMessage("The parameters map isn't correct");
	}

	@Test
	public void testCreateDataStoreWithInvalidParameters() {
		Map<String, Object> params = new HashMap<>();
		params.put("dbtype", "postgres");
		assertThrows(IOException.class, () -> factory.createDataStore(params));
	}

	@Test
	public void testCreateDataStore() {
		assertThat(dataStore).isNotNull();
		assertThat(dataStore).isInstanceOf(Neo4jSpatialDataStore.class);
	}

	// DataStore API Tests
	@Test
	public void testDataStoreAPI() throws IOException {
		// Test type names (layers)
		String[] typeNames = dataStore.getTypeNames();
		assertThat(typeNames.length).isEqualTo(1);
		assertThat(typeNames[0]).isEqualTo("map");

		// Test schema
		SimpleFeatureType schema = dataStore.getSchema("map");
		assertThat(schema).isNotNull();
		assertThat(schema.getGeometryDescriptor()).isNotNull();

		// Test feature source
		SimpleFeatureSource source = dataStore.getFeatureSource("map");
		assertThat(source).isNotNull();
		assertThat(source.getSchema()).isEqualTo(schema);

		// Test bounds
		ReferencedEnvelope bounds = source.getBounds();
		Assertions.<Object>assertThat(bounds).isNotNull();
	}

	@Test
	public void testFeatureAccess() throws IOException {
		SimpleFeatureSource source = dataStore.getFeatureSource("map");
		assertThat(source).isNotNull();

		SimpleFeatureCollection features = source.getFeatures();
		assertThat(features).extracting(FeatureCollection::size).isEqualTo(2);

		// Check we can iterate through features
		try (var iterator = features.features()) {
			assertThat(new BridgeIterator<>(iterator))
					.toIterable() // Convert iterator to iterable for AssertJ
					.extracting(SimpleFeature::getDefaultGeometry)
					.extracting(Object::toString)
					.containsExactlyInAnyOrder("POINT (13 56)", "POINT (13.1 56.1)"); //
		}
	}

	@Test
	public void testFeatureQuery() throws IOException {
		SimpleFeatureSource source = dataStore.getFeatureSource("map");
		Query query = new Query("map", Filter.INCLUDE);
		SimpleFeatureCollection features = source.getFeatures(query);
		assertThat(features).extracting(FeatureCollection::size).isEqualTo(2);
	}

	// CRUD Operation Tests
	@Test
	public void testFeatureCreation() throws IOException {
		SimpleFeatureSource source = dataStore.getFeatureSource("map");
		assertThat(source).isInstanceOf(SimpleFeatureStore.class);

		SimpleFeatureStore store = (SimpleFeatureStore) source;
		Transaction transaction = new DefaultTransaction("create");

		try {
			store.setTransaction(transaction);
			int initialCount = store.getFeatures().size();

			GeometryFactory gf = new GeometryFactory();
			Point newPoint = gf.createPoint(new Coordinate(13.5, 56.5));
			addNewFeature(newPoint, transaction);

			transaction.commit();

			SimpleFeatureCollection features = store.getFeatures();
			assertThat(features.size()).isEqualTo(initialCount + 1);

			// Verify the new feature exists
			var found = searchFeatureByXYCoordinates(13.5, 56.5, features);
			assertThat(found).as("New feature should be found").isNotNull();

		} catch (Exception e) {
			transaction.rollback();
			throw e;
		} finally {
			transaction.close();
		}
	}

	@Test
	public void testFeatureModification() throws IOException {
		SimpleFeatureSource source = dataStore.getFeatureSource("map");
		assertThat(source).isInstanceOf(SimpleFeatureStore.class);

		SimpleFeatureStore store = (SimpleFeatureStore) source;
		Transaction transaction = new DefaultTransaction("edit");

		try {
			store.setTransaction(transaction);

			SimpleFeature originalFeature = getFirstFeature(store);
			assertThat(originalFeature)
					.as("No features found to modify")
					.isNotNull();

			Point originalPoint = extractPoint(originalFeature);
			Assertions.<Object>assertThat(originalPoint)
					.as("Feature geometry should be a Point")
					.isNotNull();

			Point newPoint = createModifiedPoint(originalPoint);
			modifyFeatureGeometry(originalFeature.getID(), newPoint, transaction);

			transaction.commit();
			verifyFeatureModification(store, originalFeature.getID(), newPoint);

		} catch (Exception e) {
			transaction.rollback();
			throw e;
		} finally {
			transaction.close();
		}
	}

	@Test
	public void testFeatureDeletion() throws IOException {
		SimpleFeatureSource source = dataStore.getFeatureSource("map");
		assertThat(source).isInstanceOf(SimpleFeatureStore.class);

		SimpleFeatureStore store = (SimpleFeatureStore) source;
		Transaction transaction = new DefaultTransaction("delete");

		try {
			store.setTransaction(transaction);

			int initialCount = store.getFeatures().size();
			assertThat(initialCount).isGreaterThan(0);

			SimpleFeature featureToDelete = getFirstFeature(store);
			assertThat(featureToDelete).isNotNull();
			String featureId = featureToDelete.getID();

			// Delete the feature
			try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
					dataStore.getFeatureWriter("map", Filter.INCLUDE, transaction)) {

				while (writer.hasNext()) {
					SimpleFeature feature = writer.next();
					if (featureId.equals(feature.getID())) {
						writer.remove();
						break;
					}
				}
			}

			transaction.commit();

			SimpleFeatureCollection features = store.getFeatures();
			assertThat(features.size()).isEqualTo(initialCount - 1);

			SimpleFeature deletedFeature = findFeatureById(store, featureId);
			assertThat(deletedFeature).isNull();

		} catch (Exception e) {
			transaction.rollback();
			throw e;
		} finally {
			transaction.close();
		}
	}

	@Test
	public void testComprehensiveCRUDOperations() throws IOException {
		SimpleFeatureSource source = dataStore.getFeatureSource("map");
		assertThat(source).isInstanceOf(SimpleFeatureStore.class);
		SimpleFeatureStore store = (SimpleFeatureStore) source;

		Transaction transaction = new DefaultTransaction("crud-transaction");

		try {
			store.setTransaction(transaction);

			int initialCount = store.getFeatures().size();
			assertThat(initialCount).isGreaterThan(0);

			SimpleFeature existingFeature = getFirstFeature(store);
			assertThat(existingFeature).isNotNull();
			String existingFeatureId = existingFeature.getID();
			Point existingPoint = extractPoint(existingFeature);

			// CREATE: Add a new feature
			GeometryFactory gf = new GeometryFactory();
			Point newPoint = gf.createPoint(new Coordinate(14.0, 57.0));
			addNewFeature(newPoint, transaction);

			SimpleFeatureCollection features = store.getFeatures();
			assertThat(features.size()).isEqualTo(initialCount + 1);

			// Find the newly added feature
			SimpleFeatureCollection features1 = store.getFeatures();
			SimpleFeature addedFeature = searchFeatureByXYCoordinates(14.0, 57.0, features1);
			assertThat(addedFeature).isNotNull();
			String addedFeatureId = addedFeature.getID();

			// UPDATE: Modify an existing feature
			Assertions.<Object>assertThat(existingPoint).isNotNull();
			Point modifiedPoint = createModifiedPoint(existingPoint);
			modifyFeatureGeometry(existingFeatureId, modifiedPoint, transaction);

			// DELETE: Remove the feature we added
			try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
					dataStore.getFeatureWriter("map", Filter.INCLUDE, transaction)) {

				while (writer.hasNext()) {
					SimpleFeature feature = writer.next();
					if (addedFeatureId.equals(feature.getID())) {
						writer.remove();
						break;
					}
				}
			}

			transaction.commit();

			// Verify the final state
			features = store.getFeatures();
			assertThat(features.size()).isEqualTo(initialCount);

			// Verify modification persisted
			SimpleFeature modifiedFeature = findFeatureById(store, existingFeatureId);
			assertThat(modifiedFeature).isNotNull();
			assertThat(modifiedFeature.getDefaultGeometry())
					.isInstanceOf(Point.class)
					.asInstanceOf(InstanceOfAssertFactories.type(Point.class))
					.satisfies(point -> {
						assertThat(point.getX()).isEqualTo(modifiedPoint.getX());
						assertThat(point.getY()).isEqualTo(modifiedPoint.getY());
					});

			// Verify deletion persisted
			SimpleFeature deletedFeature = findFeatureById(store, addedFeatureId);
			assertThat(deletedFeature).isNull();

		} catch (Exception e) {
			transaction.rollback();
			throw e;
		} finally {
			transaction.close();
		}
	}

	@Test
	public void testTransactionRollback() throws IOException {
		SimpleFeatureSource source = dataStore.getFeatureSource("map");
		assertThat(source).isInstanceOf(SimpleFeatureStore.class);

		SimpleFeatureStore store = (SimpleFeatureStore) source;
		Transaction transaction = new DefaultTransaction("rollback-test");

		store.setTransaction(transaction);

		int initialCount = store.getFeatures().size();
		SimpleFeature originalFeature = getFirstFeature(store);
		assertThat(originalFeature).isNotNull();
		Point originalPoint = extractPoint(originalFeature);
		String featureId = originalFeature.getID();

		// Perform modifications
		Assertions.<Object>assertThat(originalPoint).isNotNull();
		Point modifiedPoint = createModifiedPoint(originalPoint);
		modifyFeatureGeometry(featureId, modifiedPoint, transaction);

		// Add a new feature
		GeometryFactory gf = new GeometryFactory();
		Point newPoint = gf.createPoint(new Coordinate(14.0, 57.0));
		addNewFeature(newPoint, transaction);

		// Verify changes are visible within the transaction
		SimpleFeatureCollection features = store.getFeatures();
		assertThat(features.size()).isEqualTo(initialCount + 1);

		SimpleFeature modifiedFeatureInTx = findFeatureById(store, featureId);
		assertThat(modifiedFeatureInTx).isNotNull();
		assertThat(modifiedFeatureInTx.getDefaultGeometry())
				.isInstanceOf(Point.class)
				.asInstanceOf(InstanceOfAssertFactories.type(Point.class))
				.satisfies(point -> {
					assertThat(point.getX()).isEqualTo(modifiedPoint.getX());
					assertThat(point.getY()).isEqualTo(modifiedPoint.getY());
				});

		// Now roll back the transaction
		transaction.rollback();
		transaction.close();

		// Verify state is back to the original
		store = (SimpleFeatureStore) dataStore.getFeatureSource("map");
		SimpleFeatureCollection featuresAfterRollback = store.getFeatures();
		assertThat(featuresAfterRollback.size()).isEqualTo(initialCount);

		// Feature should have original geometry
		SimpleFeature restoredFeature = findFeatureById(store, featureId);
		assertThat(restoredFeature).isNotNull();
		assertThat(restoredFeature.getDefaultGeometry())
				.isInstanceOf(Point.class)
				.asInstanceOf(InstanceOfAssertFactories.type(Point.class))
				.satisfies(point -> {
					assertThat(point.getX()).isEqualTo(originalPoint.getX());
					assertThat(point.getY()).isEqualTo(originalPoint.getY());
				});

		// New feature should not exist
		assertThat(searchFeatureByXYCoordinates(14.0, 57.0, featuresAfterRollback)).isNull();
	}

	// Helper Methods
	private SimpleFeature getFirstFeature(SimpleFeatureStore store) throws IOException {
		SimpleFeatureCollection features = store.getFeatures();
		try (SimpleFeatureIterator iterator = features.features()) {
			return iterator.hasNext() ? iterator.next() : null;
		}
	}

	private Point extractPoint(SimpleFeature feature) {
		Object geometry = feature.getDefaultGeometry();
		return geometry instanceof Point ? (Point) geometry : null;
	}

	private Point createModifiedPoint(Point originalPoint) {
		GeometryFactory gf = new GeometryFactory();
		return gf.createPoint(new Coordinate(
				originalPoint.getX() + 0.002,
				originalPoint.getY() + 0.005
		));
	}

	private void addNewFeature(Point geometry, Transaction transaction) throws IOException {
		try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
				dataStore.getFeatureWriter("map", Filter.INCLUDE, transaction)) {

			while (writer.hasNext()) {
				writer.next();
			}

			SimpleFeature feature = writer.next();
			feature.setDefaultGeometry(geometry);
			writer.write();
		}
	}

	private void modifyFeatureGeometry(String featureId, Point newGeometry, Transaction transaction)
			throws IOException {
		try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
				dataStore.getFeatureWriter("map", Filter.INCLUDE, transaction)) {

			while (writer.hasNext()) {
				SimpleFeature feature = writer.next();
				if (featureId.equals(feature.getID())) {
					feature.setDefaultGeometry(newGeometry);
					writer.write();
					return;
				}
			}
		}
		Assertions.fail("Feature with ID %s not found for modification", featureId);
	}

	private void verifyFeatureModification(SimpleFeatureStore store, String featureId, Point expectedGeometry)
			throws IOException {
		SimpleFeature modifiedFeature = findFeatureById(store, featureId);
		assertThat(modifiedFeature).isNotNull();

		assertThat(modifiedFeature.getDefaultGeometry())
				.isInstanceOf(Point.class)
				.asInstanceOf(InstanceOfAssertFactories.type(Point.class))
				.satisfies(point -> {
					assertThat(point.getX()).isEqualTo(expectedGeometry.getX());
					assertThat(point.getY()).isEqualTo(expectedGeometry.getY());
				});
	}

	private SimpleFeature findFeatureById(SimpleFeatureStore store, String featureId) throws IOException {
		SimpleFeatureCollection features = store.getFeatures(Filter.INCLUDE);
		try (SimpleFeatureIterator iterator = features.features()) {
			while (iterator.hasNext()) {
				SimpleFeature feature = iterator.next();
				if (featureId.equals(feature.getID())) {
					return feature;
				}
			}
		}
		return null;
	}

	private static SimpleFeature searchFeatureByXYCoordinates(double x, double y, SimpleFeatureCollection features) {
		try (SimpleFeatureIterator iterator = features.features()) {
			while (iterator.hasNext()) {
				SimpleFeature feature = iterator.next();
				Object geom = feature.getDefaultGeometry();
				if (geom instanceof Point point
						&& Math.abs(point.getX() - x) < 0.001
						&& Math.abs(point.getY() - y) < 0.001
				) {
					return feature;
				}
			}
		}
		return null;
	}
}

