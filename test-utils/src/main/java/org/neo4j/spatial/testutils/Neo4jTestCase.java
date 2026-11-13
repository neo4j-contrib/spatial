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
package org.neo4j.spatial.testutils;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilder;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

/**
 * Base class for the metamodel tests.
 */
public abstract class Neo4jTestCase {

	private Neo4j neo4j;
	private DatabaseManagementService databases;
	/**
	 * Only set if {@link #useBolt()}.
	 */
	protected Driver driver;

	protected abstract List<Class<?>> loadProceduresAndFunctions();

	/**
	 * Start a Bolt server and {@link #driver} (needed by exporters and the GeoTools data store). Otherwise, an
	 * impermanent embedded database is used, which is several times faster to start.
	 */
	protected boolean useBolt() {
		return false;
	}

	@BeforeEach
	void setUpDatabase() throws KernelException {
		if (!useBolt()) {
			databases = new TestDatabaseManagementServiceBuilder(getDbPath())
					.setConfig(GraphDatabaseSettings.procedure_unrestricted, List.of("spatial.*"))
					.impermanent()
					.build();
			GlobalProcedures procedures = ((GraphDatabaseAPI) graphDb()).getDependencyResolver()
					.resolveDependency(GlobalProcedures.class);
			for (Class<?> api : loadProceduresAndFunctions()) {
				procedures.registerProcedure(api);
				procedures.registerFunction(api);
			}
			return;
		}
		Neo4jBuilder neo4jBuilder = TestNeo4jBuilders
				.newInProcessBuilder(getDbPath())
				.withConfig(GraphDatabaseSettings.procedure_unrestricted, List.of("spatial.*"))
				.withConfig(GraphDatabaseSettings.pagecache_memory, 200000000L);

		loadProceduresAndFunctions().forEach(aClass -> {
			neo4jBuilder.withProcedure(aClass);
			neo4jBuilder.withFunction(aClass);
		});

		neo4j = neo4jBuilder.build();
		databases = neo4j.databaseManagementService();
		driver = GraphDatabase.driver(neo4j.boltURI().toString(), AuthTokens.basic("neo4j", ""));
	}

	@AfterEach
	public void tearDown() {
		if (neo4j != null) {
			driver.close();
			neo4j.close();
		} else {
			databases.shutdown();
		}
	}

	static Path getDbPath() {
		return Path.of("target", "neo4j-db");
	}

	protected void printDatabaseStats() {
		SpatialTestUtils.printDatabaseStats(graphDb(), getDbPath().toFile());
	}

	protected GraphDatabaseService graphDb() {
		return databases.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
	}

	protected void inTx(Consumer<Transaction> txFunction) {
		try (Transaction tx = graphDb().beginTx()) {
			txFunction.accept(tx);
			tx.commit();
		}
	}
}
