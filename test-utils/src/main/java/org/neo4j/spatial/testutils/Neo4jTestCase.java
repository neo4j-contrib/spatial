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
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilder;

/**
 * Base class for the metamodel tests.
 */
public abstract class Neo4jTestCase {

	private Neo4j neo4j;
	protected Driver driver;

	protected abstract List<Class<?>> loadProceduresAndFunctions();

	@BeforeEach
	void setUpDatabase() {
		Neo4jBuilder neo4jBuilder = TestNeo4jBuilders
				.newInProcessBuilder(getDbPath())
				.withConfig(GraphDatabaseSettings.procedure_unrestricted, List.of("spatial.*"))
				.withConfig(GraphDatabaseSettings.pagecache_memory, 200000000L);

		loadProceduresAndFunctions().forEach(aClass -> {
			neo4jBuilder.withProcedure(aClass);
			neo4jBuilder.withFunction(aClass);
		});

		neo4j = neo4jBuilder.build();
		driver = GraphDatabase.driver(neo4j.boltURI().toString(), AuthTokens.basic("neo4j", ""));
	}

	@AfterEach
	public void tearDown() {
		driver.close();
		neo4j.close();
	}

	static Path getDbPath() {
		return Path.of("target", "neo4j-db");
	}

	protected void printDatabaseStats() {
		SpatialTestUtils.printDatabaseStats(graphDb(), getDbPath().toFile());
	}

	protected GraphDatabaseService graphDb() {
		return neo4j.databaseManagementService().database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
	}

	protected void inTx(Consumer<Transaction> txFunction) {
		try (Transaction tx = graphDb().beginTx()) {
			txFunction.accept(tx);
			tx.commit();
		}
	}
}
