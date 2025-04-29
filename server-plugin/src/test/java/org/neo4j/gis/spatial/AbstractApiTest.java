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

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.doc.domain.examples.Example;
import org.neo4j.doc.domain.examples.ExamplesRepository;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public abstract class AbstractApiTest {

	private DatabaseManagementService databases;
	protected GraphDatabaseService db;
	static ExamplesRepository examples;

	@BeforeEach
	public void setUp() throws KernelException, IOException {
		Path dbRoot = new File("target/procedures").toPath();
		FileUtils.deleteDirectory(dbRoot);
		databases = new TestDatabaseManagementServiceBuilder(dbRoot)
				.setConfig(GraphDatabaseSettings.procedure_unrestricted, List.of("spatial.*"))
				.impermanent()
				.build();
		db = databases.database(DEFAULT_DATABASE_NAME);
		examples = new ExamplesRepository(db);
		registerApiProceduresAndFunctions();
	}

	protected abstract void registerApiProceduresAndFunctions() throws KernelException;

	protected void registerProceduresAndFunctions(Class<?> api) throws KernelException {
		GlobalProcedures procedures = ((GraphDatabaseAPI) db).getDependencyResolver()
				.resolveDependency(GlobalProcedures.class);
		procedures.registerProcedure(api);
		procedures.registerFunction(api);
	}

	@AfterEach
	public void tearDown() {
		databases.shutdown();
	}

	protected long execute(String statement) {
		return execute(statement, null);
	}

	protected long execute(String statement, Map<String, Object> params) {
		try (Transaction tx = db.beginTx()) {
			if (params == null) {
				params = Collections.emptyMap();
			}
			long count = Iterators.count(tx.execute(statement, params));
			tx.commit();
			return count;
		}
	}

	protected void executeWrite(String call) {
		try (Transaction tx = db.beginTx()) {
			tx.execute(call).accept(v -> true);
			tx.commit();
		}
	}

	protected Node createNode(String call, String column) {
		return (Node) executeObject(call, null, column);
	}

	protected Object executeObject(String call, String column) {
		return executeObject(call, null, column);
	}

	protected Object executeObject(String call, Map<String, Object> params, String column) {
		Object obj;
		try (Transaction tx = db.beginTx()) {
			if (params == null) {
				params = Collections.emptyMap();
			}
			ResourceIterator<Object> values = tx.execute(call, params).columnAs(column);
			obj = values.next();
			values.close();
			tx.commit();
		}
		return obj;
	}


	protected Example docExample(@Nonnull String signature, @Nonnull String title) {
		return examples.docExample(signature, title);
	}

	@AfterAll
	public static void generateDocumentation() throws IOException {
		examples.write();
	}

}
