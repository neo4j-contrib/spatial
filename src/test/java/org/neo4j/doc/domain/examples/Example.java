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

package org.neo4j.doc.domain.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.neo4j.doc.tools.DocNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.values.storable.Value;

/**
 * Represents a single example in the documentation.
 */
public class Example {

	private final GraphDatabaseService db;
	final String title;
	private final List<ExampleCypher> setup = new ArrayList<>();
	private ExampleCypher query;
	private final List<Map<String, Object>> result = new ArrayList<>();

	public Example(GraphDatabaseService db, String title) {
		this.db = db;
		this.title = title;
	}

	public Example setupCypher(String cypher, Map<String, Object> params, String comment) {
		setup.add(new ExampleCypher(cypher, params, comment));
		db.executeTransactionally(cypher, params == null ? Collections.emptyMap() : params);
		return this;
	}

	public Example setupCypher(String cypher, String comment) {
		return setupCypher(cypher, null, comment);
	}

	public Example runCypher(String cypher) {
		return runCypher(cypher, null, null);
	}

	public Example runCypher(String cypher, Map<String, Object> params, String comment) {
		this.query = new ExampleCypher(cypher, params, comment);
		try (Transaction tx = db.beginTx()) {
			if (params == null) {
				params = Collections.emptyMap();
			}
			Result values = tx.execute(cypher, params);
			while (values.hasNext()) {
				Map<String, Object> next = values.next();
				next.entrySet().forEach(entry -> {
					if (entry.getValue() instanceof NodeEntity node) {
						entry.setValue(new DocNode(node));
					}
				});
				result.add(next);
			}
			values.close();
			tx.commit();
		}
		return this;
	}

	public void assertSingleResult(String field, Consumer<Object> assertions) {
		Assertions.assertThat(result).singleElement()
				.asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
				.extracting(field)
				.satisfies(assertions);
	}

	public void assertResult(Consumer<List<Map<String, Object>>> assertions) {
		assertions.accept(result);
	}

	CharSequence generateSetupBlock() {
		if (setup.isEmpty()) {
			return "";
		}
		StringBuilder writer = new StringBuilder();
		for (ExampleCypher cypher : setup) {
			writer.append(cypher.generateDoc(null));
		}
		return writer.toString();
	}

	String generateCypher() {
		if (query == null) {
			return "";
		}
		return query.generateDoc("Query");
	}

	String generateResult() throws IOException {
		if (result.isEmpty()) {
			return "";
		}
		StringBuilder writer = new StringBuilder();
		writer.append(".Result\n\n");
		var columns = result.get(0).keySet();

		writer.append("[opts=\"header\",cols=\"")
				.append(columns.size())
				.append("\"]\n|===\n");
		writer.append("|");
		writer.append(String.join("|", columns));
		writer.append("\n");

		for (Map<String, Object> row : result) {
			for (String column : columns) {
				Object value = row.get(column);
				if (value instanceof Value || value instanceof String) {
					writer.append("|").append(value);
				} else {
					writer.append("a|\n[source]\n----\n")
							.append(Mapper.MAPPER.writerWithDefaultPrettyPrinter()
									.writeValueAsString(value))
							.append("\n----\n");
				}
			}
			writer.append("\n");
		}
		writer.append("|===\n\n");
		return writer.toString();
	}

}
