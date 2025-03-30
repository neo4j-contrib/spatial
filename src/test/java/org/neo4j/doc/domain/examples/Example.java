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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.neo4j.doc.tools.DocNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.core.NodeEntity;

/**
 * Represents a single example in the documentation.
 */
public class Example {

	private final GraphDatabaseService db;
	final String title;
	private final ExamplesRepository examplesRepository;
	private final List<ExampleCypher> queries = new ArrayList<>();
	private List<Map<String, Object>> lastResult = null;


	public Example(GraphDatabaseService db, String title, ExamplesRepository examplesRepository) {
		this.db = db;
		this.title = title;
		this.examplesRepository = examplesRepository;
	}

	public Example runCypher(String cypher) {
		return runCypher(cypher, c -> {
		});
	}

	public Example runCypher(String cypher, Consumer<ExampleCypher> customizer) {
		ExampleCypher config = new ExampleCypher(cypher);
		customizer.accept(config);
		queries.add(config);
		if (config.isStoreResult()) {
			var data = db.executeTransactionally(config.getCypher(), config.getParams(), r -> {
				List<Map<String, Object>> result = new ArrayList<>();
				while (r.hasNext()) {
					Map<String, Object> next = r.next();
					next.entrySet().forEach(entry -> {
						if (entry.getValue() instanceof NodeEntity node) {
							entry.setValue(new DocNode(node));
						}
					});
					result.add(new TreeMap<>(next));
				}
				return result;
			});
			lastResult = data;
			config.setResult(data);
		} else {
			db.executeTransactionally(config.getCypher(), config.getParams());
		}
		return this;
	}

	public Example assertSingleResult(String field, Consumer<Object> assertions) {
		Assertions.assertThat(lastResult).singleElement()
				.asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
				.extracting(field)
				.satisfies(assertions);
		return this;
	}

	public Example assertResult(Consumer<List<Map<String, Object>>> assertions) {
		assertions.accept(lastResult);
		return this;
	}

	CharSequence generateCypherBlocks() {
		if (queries.isEmpty()) {
			return "";
		}
		StringBuilder writer = new StringBuilder();
		for (ExampleCypher cypher : queries) {
			writer.append(cypher.generateDoc());
		}
		return writer.toString();
	}

	public Example additionalSignature(String signature) {
		examplesRepository.add(signature, this);
		return this;
	}

	public List<Map<String, Object>> getLastResult() {
		return lastResult;
	}

	public Map<String, Object> getLastSingleResult() {
		if (lastResult== null || lastResult.size() != 1){
			throw new IllegalStateException("Expected a single result, but got " + lastResult);
		}
		return lastResult.get(0);
	}

	public Object getLastSingleResult(String field) {
		return getLastSingleResult().get(field);
	}
}
