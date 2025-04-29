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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.neo4j.values.storable.Value;

/**
 * The example Cypher query.
 */
public class ExampleCypher {

	private final String cypher;
	private Map<String, Object> params = Collections.emptyMap();
	private String comment;
	private String title;
	private List<Map<String, Object>> result;
	private boolean storeResult;

	public ExampleCypher(String cypher) {
		this.cypher = cypher;
	}

	public String getCypher() {
		return cypher;
	}

	public Map<String, Object> getParams() {
		return params;
	}

	public ExampleCypher setParams(Map<String, Object> params) {
		if (params == null) {
			params = Collections.emptyMap();
		}
		this.params = params;
		return this;
	}

	public String getComment() {
		return comment;
	}

	public ExampleCypher setComment(String comment) {
		this.comment = comment;
		return this;
	}

	public String getTitle() {
		return title;
	}

	public ExampleCypher setTitle(String title) {
		this.title = title;
		return this;
	}

	public List<Map<String, Object>> getResult() {
		return result;
	}

	public ExampleCypher setResult(List<Map<String, Object>> result) {
		this.result = result;
		return this;
	}

	public boolean isStoreResult() {
		return storeResult;
	}

	public ExampleCypher storeResult() {
		this.storeResult = true;
		return this;
	}

	String resolvedCypher() {
		if (params == null) {
			return cypher;
		}
		String result = cypher;
		for (Map.Entry<String, Object> entry : params.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			try {
				result = result.replace("$" + key, Mapper.MAPPER.writeValueAsString(value));
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		}
		return result;
	}

	String generateDoc() {
		String result = "";
		if (comment != null) {
			result += comment + "\n\n";
		}
		if (title != null) {
			result += "." + title + "\n";
		}
		result += "[source,cypher]\n----\n"
				+ resolvedCypher() + "\n"
				+ "----\n\n";

		result += generateResult();

		return result;
	}

	private String generateResult() {
		if (!storeResult) {
			return "";
		}
		if (result.isEmpty()) {
			return ".Result\n\nNo results\n\n";
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
					try {
						writer.append("a|\n[source]\n----\n")
								.append(Mapper.MAPPER.writeValueAsString(value))
								.append("\n----\n");
					} catch (JsonProcessingException e) {
						throw new RuntimeException(e);
					}
				}
			}
			writer.append("\n");
		}
		writer.append("|===\n\n");
		return writer.toString();
	}
}
