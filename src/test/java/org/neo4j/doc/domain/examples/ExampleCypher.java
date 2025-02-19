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
import java.util.Map;

/**
 * The example Cypher query.
 *
 * @param cypher  The Cypher query.
 * @param params  The parameters for the Cypher query.
 * @param comment The comment for the Cypher query.
 */
public record ExampleCypher(String cypher, Map<String, Object> params, String comment) {


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

	String generateDoc(String title) {
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
		return result;
	}
}
