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

package org.neo4j.doc.tools;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.neo4j.doc.domain.examples.Mapper;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

/**
 * A {@link JsonSerializer} for {@link Node} that serializes it to a Cypher string.
 */
public class NodeCypherSerializer extends JsonSerializer<Node> {


	@Override
	public void serialize(Node node, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		StringBuilder cypher = new StringBuilder();
		cypher.append("(:");

		// Append labels
		for (Label label : node.getLabels()) {
			cypher.append(label.name()).append(":");
		}
		if (cypher.charAt(cypher.length() - 1) == ':') {
			cypher.setLength(cypher.length() - 1); // Remove trailing colon
		}

		if (!node.getAllProperties().isEmpty()) {
			cypher.append(" {\n");

			// Append properties
			Map<String, Object> properties = new TreeMap<>(node.getAllProperties());
			for (Map.Entry<String, Object> entry : properties.entrySet()) {
				cypher.append("    ").append(entry.getKey()).append(": ");
				if (entry.getValue() instanceof String
						|| entry.getValue() instanceof double[]
						|| entry.getValue() instanceof String[]
				) {
					cypher.append(Mapper.MAPPER.writeValueAsString(entry.getValue()));
				} else {
					cypher.append(entry.getValue());
				}
				cypher.append(",\n");
			}
			if (!properties.isEmpty()) {
				cypher.setLength(cypher.length() - 2); // Remove trailing comma and space
			}
			cypher.append("\n}");
		}
		cypher.append(")");

		gen.writeRaw(cypher.toString());
	}
}
