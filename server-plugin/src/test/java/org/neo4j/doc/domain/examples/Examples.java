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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

/**
 * Represents a collection of examples for a specific procedure or fucntion.
 *
 * @param fqname   the fully qualified name of the procedure or function
 * @param examples a list of examples
 */
public record Examples(String fqname, List<Example> examples) {

	public void writeExamples() throws IOException {
		var index = fqname.lastIndexOf(".");
		var namespace = index < 0 ? "" : fqname.substring(0, index);

		Path path = Paths.get("../docs/docs/modules/ROOT/partials/generated/api", namespace,
				fqname + "-examples.adoc");
		Files.createDirectories(path.getParent());

		examples.sort(Comparator.comparing(example -> example.title));

		try (BufferedWriter writer = Files.newBufferedWriter(path)) {
			for (Example example : examples) {
				if (example.title != null) {
					writer.write("=== " + example.title + "\n\n");
				}
				writer.append(example.generateCypherBlocks());
			}
		}

	}
}
