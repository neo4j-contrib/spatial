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
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.neo4j.graphdb.GraphDatabaseService;

public class ExamplesRepository {

	static Map<String, Examples> examples = new HashMap<>();
	private GraphDatabaseService db;

	public ExamplesRepository(GraphDatabaseService db) {
		this.db = db;
	}

	public Example docExample(@Nonnull String signature, @Nonnull String title) {
		Example example = new Example(db, title, this);
		add(signature, example);
		return example;
	}

	public void add(@Nonnull String signature, @Nonnull Example example) {
		examples.computeIfAbsent(signature, (s) -> new Examples(s, new ArrayList<>()))
				.examples().add(example);
	}

	public void write() throws IOException {
		for (Examples examples : examples.values()) {
			examples.writeExamples();
		}
	}
}
