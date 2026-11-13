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
import java.time.Duration;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.harness.Neo4jBuilder;
import org.neo4j.harness.Neo4jBuilders;

/**
 * {@link Neo4jBuilders} without Bolt's shutdown quiet period (default 5s per netty event loop group, ~10s per close).
 */
public final class TestNeo4jBuilders {

	private TestNeo4jBuilders() {
	}

	public static Neo4jBuilder newInProcessBuilder() {
		return fastShutdown(Neo4jBuilders.newInProcessBuilder());
	}

	public static Neo4jBuilder newInProcessBuilder(Path workingDirectory) {
		return fastShutdown(Neo4jBuilders.newInProcessBuilder(workingDirectory));
	}

	private static Neo4jBuilder fastShutdown(Neo4jBuilder builder) {
		return builder.withConfig(GraphDatabaseInternalSettings.netty_server_shutdown_quiet_period, Duration.ZERO);
	}
}
