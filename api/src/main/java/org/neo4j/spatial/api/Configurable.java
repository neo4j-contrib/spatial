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
package org.neo4j.spatial.api;

/**
 * A component with a typed configuration, persisted on the layer node as one property per field.
 * <p>
 * {@code Config} must be a Jackson-mappable bean whose fields are Neo4j property types:
 * {@code String}, {@code boolean}, {@code long}, {@code double}, or arrays thereof.
 *
 * @param <Config> the configuration type
 */
public interface Configurable<Config> {

	Config getConfiguration();

	void setConfiguration(Config configuration);

	/**
	 * @param configuration the legacy string form, may be {@code null} or empty
	 * @return the parsed configuration, defaults for missing parts
	 */
	Config migrateLegacyConfiguration(String configuration);

	/**
	 * @return the type of the configuration
	 */
	Class<Config> getConfigType();
}
