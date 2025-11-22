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

package org.neo4j.gis.spatial.utilities;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.logging.Logger;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.encoders.Configurable;
import org.neo4j.spatial.api.index.SpatialIndexWriter;

public class IndexRegistry {

	private static final Logger LOGGER = Logger.getLogger(IndexRegistry.class.getName());

	private final Map<String, Class<? extends SpatialIndexWriter>> registeredIndices = new HashMap<>();

	public static final IndexRegistry INSTANCE = new IndexRegistry();

	private IndexRegistry() {

		ServiceLoader.load(SpatialIndexWriter.class).forEach(index -> {
			index.getIdentifiers().forEach(identifier -> {
				if (registeredIndices.containsKey(identifier)) {
					LOGGER.warning(
							"Index identifier conflict: " + identifier
									+ " - overwriting previous registration");
				}
				registeredIndices.put(identifier, index.getClass());
			});
		});
	}

	public Map<String, Class<? extends SpatialIndexWriter>> getRegisteredIndices() {
		return Collections.unmodifiableMap(registeredIndices);
	}

	public SpatialIndexWriter constructIndex(String indexName, String config) {
		Class<? extends SpatialIndexWriter> indexClass = registeredIndices.get(indexName);
		if (indexClass == null) {
			throw new SpatialDatabaseException("no index for name: " + indexName);
		}
		return constructIndex(indexClass, config);
	}

	public <T extends SpatialIndexWriter> T constructIndex(Class<T> indexClass, String config) {
		try {
			T index = indexClass.getDeclaredConstructor().newInstance();
			if (config != null && !config.isEmpty()) {
				if (index instanceof Configurable) {
					((Configurable) index).setConfiguration(config);
				} else {
					LOGGER.warning(
							"Warning: index configuration '" + config + "' passed to non-configurable index: "
									+ indexClass);
				}
			}
			return index;
		} catch (Exception e) {
			throw new SpatialDatabaseException(e);
		}
	}

}
