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

import java.util.Map;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.neo4j.spatial.api.Configurable;
import org.neo4j.spatial.api.index.SpatialIndexWriter;

public class IndexRegistry extends BaseRegistry<SpatialIndexWriter> {

	private static final Logger LOGGER = Logger.getLogger(IndexRegistry.class.getName());

	public static final IndexRegistry INSTANCE = new IndexRegistry();

	private IndexRegistry() {
		super(SpatialIndexWriter.class, SpatialIndexWriter::getDescription);
	}

	public Map<String, EntryMeta<SpatialIndexWriter>> getRegisteredIndices() {
		return getRegisteredEntries();
	}

	public SpatialIndexWriter constructIndex(String indexName) {
		return constructIndex(indexName, null);
	}

	/**
	 * @param config see {@link #applyConfig(SpatialIndexWriter, Object)}
	 */
	public SpatialIndexWriter constructIndex(String indexName, @Nullable Object config) {
		SpatialIndexWriter index = constructEntry("index", indexName);
		applyConfig(index, config);
		return index;
	}

	/**
	 * @param config see {@link ConfigurationUtils#applyConfig(Configurable, Object)}; for a non-configurable index a
	 *               legacy string is ignored with a warning, a map is rejected
	 */
	public void applyConfig(SpatialIndexWriter index, @Nullable Object config) {
		if (index instanceof Configurable<?> configurable) {
			ConfigurationUtils.applyConfig(configurable, config);
			return;
		}
		if (ConfigurationUtils.isEmptyConfig(config)) {
			return;
		}
		switch (config) {
			case String legacy -> LOGGER.warning(
					"Warning: index configuration '" + legacy + "' passed to non-configurable index: "
							+ index.getClass());
			case Map<?, ?> map -> throw new IllegalArgumentException(
					"index type '" + index.getIdentifiers().getFirst() + "' has no config options, got: " + map);
			default -> throw ConfigurationUtils.invalidConfigType(config);
		}
	}

	public <T extends SpatialIndexWriter> T constructIndex(Class<T> indexClass) {
		return constructEntry(indexClass);
	}

	public <T extends SpatialIndexWriter & Configurable<C>, C> T constructIndex(Class<T> indexClass, C config) {
		T index = constructIndex(indexClass);
		if (config != null) {
			index.setConfiguration(config);
		}
		return index;
	}

}
