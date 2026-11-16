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
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.spatial.api.Identifiable;

/**
 * Registry of {@link Identifiable} components discovered via {@link ServiceLoader}, keyed by each of their identifiers.
 */
public class BaseRegistry<T extends Identifiable> {

	private static final Logger LOGGER = Logger.getLogger(BaseRegistry.class.getName());

	public record EntryMeta<T>(
			List<String> identifiers,
			@Nullable String description,
			Class<? extends T> clazz
	) {

	}

	private final Map<String, EntryMeta<T>> registeredEntries = new HashMap<>();

	protected BaseRegistry(Class<T> tClass) {
		this(tClass, entry -> null);
	}

	protected BaseRegistry(Class<T> tClass, Function<T, String> descriptionExtractor) {
		ServiceLoader.load(tClass).forEach(entry -> {
			EntryMeta<T> entryMeta = new EntryMeta<>(entry.getIdentifiers(), descriptionExtractor.apply(entry),
					entry.getClass().asSubclass(tClass));
			entryMeta.identifiers().forEach(identifier -> {
				if (registeredEntries.containsKey(identifier)) {
					LOGGER.warning(tClass.getSimpleName() + " identifier conflict: " + identifier
							+ " - overwriting previous registration");
				}
				registeredEntries.put(identifier, entryMeta);
			});
		});
	}

	protected Map<String, EntryMeta<T>> getRegisteredEntries() {
		return Collections.unmodifiableMap(registeredEntries);
	}

	/**
	 * @param kind used in the error message for an unknown name
	 */
	protected T constructEntry(String kind, String name) {
		EntryMeta<T> entryMeta = registeredEntries.get(name);
		if (entryMeta == null) {
			throw new SpatialDatabaseException("no " + kind + " found for name: " + name);
		}
		return constructEntry(entryMeta.clazz());
	}

	protected <E extends T> E constructEntry(Class<E> clazz) {
		try {
			return clazz.getDeclaredConstructor().newInstance();
		} catch (Exception e) {
			throw new SpatialDatabaseException(e);
		}
	}
}
