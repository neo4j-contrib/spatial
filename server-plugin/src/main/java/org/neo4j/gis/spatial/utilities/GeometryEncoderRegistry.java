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
import org.neo4j.spatial.api.encoder.GeometryEncoder;

public class GeometryEncoderRegistry {

	private static final Logger LOGGER = Logger.getLogger(GeometryEncoderRegistry.class.getName());

	private final Map<String, Class<? extends GeometryEncoder>> registeredEncoders = new HashMap<>();

	public static final GeometryEncoderRegistry INSTANCE = new GeometryEncoderRegistry();

	private GeometryEncoderRegistry() {

		ServiceLoader.load(GeometryEncoder.class).forEach(encoder -> encoder.getIdentifiers().forEach(identifier -> {
			if (registeredEncoders.containsKey(identifier)) {
				LOGGER.warning(
						"Geometry encoder identifier conflict: " + identifier
								+ " - overwriting previous registration");
			}
			registeredEncoders.put(identifier, encoder.getClass());
		}));
	}

	public Map<String, Class<? extends GeometryEncoder>> getRegisteredEncoders() {
		return Collections.unmodifiableMap(registeredEncoders);
	}

	public GeometryEncoder constructGeometryEncoder(String encoderName, String config) {
		Class<? extends GeometryEncoder> encoderClass = registeredEncoders.get(encoderName);
		if (encoderClass == null) {
			throw new SpatialDatabaseException("no encoder found for name: " + encoderName);
		}
		return constructGeometryEncoder(encoderClass, config);
	}

	public <T extends GeometryEncoder> T constructGeometryEncoder(Class<T> encoderClass, String config) {
		try {
			T geometryEncoder = encoderClass.getDeclaredConstructor().newInstance();
			if (config != null && !config.isEmpty()) {
				if (geometryEncoder instanceof Configurable) {
					((Configurable) geometryEncoder).setConfiguration(config);
				} else {
					LOGGER.warning(
							"Warning: encoder configuration '" + config + "' passed to non-configurable encoder: "
									+ encoderClass);
				}
			}
			return geometryEncoder;
		} catch (Exception e) {
			throw new SpatialDatabaseException(e);
		}
	}

}
