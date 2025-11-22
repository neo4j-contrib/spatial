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
import org.neo4j.spatial.api.layer.Layer;

public class LayerRegistry {

	private static final Logger LOGGER = Logger.getLogger(LayerRegistry.class.getName());

	private final Map<String, Class<? extends Layer>> registeredLayers = new HashMap<>();

	public static final LayerRegistry INSTANCE = new LayerRegistry();

	private LayerRegistry() {

		ServiceLoader.load(Layer.class).forEach(layer -> {
			layer.getIdentifiers().forEach(identifier -> {
				if (registeredLayers.containsKey(identifier)) {
					LOGGER.warning(
							"Layer identifier conflict: " + identifier
									+ " - overwriting previous registration");
				}
				registeredLayers.put(identifier, layer.getClass());
			});
		});
	}

	public Map<String, Class<? extends Layer>> getRegisteredLayers() {
		return Collections.unmodifiableMap(registeredLayers);
	}

	public Layer constructLayer(String layerName) {
		Class<? extends Layer> layerClass = registeredLayers.get(layerName);
		if (layerClass == null) {
			throw new SpatialDatabaseException("no layer for name: " + layerName);
		}
		return constructLayer(layerClass);
	}

	public <T extends Layer> T constructLayer(Class<T> layerClass) {
		try {
			return layerClass.getDeclaredConstructor().newInstance();
		} catch (Exception e) {
			throw new SpatialDatabaseException(e);
		}
	}

}
