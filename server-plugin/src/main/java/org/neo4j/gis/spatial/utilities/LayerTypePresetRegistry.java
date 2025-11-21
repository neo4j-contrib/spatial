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
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.neo4j.gis.spatial.EditableLayerImpl;
import org.neo4j.spatial.api.encoder.GeometryEncoder;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.api.layer.LayerTypePresets;
import org.neo4j.spatial.api.layer.LayerTypePresets.RegisteredLayerType;

public class LayerTypePresetRegistry {

	private static final Logger LOGGER = Logger.getLogger(LayerTypePresetRegistry.class.getName());

	private final Map<String, RegisteredLayerType> registeredLayerPresets = new TreeMap<>();

	public static final LayerTypePresetRegistry INSTANCE = new LayerTypePresetRegistry();

	private LayerTypePresetRegistry() {
		ServiceLoader.load(LayerTypePresets.class).forEach(layerPresets -> {
			layerPresets.getLayerTypePresets().forEach(preset -> {
				String key = preset.typeName().toLowerCase();
				if (registeredLayerPresets.containsKey(key)) {
					LOGGER.warning(
							"Duplicate layer type preset detected: " + key + " - overwriting previous registration");
				}
				registeredLayerPresets.put(key, preset);
			});
		});
	}

	public Map<String, RegisteredLayerType> getRegisteredLayerPresets() {
		return Collections.unmodifiableMap(registeredLayerPresets);
	}


	@Nonnull
	public Class<? extends Layer> suggestLayerClassForEncoder(Class<? extends GeometryEncoder> encoderClass) {
		for (RegisteredLayerType type : registeredLayerPresets.values()) {
			if (type.geometryEncoder() == encoderClass) {
				return type.layerClass();
			}
		}
		return EditableLayerImpl.class;
	}

	@Nullable
	public RegisteredLayerType getRegisteredLayerType(String identifier) {
		return registeredLayerPresets.get(identifier.toLowerCase());
	}

}
