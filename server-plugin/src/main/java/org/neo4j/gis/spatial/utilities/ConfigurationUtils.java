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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.neo4j.procedure.Description;
import org.neo4j.spatial.api.Configurable;

public class ConfigurationUtils {

	// longs: config fields are persisted as Neo4j properties, which have no int type
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
			.enable(DeserializationFeature.USE_LONG_FOR_INTS);

	public record ConfigParam(
			@Description("The name of the config option")
			String name,
			@Description("What the config option controls")
			String description,
			@Description("The Cypher type of the config option")
			String type,
			@Description("The value used if the config option is not set")
			Object defaultValue
	) {

	}

	public static List<ConfigParam> getConfigMeta(Class<?> configType) {
		Map<String, Object> defaults = getConfigMap(OBJECT_MAPPER.convertValue(Map.of(), configType));
		JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructType(configType);
		return OBJECT_MAPPER.getSerializationConfig().introspect(javaType).findProperties().stream()
				.map(property -> new ConfigParam(
						property.getName(),
						property.getMetadata().getDescription(),
						cypherType(property.getPrimaryType()),
						defaults.get(property.getName())))
				.sorted(Comparator.comparing(ConfigParam::name))
				.toList();
	}

	private static String cypherType(JavaType type) {
		if (type.isArrayType() || type.isCollectionLikeType()) {
			return "LIST<" + cypherType(type.getContentType()) + ">";
		}
		Class<?> raw = type.getRawClass();
		if (raw == String.class) {
			return "STRING";
		}
		if (raw == boolean.class || raw == Boolean.class) {
			return "BOOLEAN";
		}
		if (raw == long.class || raw == int.class || raw == short.class || raw == byte.class
				|| raw == Long.class || raw == Integer.class || raw == Short.class || raw == Byte.class) {
			return "INTEGER";
		}
		if (raw == double.class || raw == float.class || raw == Double.class || raw == Float.class) {
			return "FLOAT";
		}
		// not throwing: registries resolve this at startup, one odd third-party field must not break them
		return "ANY";
	}

	public static <C> void setConfig(Configurable<C> configurable, Map<String, Object> config) {
		C typeSafeConfig = OBJECT_MAPPER.convertValue(config, configurable.getConfigType());
		configurable.setConfiguration(typeSafeConfig);
	}

	public static Map<String, Object> getConfigMap(Configurable<?> configurable) {
		return getConfigMap(configurable.getConfiguration());
	}

	public static Map<String, Object> getConfigMap(Object configuration) {
		return OBJECT_MAPPER.convertValue(configuration,
				new TypeReference<HashMap<String, Object>>() {
				});
	}


	/**
	 * @param config a map by option name (see {@link #getConfigMeta(Class)}) or a legacy string
	 *               (see {@link Configurable#migrateLegacyConfiguration(String)}); {@code null} or empty: unchanged
	 */
	@SuppressWarnings("unchecked")
	public static void applyConfig(Configurable<?> configurable, @Nullable Object config) {
		if (isEmptyConfig(config)) {
			return;
		}
		switch (config) {
			case String legacy -> setLegacyConfig(configurable, legacy);
			case Map<?, ?> map -> setConfig(configurable, (Map<String, Object>) map);
			default -> throw invalidConfigType(config);
		}
	}

	public static IllegalArgumentException invalidConfigType(Object config) {
		return new IllegalArgumentException(
				"config must be a MAP or STRING, got: " + config.getClass().getSimpleName());
	}

	public static boolean isEmptyConfig(@Nullable Object config) {
		return config == null
				|| config instanceof String s && s.isEmpty()
				|| config instanceof Map<?, ?> m && m.isEmpty();
	}

	public static <C> void setLegacyConfig(Configurable<C> configurable, String config) {
		C configuration = configurable.migrateLegacyConfiguration(config);
		configurable.setConfiguration(configuration);
	}

}
