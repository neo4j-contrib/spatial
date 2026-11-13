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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.spatial.api.Configurable;

public class ConfigurationUtils {

	// longs: config fields are persisted as Neo4j properties, which have no int type
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
			.enable(DeserializationFeature.USE_LONG_FOR_INTS);

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


	public static <C> void setLegacyConfig(Configurable<C> configurable, String config) {
		C configuration = configurable.migrateLegacyConfiguration(config);
		configurable.setConfiguration(configuration);
	}

}
