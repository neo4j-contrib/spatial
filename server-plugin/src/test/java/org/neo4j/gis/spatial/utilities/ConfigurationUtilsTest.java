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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.spatial.api.Identifiable;

class ConfigurationUtilsTest {

	private static final Identifiable WITHOUT_OPTIONS = () -> List.of("plain");

	@Test
	void component_without_options_ignores_empty_and_legacy_config() {
		assertThatCode(() -> {
			ConfigurationUtils.applyConfig("encoder", WITHOUT_OPTIONS, null);
			ConfigurationUtils.applyConfig("encoder", WITHOUT_OPTIONS, "");
			ConfigurationUtils.applyConfig("encoder", WITHOUT_OPTIONS, Map.of());
			ConfigurationUtils.applyConfig("encoder", WITHOUT_OPTIONS, "legacy");
		}).doesNotThrowAnyException();
	}

	@Test
	void component_without_options_rejects_config_map() {
		assertThatThrownBy(() -> ConfigurationUtils.applyConfig("encoder", WITHOUT_OPTIONS, Map.of("a", 1)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("encoder 'plain' has no config options, got: {a=1}");
	}

	@Test
	void component_without_options_rejects_config_of_wrong_type() {
		assertThatThrownBy(() -> ConfigurationUtils.applyConfig("encoder", WITHOUT_OPTIONS, 42L))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("config must be a MAP or STRING, got: Long");
	}
}
