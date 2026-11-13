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

import static org.assertj.core.api.Assertions.assertThat;

import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.jupiter.api.Test;
import org.neo4j.gis.spatial.SimplePointLayer;
import org.neo4j.gis.spatial.encoders.NativePointEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.spatial.api.layer.LayerTypePresets.RegisteredLayerType;

class LayerTypePresetRegistryTest {

	private static RegisteredLayerType preset(Object config) {
		return new RegisteredLayerType("test", SimplePointEncoder.class, SimplePointLayer.class,
				DefaultGeographicCRS.WGS84, LayerRTreeIndex.class, config);
	}

	@Test
	void typedConfigIsKept() {
		var config = new SimplePointEncoder.Config("x", "y");
		assertThat(LayerTypePresetRegistry.withTypedDefaultConfig(preset(config)).defaultEncoderConfig())
				.isSameAs(config);
	}

	@Test
	void nullConfigIsKept() {
		assertThat(LayerTypePresetRegistry.withTypedDefaultConfig(preset(null)).defaultEncoderConfig()).isNull();
	}

	@Test
	void legacyStringConfigIsMigrated() {
		var migrated = LayerTypePresetRegistry.withTypedDefaultConfig(preset("x:y:mbr")).defaultEncoderConfig();
		assertThat(migrated).isInstanceOfSatisfying(SimplePointEncoder.Config.class, c -> {
			assertThat(c.xProperty).isEqualTo("x");
			assertThat(c.yProperty).isEqualTo("y");
			assertThat(c.bboxProperty).isEqualTo("mbr");
		});
	}

	@Test
	void configOfWrongTypeSkipsPreset() {
		assertThat(LayerTypePresetRegistry.withTypedDefaultConfig(preset(new NativePointEncoder.Config()))).isNull();
	}
}
