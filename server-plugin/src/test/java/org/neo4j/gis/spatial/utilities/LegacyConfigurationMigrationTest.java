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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gis.spatial.encoders.NativePointEncoder;
import org.neo4j.gis.spatial.encoders.NativePointsEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.encoders.WKBGeometryEncoder;
import org.neo4j.gis.spatial.encoders.WKTGeometryEncoder;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.spatial.api.Configurable;

class LegacyConfigurationMigrationTest {

	static Stream<Arguments> legacyConfigurations() {
		Supplier<Configurable<?>> simplePoint = SimplePointEncoder::new;
		Supplier<Configurable<?>> wkt = WKTGeometryEncoder::new;
		Supplier<Configurable<?>> wkb = WKBGeometryEncoder::new;
		Supplier<Configurable<?>> nativePoint = NativePointEncoder::new;
		Supplier<Configurable<?>> nativePoints = NativePointsEncoder::new;
		Supplier<Configurable<?>> rtree = LayerRTreeIndex::new;
		return Stream.of(
				arguments(simplePoint, null, Map.of("xProperty", "longitude", "yProperty", "latitude",
						"bboxProperty", "bbox")),
				arguments(simplePoint, "", Map.of("xProperty", "longitude", "yProperty", "latitude",
						"bboxProperty", "bbox")),
				arguments(simplePoint, "x", Map.of("xProperty", "x", "yProperty", "latitude", "bboxProperty", "bbox")),
				arguments(simplePoint, "x:y:mbr", Map.of("xProperty", "x", "yProperty", "y", "bboxProperty", "mbr")),

				arguments(wkt, null, Map.of("geomProperty", "geometry", "bboxProperty", "bbox")),
				arguments(wkt, "", Map.of("geomProperty", "geometry", "bboxProperty", "bbox")),
				arguments(wkt, "wkt", Map.of("geomProperty", "wkt", "bboxProperty", "bbox")),
				arguments(wkb, "wkb:mbr", Map.of("geomProperty", "wkb", "bboxProperty", "mbr")),

				arguments(nativePoint, null, Map.of("locationProperty", "location", "bboxProperty", "bbox",
						"crs", "wgs84")),
				arguments(nativePoint, "", Map.of("locationProperty", "location", "bboxProperty", "bbox",
						"crs", "wgs84")),
				arguments(nativePoint, "pos", Map.of("locationProperty", "pos", "bboxProperty", "bbox",
						"crs", "wgs84")),
				arguments(nativePoint, "pos:mbr:Cartesian", Map.of("locationProperty", "pos", "bboxProperty", "mbr",
						"crs", "Cartesian")),

				arguments(nativePoints, null, Map.of("geomProperty", "geometry", "bboxProperty", "bbox",
						"crs", "wgs84")),
				arguments(nativePoints, "", Map.of("geomProperty", "geometry", "bboxProperty", "bbox",
						"crs", "wgs84")),
				arguments(nativePoints, "pts", Map.of("geomProperty", "pts", "bboxProperty", "bbox",
						"crs", "wgs84")),
				arguments(nativePoints, "pts:mbr:Cartesian", Map.of("geomProperty", "pts", "bboxProperty", "mbr",
						"crs", "Cartesian")),

				arguments(rtree, null, Map.of("splitMode", "greene", "maxNodeReferences", 100L,
						"shouldMergeTrees", false, "referenceRelationshipType", "RTREE_REFERENCE")),
				arguments(rtree, "", Map.of("splitMode", "greene", "maxNodeReferences", 100L,
						"shouldMergeTrees", false, "referenceRelationshipType", "RTREE_REFERENCE")),
				arguments(rtree, "{\"maxNodeReferences\":50}", Map.of("splitMode", "greene",
						"maxNodeReferences", 50L, "shouldMergeTrees", false,
						"referenceRelationshipType", "RTREE_REFERENCE")),
				arguments(rtree, "{\"splitMode\":\"quadratic\",\"maxNodeReferences\":20,\"shouldMergeTrees\":true,"
								+ "\"referenceRelationshipType\":\"REF\"}",
						Map.of("splitMode", "quadratic", "maxNodeReferences", 20L, "shouldMergeTrees", true,
								"referenceRelationshipType", "REF"))
		);
	}

	@ParameterizedTest
	@MethodSource("legacyConfigurations")
	void migratesLegacyConfiguration(Supplier<Configurable<?>> component, String legacy, Map<String, Object> expected) {
		Configurable<?> configurable = component.get();
		assertThat(ConfigurationUtils.getConfigMap(configurable.migrateLegacyConfiguration(legacy)))
				.isEqualTo(expected);
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"{\"unknown\":1}",
			"{\"splitMode\":\"linear\"}",
			"{\"maxNodeReferences\":9}",
			"{\"maxNodeReferences\":1000001}"
	})
	void rejectsInvalidLegacyRTreeConfiguration(String legacy) {
		assertThatThrownBy(() -> new LayerRTreeIndex().migrateLegacyConfiguration(legacy))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@ParameterizedTest
	@ValueSource(strings = {"splitMode=linear", "maxNodeReferences=9", "maxNodeReferences=1000001"})
	void rejectsInvalidTypedRTreeConfiguration(String invalid) {
		String[] kv = invalid.split("=");
		RTreeIndex.Config config = new RTreeIndex.Config();
		if (kv[0].equals("splitMode")) {
			config.splitMode = kv[1];
		} else {
			config.maxNodeReferences = Long.parseLong(kv[1]);
		}
		assertThatThrownBy(() -> new LayerRTreeIndex().setConfiguration(config))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining(kv[0]);
	}
}
