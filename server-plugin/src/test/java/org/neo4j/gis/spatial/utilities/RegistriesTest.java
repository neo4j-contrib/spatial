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

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gis.spatial.SimplePointLayer;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.gis.spatial.utilities.BaseRegistry.EntryMeta;

class RegistriesTest {

	static Stream<Arguments> registries() {
		return Stream.of(
				Arguments.of("encoder", GeometryEncoderRegistry.INSTANCE.getRegisteredEncoders(),
						(Function<String, Object>) GeometryEncoderRegistry.INSTANCE::constructGeometryEncoder,
						"SimplePointEncoder", SimplePointEncoder.class),
				Arguments.of("index", IndexRegistry.INSTANCE.getRegisteredIndices(),
						(Function<String, Object>) IndexRegistry.INSTANCE::constructIndex,
						"rtree", LayerRTreeIndex.class),
				Arguments.of("layer", LayerRegistry.INSTANCE.getRegisteredLayers(),
						(Function<String, Object>) LayerRegistry.INSTANCE::constructLayer,
						"SimplePointLayer", SimplePointLayer.class)
		);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("registries")
	void unknownNameThrows(String kind, Map<String, ? extends EntryMeta<?>> entries,
			Function<String, Object> construct, String shortId, Class<?> clazz) {
		assertThatThrownBy(() -> construct.apply("does-not-exist"))
				.isInstanceOf(SpatialDatabaseException.class)
				.hasMessage("no " + kind + " found for name: does-not-exist");
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("registries")
	void shortIdAndClassNameResolveToSameEntry(String kind, Map<String, ? extends EntryMeta<?>> entries,
			Function<String, Object> construct, String shortId, Class<?> clazz) {
		EntryMeta<?> byShortId = entries.get(shortId);
		assertThat(byShortId).isNotNull().isSameAs(entries.get(clazz.getName()));
		assertThat(byShortId.clazz()).isEqualTo(clazz);
		assertThat(construct.apply(shortId)).isExactlyInstanceOf(clazz);
		assertThat(construct.apply(clazz.getName())).isExactlyInstanceOf(clazz);
	}

	// persisted in layers created by releases before 2026.10, before the classes moved (#458)
	@ParameterizedTest
	@CsvSource({"org.neo4j.gis.spatial.WKTGeometryEncoder, WKTGeometryEncoder",
			"org.neo4j.gis.spatial.WKBGeometryEncoder, WKBGeometryEncoder"})
	void legacyEncoderClassNamesResolve(String legacyName, String id) {
		assertThat(GeometryEncoderRegistry.INSTANCE.getRegisteredEncoders().get(legacyName))
				.isSameAs(GeometryEncoderRegistry.INSTANCE.getRegisteredEncoders().get(id));
	}

	@Test
	void encodersAndIndicesHaveDescriptions() {
		Stream.concat(
						GeometryEncoderRegistry.INSTANCE.getRegisteredEncoders().values().stream(),
						IndexRegistry.INSTANCE.getRegisteredIndices().values().stream())
				.forEach(entry -> assertThat(entry.description()).as(entry.clazz().getName()).isNotBlank());
	}
}
