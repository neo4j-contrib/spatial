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

package org.neo4j.spatial.compat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spatial.osm.server.plugin.procedures.OsmSpatialProcedures;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

/**
 * Opens stores written by released plugin versions (see {@code src/test/compat/generate-fixture.sh}) with the current
 * code. Read results are compared to {@code expected.txt}; run with {@code -Dcompat.update=true} to rewrite it after an
 * intentional change and review the diff.
 */
class UpgradeCompatibilityTest {

	private static final Path FIXTURES = Path.of("src/test/resources/compat");
	private static final boolean UPDATE = Boolean.getBoolean("compat.update");

	private static final List<String> GLOBAL_QUERIES = List.of(
			"CALL spatial.layers() YIELD name, signature RETURN name, signature ORDER BY name",
			"MATCH (l:SpatialLayer) UNWIND keys(l) AS k WITH l, k WHERE k <> 'layercrs' "
					+ "RETURN l.layer AS layer, k, l[k] AS v ORDER BY layer, k",
			"MATCH (l:SpatialLayer)-[:RTREE_METADATA]->(m) UNWIND keys(m) AS k "
					+ "RETURN l.layer AS layer, k, m[k] AS v ORDER BY layer, k");

	// node ids only: the database id within elementId changes each time the store is opened
	private static final Map<String, String> LAYER_QUERIES = Map.of(
			"1 layerMeta", "CALL spatial.layerMeta($l) YIELD geometryType, crs, hasComplexAttributes, extraAttributes "
					+ "RETURN geometryType, split(crs, ',')[0] AS crs, hasComplexAttributes, "
					+ "COLLECT { UNWIND keys(extraAttributes) AS k WITH k ORDER BY k "
					+ "RETURN k + ':' + extraAttributes[k] } AS extraAttributes",
			"2 count", "CALL spatial.getFeatureCount($l) YIELD count RETURN count",
			"3 layerBbox", "CALL spatial.getLayerBoundingBox($l) YIELD minX, minY, maxX, maxY "
					+ "RETURN minX, minY, maxX, maxY",
			"4 bbox", "CALL spatial.bbox($l, {lon: 15.0, lat: 60.0}, {lon: 16.0, lat: 61.0}) YIELD node "
					+ "WITH toInteger(last(split(elementId(node), ':'))) AS id ORDER BY id RETURN collect(id) AS ids",
			"5 withinDistance", "CALL spatial.withinDistance($l, {lon: 15.2, lat: 60.1}, 100) YIELD node, distance "
					+ "RETURN toInteger(last(split(elementId(node), ':'))) AS id, round(distance, 4) AS d ORDER BY d, id",
			"6 closest", "CALL spatial.closest($l, {lon: 15.2, lat: 60.1}, 1.0) YIELD node "
					+ "WITH toInteger(last(split(elementId(node), ':'))) AS id ORDER BY id RETURN collect(id) AS ids",
			"7 cql", "CALL spatial.cql($l, 'BBOX(the_geom, 15, 60, 16, 61)') YIELD node RETURN count(node) AS n");

	// layer -> properties its encoder must write, per the config stored by the release
	private static final Map<String, List<String>> WRITTEN_PROPERTIES = Map.of(
			"xy", List.of("x", "y"),
			"native_cfg", List.of("pos", "mbr"),
			"p_SimplePoint", List.of("longitude", "latitude", "bbox"),
			"p_NativePoint", List.of("location", "bbox"),
			"p_WKT", List.of("wkt"),
			"p_WKB", List.of("wkb"));

	static Stream<String> fixtures() throws IOException {
		try (var dirs = Files.list(FIXTURES)) {
			return dirs.filter(dir -> Files.exists(dir.resolve("store.zip")))
					.map(dir -> dir.getFileName().toString()).sorted().toList().stream();
		}
	}

	@ParameterizedTest
	@MethodSource("fixtures")
	void storeWrittenByReleaseStillWorks(String version) throws Exception {
		Path home = Path.of("target", "compat", version);
		FileUtils.deleteDirectory(home);
		unzip(FIXTURES.resolve(version).resolve("store.zip"), home.resolve("data"));
		DatabaseManagementService dbms = new TestDatabaseManagementServiceBuilder(home)
				.setConfig(GraphDatabaseSettings.procedure_unrestricted, List.of("spatial.*"))
				.build();
		try {
			GraphDatabaseService db = dbms.database(DEFAULT_DATABASE_NAME);
			register(db);

			Path expected = FIXTURES.resolve(version).resolve("expected.txt");
			String actual = capture(db);
			if (UPDATE) {
				// a failing query means the current code cannot read the old store: never accept that as expected
				assertThat(actual.lines().filter(line -> line.startsWith("  ERROR ")))
						.as("queries failing on the store of " + version).isEmpty();
				Files.writeString(expected, actual);
			} else {
				// for diffing against expected.txt
				Files.writeString(home.resolve("actual.txt"), actual);
				assertThat(actual).isEqualTo(Files.readString(expected));
			}
			assertWritesHonourStoredConfig(db);
		} finally {
			dbms.shutdown();
		}
	}

	private static void register(GraphDatabaseService db) throws KernelException {
		GlobalProcedures procedures = ((GraphDatabaseAPI) db).getDependencyResolver()
				.resolveDependency(GlobalProcedures.class);
		for (Class<?> api : List.of(SpatialProcedures.class, OsmSpatialProcedures.class, SpatialFunctions.class)) {
			procedures.registerProcedure(api);
			procedures.registerFunction(api);
		}
	}

	private static String capture(GraphDatabaseService db) {
		StringBuilder out = new StringBuilder();
		for (String query : GLOBAL_QUERIES) {
			out.append(query).append('\n');
			run(db, query, Map.of(), out);
		}
		for (String layer : layers(db)) {
			for (var entry : LAYER_QUERIES.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
				out.append(layer).append(" | ").append(entry.getKey().substring(2)).append('\n');
				run(db, entry.getValue(), Map.of("l", layer), out);
			}
		}
		return out.toString();
	}

	// one transaction per query: a failing procedure marks its transaction for rollback
	private static void run(GraphDatabaseService db, String query, Map<String, Object> params, StringBuilder out) {
		try (Transaction tx = db.beginTx(); Result result = tx.execute(query, params)) {
			List<String> columns = result.columns();
			result.accept(row -> {
				out.append("  ").append(columns.stream().map(c -> c + "=" + format(row.get(c)))
						.collect(Collectors.joining(", "))).append('\n');
				return true;
			});
		} catch (RuntimeException e) {
			Throwable root = e;
			while (root.getCause() != null) {
				root = root.getCause();
			}
			out.append("  ERROR ").append(root.getClass().getSimpleName()).append(": ")
					.append(String.valueOf(root.getMessage()).lines().findFirst().orElse("")).append('\n');
		}
	}

	private static String format(Object value) {
		if (value != null && value.getClass().isArray()) {
			List<String> items = new ArrayList<>();
			for (int i = 0; i < Array.getLength(value); i++) {
				items.add(format(Array.get(value, i)));
			}
			return items.toString();
		}
		if (value instanceof List<?> list) {
			return list.stream().map(UpgradeCompatibilityTest::format).toList().toString();
		}
		return String.valueOf(value);
	}

	private static List<String> layers(GraphDatabaseService db) {
		try (Transaction tx = db.beginTx();
				var names = tx.execute("CALL spatial.layers() YIELD name RETURN name ORDER BY name")
						.<String>columnAs("name")) {
			return names.stream().toList();
		}
	}

	private static void assertWritesHonourStoredConfig(GraphDatabaseService db) {
		List<String> layers = layers(db);
		for (var entry : WRITTEN_PROPERTIES.entrySet()) {
			String layer = entry.getKey();
			if (!layers.contains(layer)) {
				continue;
			}
			long before = featureCount(db, layer);
			try (Transaction tx = db.beginTx()) {
				Node node = (Node) tx.execute("CALL spatial.addWKT($l, 'POINT (15.3 60.3)') YIELD node RETURN node",
						Map.of("l", layer)).columnAs("node").next();
				assertThat(node.getPropertyKeys()).as(layer).containsAll(entry.getValue());
				tx.commit();
			}
			assertThat(featureCount(db, layer)).as(layer).isEqualTo(before + 1);
		}
		if (layers.contains("rtree20")) {
			try (Transaction tx = db.beginTx()) {
				tx.execute("""
						UNWIND range(0, 59) AS i
						CALL spatial.addWKT('rtree20', 'POINT (' + toString(15.05 + (i % 10) * 0.1) + ' '
							+ toString(60.05 + (i / 10) * 0.1) + ')') YIELD node
						RETURN count(node)""").accept(row -> true);
				tx.commit();
			}
			try (Transaction tx = db.beginTx()) {
				var row = tx.execute("""
						MATCH (l:SpatialLayer {layer: 'rtree20'})-[:RTREE_METADATA]->(m)
						MATCH (l)-[:RTREE_ROOT]->()-[:RTREE_CHILD*0..]->(n)
						RETURN m.maxNodeReferences AS max,
							max(COUNT { (n)-[:RTREE_CHILD|RTREE_REFERENCE]->() }) AS children""").next();
				assertThat((Long) row.get("children")).isLessThanOrEqualTo(((Number) row.get("max")).longValue());
			}
		}
		for (String layer : List.of("places", "native_places")) {
			if (layers.contains(layer)) {
				assertRemovalOfUserNodes(db, layer);
			}
		}
	}

	// removes all user nodes from the old index one by one; the nodes themselves stay
	private static void assertRemovalOfUserNodes(GraphDatabaseService db, String layer) {
		long count = featureCount(db, layer);
		assertThat(count).as(layer).isPositive();
		for (long remaining = count - 1; remaining >= 0; remaining--) {
			try (Transaction tx = db.beginTx()) {
				tx.execute("""
						CALL spatial.withinDistance($l, {lon: 15.2, lat: 60.1}, 1000) YIELD node
						WITH node LIMIT 1
						CALL spatial.removeNode($l, node) YIELD nodeId
						RETURN nodeId""", Map.of("l", layer)).accept(row -> true);
				tx.commit();
			}
			assertThat(featureCount(db, layer)).as(layer).isEqualTo(remaining);
		}
		try (Transaction tx = db.beginTx()) {
			assertThat(tx.execute("CALL spatial.getLayerBoundingBox($l)", Map.of("l", layer)).hasNext()).as(layer)
					.isFalse();
			assertThat(tx.execute("CALL spatial.closest($l, {lon: 15.2, lat: 60.1}, 0.0)", Map.of("l", layer))
					.hasNext()).as(layer).isFalse();
			assertThat((Long) tx.execute("MATCH (n:Place|NativePlace) RETURN count(n) AS n").columnAs("n").next())
					.as("user nodes are kept").isEqualTo(6L);
		}
	}

	private static long featureCount(GraphDatabaseService db, String layer) {
		try (Transaction tx = db.beginTx()) {
			return (Long) tx.execute("CALL spatial.getFeatureCount($l) YIELD count RETURN count", Map.of("l", layer))
					.columnAs("count").next();
		}
	}

	private static void unzip(Path zip, Path target) throws IOException {
		try (InputStream in = Files.newInputStream(zip); ZipInputStream entries = new ZipInputStream(in)) {
			for (ZipEntry entry = entries.getNextEntry(); entry != null; entry = entries.getNextEntry()) {
				Path file = target.resolve(entry.getName()).normalize();
				if (!file.startsWith(target)) {
					throw new IOException("zip entry outside target: " + entry.getName());
				}
				if (entry.isDirectory()) {
					Files.createDirectories(file);
				} else {
					Files.createDirectories(file.getParent());
					Files.copy(entries, file);
				}
			}
		}
	}
}
