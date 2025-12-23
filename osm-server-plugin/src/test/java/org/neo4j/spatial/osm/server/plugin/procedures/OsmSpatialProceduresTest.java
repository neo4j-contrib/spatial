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
package org.neo4j.spatial.osm.server.plugin.procedures;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.spatial.testutils.AbstractApiTest;

public class OsmSpatialProceduresTest extends AbstractApiTest {

	private static final Logger LOGGER = Logger.getLogger(OsmSpatialProceduresTest.class.getName());

	@Override
	protected void registerApiProceduresAndFunctions() throws KernelException {
		registerProceduresAndFunctions(SpatialProcedures.class);
		registerProceduresAndFunctions(OsmSpatialProcedures.class);
		registerProceduresAndFunctions(SpatialFunctions.class);
	}

	public static void testCall(GraphDatabaseService db, String call, Consumer<Map<String, Object>> consumer) {
		testCall(db, call, null, consumer);
	}

	public static void testCall(GraphDatabaseService db, String call, Map<String, Object> params,
			Consumer<Map<String, Object>> consumer) {
		testCall(db, call, params, consumer, true);
	}

	public static void testCallFails(GraphDatabaseService db, String call, Map<String, Object> params, String error) {
		try {
			testResult(db, call, params, (res) -> {
				while (res.hasNext()) {
					res.next();
				}
			});
			fail("Expected an exception containing '" + error + "', but no exception was thrown");
		} catch (Exception e) {
			Assertions.assertThat(e.getMessage())
					.contains(error);
		}
	}

	public static void testCall(GraphDatabaseService db, String call, Map<String, Object> params,
			Consumer<Map<String, Object>> consumer, boolean onlyOne) {
		testResult(db, call, params, (res) -> {
			assertTrue(res.hasNext(), "Expect at least one result but got none: " + call);
			Map<String, Object> row = res.next();
			consumer.accept(row);
			if (onlyOne) {
				assertFalse(res.hasNext(), "Expected only one result, but there are more");
			}
		});
	}

	public static void testCallCount(GraphDatabaseService db, String call, Map<String, Object> params, int count) {
		testResult(db, call, params, (res) -> {
			int numLeft = count;
			while (numLeft > 0) {
				assertTrue(res.hasNext(),
						"Expected " + count + " results but found only " + (count - numLeft));
				res.next();
				numLeft--;
			}
			assertFalse(res.hasNext(), "Expected " + count + " results but there are more");
		});
	}

	public static void testResult(GraphDatabaseService db, String call, Consumer<Result> resultConsumer) {
		testResult(db, call, null, resultConsumer);
	}

	public static void testResult(GraphDatabaseService db, String call, Map<String, Object> params,
			Consumer<Result> resultConsumer) {
		try (Transaction tx = db.beginTx()) {
			Map<String, Object> p = (params == null) ? Map.of() : params;
			resultConsumer.accept(tx.execute(call, p));
			tx.commit();
		}
	}

	@Test
	public void create_a_pointlayer_with_config_on_existing_osm_layer() {
		execute("CALL spatial.addLayer('geom','OSM','')");
		try {
			testCall(db, "CALL spatial.addPointLayerWithConfig('geom','lon:lat')",
					(r) -> assertEquals("geom", ((Node) r.get("node")).getProperty("layer")));
			fail("Expected exception to be thrown");
		} catch (Exception e) {
			Assertions.assertThat(e.getMessage()).contains("Layer already exists: 'geom'");
		}
	}


	@Test
	public void list_layer_types() {
		testResult(db, "CALL spatial.layerTypes()", (res) -> {
			Map<String, Map<String, Object>> layerTypes = res.stream()
					.collect(Collectors.toMap(r -> r.get("id").toString(), r -> r));

			Assertions.assertThat(layerTypes).containsKey("OSM");
			Assertions.assertThat(layerTypes.get("OSM"))
					.containsEntry("id", "OSM")
					.containsEntry("encoder", "OSMGeometryEncoder")
					.containsEntry("layer", "OSMLayer")
					.containsEntry("index", "rtree")
					.containsEntry("crsName", "WGS84(DD)")
					.containsEntry("defaultEncoderConfig", "geometry");
		});
	}


	@Test
	public void import_osm() {
		testCountQuery("importOSM", "CALL spatial.importOSM('map.osm')", 55, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}

	@Test
	public void import_osm_twice_should_fail() {
		testCountQuery("importOSM", "CALL spatial.importOSM('map.osm')", 55, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
		testCallFails(db, "CALL spatial.importOSM('map.osm')", null, "Layer already exists: 'map.osm'");
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}

	@Test
	public void import_osm_without_extension() {
		testCountQuery("importOSM", "CALL spatial.importOSM('map')", 55, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}

	@Test
	public void import_osm_to_layer() {
		execute("CALL spatial.addLayer('geom','OSM','')");
		testCountQuery("importOSMToLayer", "CALL spatial.importOSMToLayer('geom','map.osm')", 55, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}

	@Test
	public void import_osm_twice_should_pass_with_different_layers() {
		execute("CALL spatial.addLayer('geom1','OSM','')");
		execute("CALL spatial.addLayer('geom2','OSM','')");

		testCountQuery("importOSM", "CALL spatial.importOSMToLayer('geom1','map.osm')", 55, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 2);
		testCallCount(db, "CALL spatial.withinDistance('geom1',{lon:6.3740429666,lat:50.93676351666},10000)", null,
				217);
		testCallCount(db, "CALL spatial.withinDistance('geom2',{lon:6.3740429666,lat:50.93676351666},10000)", null, 0);

		testCountQuery("importOSM", "CALL spatial.importOSMToLayer('geom2','map.osm')", 55, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 2);
		testCallCount(db, "CALL spatial.withinDistance('geom1',{lon:6.3740429666,lat:50.93676351666},10000)", null,
				217);
		testCallCount(db, "CALL spatial.withinDistance('geom2',{lon:6.3740429666,lat:50.93676351666},10000)", null,
				217);
	}

	@Test
	public void import_osm_to_layer_without_changesets() {
		execute("CALL spatial.addLayer('osm_example','OSM','')");
		testCountQuery("importOSMToLayerWithoutChangesets", "CALL spatial.importOSMToLayer('osm_example','sample.osm')",
				1, "count", null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
	}

	@Test
	public void import_osm_and_add_geometry() {
		execute("CALL spatial.addLayer('geom','OSM','')");
		testCountQuery("importOSMToLayerAndAddGeometry", "CALL spatial.importOSMToLayer('geom','map.osm')", 55, "count",
				null);
		testCallCount(db, "CALL spatial.layers()", null, 1);
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},100)", null, 0);
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},10000)", null, 217);

		// Adding a point to the layer
		Node node = createNode(
				"CALL spatial.addWKT('geom', 'POINT(6.3740429666 50.93676351666)') YIELD node RETURN node", "node");
		testCall(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},100)",
				r -> assertEquals(node, r.get("node")));
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},100)", null, 1);
		testCallCount(db, "CALL spatial.withinDistance('geom',{lon:6.3740429666,lat:50.93676351666},10000)", null, 218);
	}

	@Test
	public void import_osm_and_polygons_withinDistance() {
		Map<String, Object> params = Map.of("osmFile", "withinDistance.osm", "busShelterID", 2938842290L);
		execute("CALL spatial.addLayer('geom','OSM','')");
		testCountQuery("importOSMAndPolygonsWithinDistance", "CALL spatial.importOSMToLayer('geom',$osmFile)", 74,
				"count", params);
		testCallCount(db, "CALL spatial.layers()", null, 1);
		testCallCount(db,
				"MATCH (n) WHERE n.node_osm_id = $busShelterID CALL spatial.withinDistance('geom',n,100) YIELD node, distance RETURN node, distance",
				params, 516);
		testResult(db,
				"MATCH (n) WHERE n.node_osm_id = $busShelterID CALL spatial.withinDistance('geom',n,100) YIELD node, distance WITH node, distance ORDER BY distance LIMIT 20 MATCH (node)<-[:GEOM]-(osmNode) RETURN node, distance, osmNode, properties(osmNode) as props",
				params, res -> {
					while (res.hasNext()) {
						Map<String, Object> r = res.next();
						assertThat("Result should have 'node'", r, hasKey("node"));
						assertThat("Result should have 'distance'", r, hasKey("distance"));
						assertThat("Result should have 'osmNode'", r, hasKey("osmNode"));
						assertThat("Result should have 'props'", r, hasKey("props"));
						Node node = (Node) r.get("node");
						double distance = (Double) r.get("distance");
						Node osmNode = (Node) r.get("osmNode");
						@SuppressWarnings({"rawtypes", "unchecked"}) Map<String, Object> props = (Map) r.get("props");
						LOGGER.fine(
								"(node[" + node.getElementId() + "])<-[:GEOM {distance:" + distance + "}]-(osmNode["
										+ osmNode.getElementId() + "] " + props + ") ");
						assertThat("Node should have either way_osm_id or node_osm_id", props,
								anyOf(hasKey("node_osm_id"), hasKey("way_osm_id")));
					}
				});
		testResult(db,
				"MATCH (n) WHERE n.node_osm_id = $busShelterID CALL spatial.withinDistance('geom',n,100) YIELD node, distance WITH node, distance ORDER BY distance LIMIT 20 MATCH (n) WHERE elementId(n)=elementId(node) RETURN node, distance, spatial.decodeGeometry('geom',n) AS geometry",
				params, res -> {
					while (res.hasNext()) {
						Map<String, Object> r = res.next();
						assertThat("Result should have 'node'", r, hasKey("node"));
						assertThat("Result should have 'distance'", r, hasKey("distance"));
						assertThat("Result should have 'geometry'", r, hasKey("geometry"));
						Node node = (Node) r.get("node");
						double distance = (Double) r.get("distance");
						Object geometry = r.get("geometry");
						LOGGER.fine(node.toString() + " at " + distance + ": " + geometry);
						if (geometry instanceof Point) {
							assertThat("Point has 2D coordinates",
									((Point) geometry).getCoordinate().getCoordinate().length, equalTo(2));
						} else if (geometry instanceof Map) {
							@SuppressWarnings("unchecked") Map<String, Object> map = (Map<String, Object>) geometry;
							assertThat("Geometry should contain a type", map, hasKey("type"));
							assertThat("Geometry should contain coordinates", map, hasKey("coordinates"));
							assertThat("Geometry should not be a point", map.get("type"), not(equalTo("Point")));
						} else {
							fail("Geometry should be either a point or a Map containing coordinates");
						}
					}
				});
	}

}
