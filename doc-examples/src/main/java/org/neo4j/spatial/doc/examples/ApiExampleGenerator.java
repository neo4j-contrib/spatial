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

package org.neo4j.spatial.doc.examples;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.spatial.functions.SpatialFunctions;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spatial.doc.examples.domain.Example;
import org.neo4j.spatial.doc.examples.domain.ExampleCypher;
import org.neo4j.spatial.doc.examples.domain.ExamplesRepository;
import org.neo4j.test.GraphDatabaseServiceCleaner;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "ApiExampleGenerator", mixinStandardHelpOptions = true)
public class ApiExampleGenerator implements Callable<Integer> {

	@Option(names = {"-p",
			"--partials-root"}, description = "The root directory where the examples should be placed in")
	private Path partialsRoot = Path.of("./docs/docs/modules/ROOT/partials/generated/api");

	private GraphDatabaseService db;
	private ExamplesRepository examples;

	public static void main(String... args) {
		int exitCode = new CommandLine(new ApiExampleGenerator()).execute(args);
		System.exit(exitCode);
	}

	@Override
	public Integer call() throws Exception {
		ApiExampleGenerator generator = new ApiExampleGenerator();
		try (var neo4j = new TestDatabaseManagementServiceBuilder()
				.setConfig(GraphDatabaseSettings.procedure_unrestricted, List.of("spatial.*"))
				.impermanent()
				.build()) {

			generator.setUp(neo4j);
			generator.generateAllExamples();
			generator.writeDocumentation();
		}
		System.out.println("Examples successfully generated!");
		return 0;
	}

	private void setUp(DatabaseManagementService neo4j) throws KernelException {
		db = neo4j.database(DEFAULT_DATABASE_NAME);

		GlobalProcedures procedures = ((GraphDatabaseAPI) db)
				.getDependencyResolver()
				.resolveDependency(GlobalProcedures.class);
		procedures.registerProcedure(SpatialProcedures.class);
		procedures.registerFunction(SpatialFunctions.class);

		examples = new ExamplesRepository();
	}

	private Example docExample(String signature, String title) {
		GraphDatabaseServiceCleaner.cleanDatabaseContent(db);
		return examples.docExample(signature, title, db);
	}

	private void generateAllExamples() {
		generateSpatialFunctionExamples();
		generateSpatialProcedureExamples();
	}

	private void generateSpatialFunctionExamples() {

		docExample("spatial.asGeometry", "Creates a point geometry")
				.runCypher(
						"WITH point({latitude: 5.0, longitude: 4.0}) as geom RETURN spatial.asGeometry(geom) AS geometry");

		docExample("spatial.asMap", "Creates a point geometry as map")
				.runCypher(
						"WITH point({latitude: 5.0, longitude: 4.0}) as geom RETURN spatial.asMap(geom) AS geometry");

		docExample("spatial.asGeometry", "Creates a point geometry from a map")
				.runCypher("WITH spatial.asGeometry({latitude: 5.0, longitude: 4.0}) AS geometry RETURN geometry");

		docExample("spatial.wktToGeoJson", "1. Converts a WKT POINT")
				.runCypher("RETURN spatial.wktToGeoJson('POINT (30 10)') as json");

		docExample("spatial.wktToGeoJson", "2. Converts a WKT LINESTRING")
				.runCypher("RETURN spatial.wktToGeoJson('LINESTRING (30 10, 10 30, 40 40)') as json");

		docExample("spatial.wktToGeoJson", "3. Converts a WKT POLYGON")
				.runCypher("RETURN spatial.wktToGeoJson('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') as json");

		docExample("spatial.wktToGeoJson", "4. Converts a WKT POLYGON with a hole")
				.runCypher(
						"RETURN spatial.wktToGeoJson('POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))') as json");

		docExample("spatial.wktToGeoJson", "5a. Converts a WKT MULTIPOINT")
				.runCypher(
						"RETURN spatial.wktToGeoJson('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))') as json");

		docExample("spatial.wktToGeoJson", "5b. Converts a WKT MULTIPOINT")
				.runCypher("RETURN spatial.wktToGeoJson('MULTIPOINT (10 40, 40 30, 20 20, 30 10)') as json");

		docExample("spatial.wktToGeoJson", "6. Converts a WKT MULTILINESTRING")
				.runCypher(
						"RETURN spatial.wktToGeoJson('MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))') as json");

		docExample("spatial.wktToGeoJson", "7a. Converts a WKT MULTIPOLYGON")
				.runCypher(
						"RETURN spatial.wktToGeoJson('MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))') as json");

		docExample("spatial.wktToGeoJson", "7b. Converts a WKT MULTIPOLYGON")
				.runCypher(
						"RETURN spatial.wktToGeoJson('MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))') as json");

		docExample("spatial.wktToGeoJson", "8. FConverts a WKT GEOMETRYCOLLECTION")
				.runCypher(
						"RETURN spatial.wktToGeoJson('GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))') as json");

		docExample("spatial.neo4jGeometryToWkt", "Converting a point to WKT")
				.runCypher("RETURN spatial.neo4jGeometryToWkt(point({longitude: 1, latitude: 2})) as wkt");

		docExample("spatial.neo4jGeometryToWkt", "Converting a point array to WKT")
				.runCypher(
						"RETURN spatial.neo4jGeometryToWkt([point({longitude: 1, latitude: 2}), point({longitude: 3, latitude: 4}) ]) as wkt");

		docExample("spatial.extractAttributes", "Extracts attributes from a layer node")
				.runCypher("""
						CALL spatial.addPointLayer('attr_layer') YIELD node
						WITH node
						CREATE (n:Point {longitude: 10.0, latitude: 20.0, name: 'test_point', type: 'landmark', elevation: 100})
						WITH n
						CALL spatial.addNode('attr_layer', n) YIELD node as added_node
						WITH n
						RETURN spatial.extractAttributes('attr_layer', n) as attributes
						""");

		docExample("spatial.extractAttributes", "Extracts attributes from a WKT layer node")
				.runCypher("""
						CALL spatial.addLayer('wkt_attr_layer', 'WKT', '') YIELD node
						WITH node
						CREATE (n:Geometry {geometry: 'POINT (30 10)', name: 'test_wkt_point', category: 'marker', id: 42})
						WITH n
						CALL spatial.addNode('wkt_attr_layer', n) YIELD node as added_node
						WITH n
						RETURN spatial.extractAttributes('wkt_attr_layer', n) as attributes
						""");

		docExample("spatial.decodeGeometry", "Using both functions together")
				.additionalSignature("spatial.extractAttributes")
				.runCypher("""
						CALL spatial.addPointLayer('combined_layer') YIELD node
						WITH node
						CREATE (n:Point {longitude: 5.5, latitude: 45.5, city: 'TestCity', population: 50000})
						WITH n
						CALL spatial.addNode('combined_layer', n) YIELD node as added_node
						WITH n
						RETURN
							spatial.decodeGeometry('combined_layer', n) as geometry,
							spatial.extractAttributes('combined_layer', n) as attributes
						""");

		docExample("spatial.nodeAsWKT", "Converting a layer node to WKT")
				.runCypher("""
						CALL spatial.addPointLayer('wkt_layer') YIELD node
						WITH node
						CREATE (n:Point {longitude: 10.0, latitude: 20.0})
						WITH n
						CALL spatial.addNode('wkt_layer', n) YIELD node as added_node
						WITH n
						RETURN spatial.nodeAsWKT('wkt_layer', n) as wkt
						""");
	}

	private void generateSpatialProcedureExamples() {
		docExample("spatial.decodeGeometry", "Decode a geometry from a node property")
				.additionalSignature("spatial.addWKTLayer")
				.runCypher("CALL spatial.addWKTLayer('geom','geom')",
						config -> config.setTitle("Create a WKT layer"))
				.runCypher(
						"CREATE (n:Node {geom:'POINT(4.0 5.0)'}) RETURN spatial.decodeGeometry('geom',n) AS geometry",
						config -> config.setTitle("Decode a geometry"));

		docExample("spatial.addNativePointLayerWithConfig", "Create a native point layer with a configuration")
				.runCypher("CALL spatial.addNativePointLayerWithConfig('geom','pos:mbr','hilbert')");

		docExample("spatial.addNativePointLayerXY", "Create a native point layer")
				.additionalSignature("spatial.withinDistance")
				.additionalSignature("spatial.addNode")
				.runCypher("CALL spatial.addNativePointLayerXY('geom','x','y')")
				.runCypher(
						"CREATE (n:Node {id: 42, x: 5.0, y: 4.0}) WITH n CALL spatial.addNode('geom',n) YIELD node RETURN node",
						config -> config.skipResult().setComment("create a node and add it to the index"))
				.runCypher("CALL spatial.withinDistance('geom',point({latitude:4.1,longitude:5.1}),100)",
						config -> config.setComment("Find node within distance"));

		docExample("spatial.addPointLayerWithConfig", "Create a point layer with X and Y properties")
				.runCypher("CALL spatial.addPointLayerWithConfig('geom','lon:lat')");

		docExample("spatial.addLayerWithEncoder", "Create a `SimplePointEncoder`")
				.runCypher("CALL spatial.addLayerWithEncoder('geom','SimplePointEncoder','')");

		docExample("spatial.addLayerWithEncoder",
				"Create a `SimplePointEncoder` with a customized encoder configuration")
				.runCypher("CALL spatial.addLayerWithEncoder('geom','SimplePointEncoder','x:y:mbr')",
						config -> config.setComment("""
								Configures the encoder to use the nodes `x` property instead of `longitude`,
								the `y` property instead of `latitude`
								and the `mbr` property instead of `bbox`.
								"""));

		docExample("spatial.addLayerWithEncoder", "Create a `NativePointEncoder`")
				.runCypher("CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','')");

		docExample("spatial.addLayerWithEncoder",
				"Create a `NativePointEncoder` with a customized encoder configuration")
				.runCypher("CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','pos:mbr')",
						config -> config.setComment("""
								Configures the encoder to use the nodes `pos` property instead of `location`
								and the `mbr` property instead of `bbox`.
								"""));

		docExample("spatial.addLayerWithEncoder",
				"Create a `NativePointEncoder` with a customized encoder configuration using Cartesian coordinates")
				.runCypher("CALL spatial.addLayerWithEncoder('geom','NativePointEncoder','pos:mbr:Cartesian')",
						config -> config.setComment("""
								Configures the encoder to use the nodes `pos` property instead of `location`,
								the `mbr` property instead of `bbox` and Cartesian coordinates.
								"""));

		docExample("spatial.layers", "Add and Remove a layer")
				.additionalSignature("spatial.removeLayer")
				.runCypher("CALL spatial.addWKTLayer('geom','wkt')")
				.runCypher("CALL spatial.layers()")
				.runCypher("CALL spatial.removeLayer('geom')")
				.runCypher("CALL spatial.layers()");

		docExample("spatial.getFeatureAttributes", "Get the feature attributes of a layer")
				.additionalSignature("spatial.setFeatureAttributes")
				.runCypher("CALL spatial.addWKTLayer('geom','wkt')")
				.runCypher("CALL spatial.getFeatureAttributes('geom')")
				.runCypher("CALL spatial.setFeatureAttributes('geom',['name','type','color'])")
				.runCypher("CALL spatial.getFeatureAttributes('geom')");

		docExample("spatial.layerTypes", "List the available layer types")
				.runCypher("CALL spatial.layerTypes()");

		Stream.of("Simple", "Native").forEach(encoder ->
				Stream.of("Geohash", "ZOrder", "Hilbert", "RTree").forEach(indexType -> {
					String procName =
							encoder.equals("Native") ? "spatial.addNativePointLayer" : "spatial.addPointLayer";
					if (!indexType.equals("RTree")) {
						procName += indexType;
					}
					String layerName = ("my-" + encoder + "-" + indexType + "-layer").toLowerCase();

					docExample(procName, "Create a layer to index a node")
							.runCypher("CALL " + procName + "('" + layerName + "')")
							.runCypher(
									"CREATE (n:Node {id: 42, latitude:60.1,longitude:15.2}) SET n.location=point(n) RETURN n",
									config -> config.skipResult().setComment("Create a node to index"))
							.runCypher("MATCH (n:Node) WITH n CALL spatial.addNode('" + layerName
											+ "',n) YIELD node RETURN node",
									config -> config.setComment("Index node"))
							.runCypher(
									"CALL spatial.withinDistance('" + layerName + "',{lon:15.0,lat:60.0},100)",
									config -> config.setComment("Find node within distance"));

				}));

		docExample("spatial.addPointLayerXY", "Create a point layer with X and Y properties")
				.additionalSignature("spatial.addNodes")
				.additionalSignature("spatial.removeNode")
				.additionalSignature("spatial.removeNode.byId")
				.runCypher("CALL spatial.addPointLayerXY('geom','lon','lat')", ExampleCypher::skipResult)
				.runCypher(
						"CREATE (n1:Node {id: 1, lat:60.1,lon:15.2}),(n2:Node {id: 2, lat:60.1,lon:15.3}) WITH n1,n2 CALL spatial.addNodes('geom',[n1,n2]) YIELD count RETURN n1,n2,count",
						config -> config.setComment("Add two nodes to the layer"))
				.runCypher("CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)",
						config -> config.setComment("Find nodes within distance"))
				.runCypher("""
								MATCH (node) WHERE node.id = 1
								CALL spatial.removeNode('geom', node) YIELD nodeId
								RETURN nodeId
								""",
						config -> config.skipResult().setComment("Remove node 1"))
				.runCypher("CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)")
				.runCypher("""
								MATCH (node) WHERE node.id = 2
								CALL spatial.removeNode.byId('geom', elementId(node)) YIELD nodeId
								RETURN nodeId
								""",
						config -> config.skipResult().setComment("Remove node 2"))
				.runCypher("CALL spatial.withinDistance('geom',{lon:15.0,lat:60.0},100)");

		docExample("spatial.addLayer", "Add the same node to multiple layers")
				.runCypher("""
								UNWIND range(1,$count) as i
								CREATE (n:Point {
								    id: i,
								    point1: point( { latitude: 56.0, longitude: 12.0 } ),
								    point2: point( { latitude: 57.0, longitude: 13.0 } )
								})""",
						config -> config.skipResult().setParams(Map.of("count", 100)).setTitle("Create some nodes"))
				.runCypher("""
						CALL spatial.addLayer(
							'point1',
							'NativePoint',
							'point1:point1BB',
							'{"referenceRelationshipType": "RTREE_P1_TYPE"}'
						)
						""", config -> config.skipResult().setComment("""
						Create a layer `point1` to index property `point1` of node `Point`.
						Save the bounding box in the property `point1BB` of the `Point` node.
						Associate the node with the index layer via relationship type `RTREE_P1_TYPE`.
						"""))
				.runCypher("""
						CALL spatial.addLayer(
							'point2',
							'NativePoint',
							'point2:point2BB',
							'{"referenceRelationshipType": "RTREE_P2_TYPE"}'
						)
						""", config -> config.skipResult().setComment("""
						Create a layer `point2` to index property `point2` of node `Point`.
						Save the bounding box in the property `point2BB` of the `Point` node.
						Associate the node with the index layer via relationship type `RTREE_P2_TYPE`.
						"""))
				.runCypher("""
						MATCH (p:Point)
						WITH (count(p) / 10) AS pages, collect(p) AS nodes
						UNWIND range(0, pages) AS i CALL {
						    WITH i, nodes
						    CALL spatial.addNodes('point1', nodes[(i * 10)..((i + 1) * 10)]) YIELD count
						    RETURN count AS count
						} IN TRANSACTIONS OF 1 ROWS
						RETURN sum(count) AS count
						""", config -> config.setComment("Index the nodes in layer `point1` in chunks of 10"))
				.runCypher("""
								MATCH (p:Point)
								WITH (count(p) / 10) AS pages, collect(p) AS nodes
								UNWIND range(0, pages) AS i CALL {
									WITH i, nodes
									CALL spatial.addNodes('point2', nodes[(i * 10)..((i + 1) * 10)]) YIELD count
									RETURN count AS count
								} IN TRANSACTIONS OF 1 ROWS
								RETURN sum(count) AS count
								""",
						config -> config.setComment("Index the nodes in layer `point2` in chunks of 10"));

		docExample("spatial.importShapefile", "Import a shape-file")
				.runCypher("CALL spatial.importShapefile('example-data/shp/highway.shp')")
				.runCypher("CALL spatial.layers()");

		docExample("spatial.importShapefileToLayer", "Import a shape-file")
				.runCypher("CALL spatial.addWKTLayer('geom','wkt')", ExampleCypher::skipResult)
				.runCypher("CALL spatial.importShapefileToLayer('geom', 'example-data/shp/highway.shp')")
				.runCypher("CALL spatial.layers()");

		docExample("spatial.importOSM", "Import an OSM file")
				.runCypher("CALL spatial.importOSM('example-data/osm/example.osm')")
				.runCypher("CALL spatial.layers()");

		docExample("spatial.importOSMToLayer", "Import an OSM file")
				.runCypher("CALL spatial.addLayer('geom','OSM','')", ExampleCypher::skipResult)
				.runCypher("CALL spatial.importOSMToLayer('geom','example-data/osm/example.osm')")
				.runCypher("CALL spatial.layers()");

		docExample("spatial.bbox", "Find geometries in a bounding box")
				.runCypher("CALL spatial.addPointLayer('geom')", ExampleCypher::skipResult)
				.runCypher("""
						CREATE (n:Node {id: 1, latitude:60.1,longitude:15.2})
						WITH n CALL spatial.addNode('geom',n) YIELD node
						RETURN node
						""", ExampleCypher::skipResult)
				.runCypher("CALL spatial.bbox('geom',{lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})",
						c -> c.setComment("Find node within bounding box"));

		String polygon = "POLYGON((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2))";
		docExample("spatial.intersects", "Find geometries in a polygon")
				.runCypher("CALL spatial.addPointLayer('geom')", ExampleCypher::skipResult)
				.runCypher("""
						UNWIND [ {name:'a',latitude:60.1,longitude:15.2}, {name:'b',latitude:60.3,longitude:15.5} ] as point
						CREATE (n:Node)
						SET n += point
						WITH n
						CALL spatial.addNode('geom',n) YIELD node
						RETURN node.name as name
						""")
				.runCypher(
						"CALL spatial.intersects('geom','" + polygon + "') YIELD node\n RETURN node.name as name");

		String lineString = "LINESTRING (15.2 60.1, 15.3 60.1)";
		docExample("spatial.addWKT", "Add a WKT geometry to a layer")
				.additionalSignature("spatial.addWKTLayer")
				.runCypher("CALL spatial.addWKTLayer('geom', 'wkt')", ExampleCypher::skipResult)
				.runCypher("CALL spatial.addWKT('geom',$wkt)",
						c -> c.setParams(Map.of("wkt", lineString)))
				.runCypher("CALL spatial.closest('geom',{lon:15.2, lat:60.1}, 1.0)");

		var points = List.of("POINT (15.2 60.1)", "POINT (25.2 30.1)");
		docExample("spatial.addWKTs", "Add multiple WKT geometries to a layer")
				.additionalSignature("spatial.closest")
				.runCypher("CALL spatial.addLayer('geom','geohash','lon:lat')", ExampleCypher::skipResult)
				.runCypher("CALL spatial.addWKTs('geom',$wkt)",
						c -> c.setParams(Map.of("wkt", points)))
				.runCypher("CALL spatial.closest('geom',{lon:15.0, lat:60.0}, 1.0)");

		docExample("spatial.cql", "Find geometries using CQL")
				.runCypher("CALL spatial.addWKTLayer('geom','wkt') YIELD node")
				.runCypher("""
								CREATE (n1:Node {wkt: 'POINT(15.2 60.1)', name: 'point1'})
								CREATE (n2:Node {wkt: 'POINT(25.2 30.1)', name: 'point2'})
								WITH n1, n2
								CALL spatial.addNode('geom', n1) YIELD node as added1
								WITH n2, added1
								CALL spatial.addNode('geom', n2) YIELD node as added2
								RETURN added1, added2
								""",
						config -> config.skipResult().setComment("Create and add nodes with different coordinates"))
				.runCypher("CALL spatial.cql('geom', 'name = \\'point1\\'') YIELD node RETURN node.name as name");

		docExample("spatial.getFeatureCount", "Get the number of features in a layer")
				.runCypher("CALL spatial.addPointLayer('count_layer') YIELD node")
				.runCypher("CALL spatial.getFeatureCount('count_layer') YIELD count",
						config -> config.setComment("Get count of empty layer"))
				.runCypher("""
						CREATE (n1:Point {latitude: 60.1, longitude: 15.2, name: 'point1'})
						CREATE (n2:Point {latitude: 60.3, longitude: 15.5, name: 'point2'})
						WITH n1, n2
						CALL spatial.addNode('count_layer', n1) YIELD node as added1
						WITH n2, added1
						CALL spatial.addNode('count_layer', n2) YIELD node as added2
						RETURN added1, added2
						""", config -> config.skipResult().setComment("Add two points to the layer"))
				.runCypher("CALL spatial.getFeatureCount('count_layer') YIELD count",
						config -> config.setComment("Get count after adding points"));

		docExample("spatial.getLayerBoundingBox", "Get the bounding box of a layer")
				.runCypher("CALL spatial.addPointLayer('bbox_layer', 'rtree', 'wgs84') YIELD node")
				.runCypher("""
						CREATE (n1:Point {latitude: 60.0, longitude: 15.0, name: 'southwest'})
						CREATE (n2:Point {latitude: 61.0, longitude: 16.0, name: 'northeast'})
						WITH n1, n2
						CALL spatial.addNode('bbox_layer', n1) YIELD node as added1
						WITH n2, added1
						CALL spatial.addNode('bbox_layer', n2) YIELD node as added2
						RETURN added1, added2
						""", config -> config.skipResult().setComment("Add points at opposite corners"))
				.runCypher("CALL spatial.getLayerBoundingBox('bbox_layer') YIELD minX, minY, maxX, maxY, crs");

		docExample("spatial.layerMeta", "Get metadata about a layer")
				.runCypher("CALL spatial.addPointLayer('meta_layer', 'rtree', 'wgs84') YIELD node")
				.runCypher("""
						CREATE (n1:Point {latitude: 60.0, longitude: 15.0, name: 'southwest'})
						WITH n1
						CALL spatial.addNode('meta_layer', n1) YIELD node
						RETURN node
						""", config -> config.skipResult().setComment("Add points at opposite corners"))
				.runCypher(
						"CALL spatial.layerMeta('meta_layer') YIELD name, geometryType, crs, hasComplexAttributes, extraAttributes");

		docExample("spatial.updateWKT", "Update a node's WKT geometry")
				.runCypher("CALL spatial.addWKTLayer('update_layer', 'wkt') YIELD node")
				.runCypher("""
						CREATE (n:Node {wkt: 'POINT(15.2 60.1)', name: 'updatable_point'})
						WITH n
						CALL spatial.addNode('update_layer', n) YIELD node as added_node
						RETURN n, added_node
						""", config1 -> config1.skipResult().setComment("Create and add a node with initial WKT"))
				.runCypher("""
								MATCH (n:Node {name: 'updatable_point'})
								CALL spatial.updateWKT('update_layer', n, 'POINT(25.5 65.5)') YIELD node
								RETURN node.wkt as wkt
								""",
						config1 -> config1.setComment("Update the node's WKT geometry"))
				.runCypher("""
								CALL spatial.withinDistance('update_layer', {longitude: 25.5, latitude: 65.5}, 1) YIELD node
								RETURN node.name as name
								""",
						config -> config.setComment("Verify the updated geometry is indexed correctly"));

		docExample("spatial.getFeatureCount", "Count features in different layer types")
				.runCypher("CALL spatial.addPointLayer('count_layer')",
						config1 -> config1.skipResult().setComment("Create a point layer"))
				.runCypher("CALL spatial.getFeatureCount('count_layer') YIELD count",
						config1 -> config1.setComment("Count features in empty layer"))
				.runCypher("""
						CREATE (n:Node {latitude: 60.1, longitude: 15.2, name: 'first'})
						WITH n
						CALL spatial.addNode('count_layer', n) YIELD node
						RETURN node
						""", config1 -> config1.skipResult().setComment("Add one node to the layer"))
				.runCypher("CALL spatial.getFeatureCount('count_layer') YIELD count",
						config1 -> config1.setComment("Count after adding one feature"))
				.runCypher("""
						UNWIND range(1,3) as i
						CREATE (n:Node {id: i, latitude: (60.0 + i * 0.1), longitude: (15.0 + i * 0.1)})
						WITH collect(n) as nodes
						CALL spatial.addNodes('count_layer', nodes) YIELD count
						RETURN count
						""", config -> config.skipResult().setComment("Add multiple nodes at once"))
				.runCypher("CALL spatial.getFeatureCount('count_layer') YIELD count",
						config -> config.setComment("Count after adding multiple features"))
				.runCypher("CALL spatial.addWKTLayer('wkt_layer', 'wkt')",
						config -> config.skipResult().setComment("Create a WKT layer"))
				.runCypher("CALL spatial.getFeatureCount('wkt_layer') YIELD count",
						config -> config.setComment("Count features in empty WKT layer"))
				.runCypher("CALL spatial.addWKT('wkt_layer', 'POINT(15.2 60.1)') YIELD node RETURN node",
						config -> config.skipResult().setComment("Add a WKT point"))
				.runCypher(
						"CALL spatial.addWKT('wkt_layer', 'LINESTRING (15.2 60.1, 15.3 60.1)') YIELD node RETURN node",
						config -> config.skipResult().setComment("Add a WKT linestring"))
				.runCypher("CALL spatial.getFeatureCount('wkt_layer') YIELD count",
						config -> config.setComment("Count features in WKT layer"));
	}

	private void writeDocumentation() throws IOException {
		examples.write(partialsRoot);
	}
}
