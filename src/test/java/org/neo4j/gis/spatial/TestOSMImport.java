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
package org.neo4j.gis.spatial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class TestOSMImport extends TestOSMImportBase {

	public static final String spatialTestMode = System.getProperty("spatial.test.mode");

	private static final Stream<Arguments> parameters() {
		deleteBaseDir();
		String[] smallModels = new String[]{"one-street.osm", "two-street.osm"};
//		String[] mediumModels = new String[] { "map.osm", "map2.osm" };
		String[] mediumModels = new String[]{"map.osm"};
		String[] largeModels = new String[]{"cyprus.osm", "croatia.osm", "denmark.osm"};

		// Setup default test cases (short or medium only, no long cases)
		//		layersToTest.addAll(Arrays.asList(smallModels));
		ArrayList<String> layersToTest = new ArrayList<>(Arrays.asList(mediumModels));

		// Now modify the test cases based on the spatial.test.mode setting
		if (spatialTestMode != null && spatialTestMode.equals("long")) {
			// Very long running tests
			layersToTest.addAll(Arrays.asList(largeModels));
		} else if (spatialTestMode != null && spatialTestMode.equals("short")) {
			// Tests used for a quick check
			layersToTest.clear();
			layersToTest.addAll(Arrays.asList(smallModels));
		} else if (spatialTestMode != null && spatialTestMode.equals("dev")) {
			// Tests relevant to current development
			layersToTest.clear();
//			layersToTest.add("/home/craig/Desktop/AWE/Data/MapData/baden-wurttemberg.osm/baden-wurttemberg.osm");
			layersToTest.addAll(Arrays.asList(largeModels));
		}
		boolean[] pointsTestModes = new boolean[]{true, false};

		// Finally, build the set of complete test cases based on the collection above
		ArrayList<Arguments> params = new ArrayList<>();
		for (final String layerName : layersToTest) {
			for (final boolean includePoints : pointsTestModes) {
				params.add(Arguments.of(layerName, includePoints));
			}
		}
		System.out.println("This suite has " + params.size() + " tests");
		for (Arguments arguments : params) {
			System.out.println("\t" + Arrays.toString(arguments.get()));
		}
		return params.stream();
	}

	@ParameterizedTest
	@MethodSource("parameters")
	public void runTest(String layerName, boolean includePoints) throws Exception {
		runImport(layerName, includePoints);
		try (Transaction tx = graphDb().beginTx()) {
			for (Node n : tx.getAllNodes()) {
				debugNode(n);
			}
			tx.commit();
		}
	}

	@Test
	public void buildDataModel() {
		String n1Id;
		String n2Id;
		try (Transaction tx = this.graphDb().beginTx()) {
			Node n1 = tx.createNode();
			n1.setProperty("name", "n1");
			Node n2 = tx.createNode();
			n2.setProperty("name", "n2");
			n1.createRelationshipTo(n2, RelationshipType.withName("LIKES"));
			n1Id = n1.getElementId();
			n2Id = n2.getElementId();
			debugNode(n1);
			debugNode(n2);
			tx.commit();
		}
		try (Transaction tx = this.graphDb().beginTx()) {
			for (Node n : tx.getAllNodes()) {
				debugNode(n);
			}
			tx.commit();
		}
	}

	private static void debugNode(Node node) {
		Map<String, Object> properties = node.getProperties();
		System.out.println(node + " has " + properties.size() + " properties");
		for (Map.Entry<String, Object> property : properties.entrySet()) {
			System.out.println("      key: " + property.getKey());
			System.out.println("    value: " + property.getValue());
		}
		Iterable<Relationship> relationships = node.getRelationships();
		long count = StreamSupport.stream(relationships.spliterator(), false).count();
		System.out.println(node + " has " + count + " relationships");
		for (Relationship relationship : relationships) {
			System.out.println("     (" + relationship.getStartNode() + ")-[:" + relationship.getType() + "]->("
					+ relationship.getEndNode() + ")");
		}
	}
}
