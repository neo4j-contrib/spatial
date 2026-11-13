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
import static org.neo4j.gis.spatial.Constants.LABEL_LAYER;
import static org.neo4j.gis.spatial.Constants.PROP_GEOMENCODER;
import static org.neo4j.gis.spatial.Constants.PROP_GEOMENCODER_CONFIG;
import static org.neo4j.gis.spatial.Constants.PROP_INDEX_CONFIG;
import static org.neo4j.gis.spatial.Constants.PROP_INDEX_TYPE;
import static org.neo4j.gis.spatial.Constants.PROP_LAYER;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gis.spatial.EditableLayerImpl;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.encoders.NativePointEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.index.IndexManagerImpl;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spatial.api.Configurable;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.testutils.Neo4jTestCase;

class LayerConfigPersistenceTest extends Neo4jTestCase {

	private SpatialDatabaseService spatial;

	@Override
	protected List<Class<?>> loadProceduresAndFunctions() {
		return List.of();
	}

	@BeforeEach
	void setUpSpatial() {
		spatial = new SpatialDatabaseService(
				new IndexManagerImpl((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
	}

	@Test
	void newLayerPersistsFlattenedConfigAndReloadsIt() {
		inTx(tx -> spatial.createLayer(tx, "layer", NativePointEncoder.class, EditableLayerImpl.class,
				LayerRTreeIndex.class, "pos:mbr:Cartesian", "{\"maxNodeReferences\":20,\"splitMode\":\"quadratic\"}"));

		Map<String, Object> expectedEncoder = Map.of("locationProperty", "pos", "bboxProperty", "mbr",
				"crs", "Cartesian");
		Map<String, Object> expectedIndex = Map.of("splitMode", "quadratic", "maxNodeReferences", 20L,
				"shouldMergeTrees", false, "referenceRelationshipType", "RTREE_REFERENCE");
		inTx(tx -> {
			Node layerNode = tx.findNode(LABEL_LAYER, PROP_LAYER, "layer");
			assertThat(layerNode.hasProperty(PROP_GEOMENCODER_CONFIG)).isFalse();
			assertThat(layerNode.hasProperty(PROP_INDEX_CONFIG)).isFalse();
			assertThat(prefixed(layerNode, PROP_GEOMENCODER_CONFIG)).isEqualTo(expectedEncoder);
			assertThat(prefixed(layerNode, PROP_INDEX_CONFIG)).isEqualTo(expectedIndex);
		});
		inTx(tx -> {
			Layer layer = spatial.getLayer(tx, "layer", true);
			assertThat(config(layer.getGeometryEncoder())).isEqualTo(expectedEncoder);
			assertThat(config(layer.getIndex())).isEqualTo(expectedIndex);
		});
	}

	@Test
	void legacyLayerLoadsStringConfig() {
		inTx(tx -> {
			Node layerNode = legacyLayerNode(tx);
			layerNode.setProperty(PROP_INDEX_CONFIG, "{\"maxNodeReferences\":20,\"splitMode\":\"quadratic\"}");
		});
		inTx(tx -> {
			Layer layer = spatial.getLayer(tx, "legacy", false);
			assertThat(config(layer.getGeometryEncoder())).isEqualTo(
					Map.of("xProperty", "x", "yProperty", "y", "bboxProperty", "mbr"));
			assertThat(config(layer.getIndex())).isEqualTo(Map.of("splitMode", "quadratic",
					"maxNodeReferences", 20L, "shouldMergeTrees", false,
					"referenceRelationshipType", "RTREE_REFERENCE"));
		});
	}

	@Test
	void legacyLayerUsesMaxNodeReferencesFromRTreeMetadata() {
		inTx(tx -> {
			Node layerNode = legacyLayerNode(tx);
			Node metadata = tx.createNode();
			metadata.setProperty("maxNodeReferences", 42);
			layerNode.createRelationshipTo(metadata, RTreeRelationshipTypes.RTREE_METADATA);
		});
		inTx(tx -> {
			Layer layer = spatial.getLayer(tx, "legacy", false);
			assertThat(((RTreeIndex) layer.getIndex()).getConfiguration().maxNodeReferences).isEqualTo(42L);
		});
	}

	@Test
	void flattenedMaxNodeReferencesWinsOverRTreeMetadata() {
		inTx(tx -> spatial.createLayer(tx, "layer", SimplePointEncoder.class, EditableLayerImpl.class,
				LayerRTreeIndex.class, null, "{\"maxNodeReferences\":20}"));
		inTx(tx -> {
			Node metadata = tx.findNode(LABEL_LAYER, PROP_LAYER, "layer")
					.getSingleRelationship(RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING)
					.getEndNode();
			metadata.setProperty("maxNodeReferences", 42);
		});
		inTx(tx -> {
			Layer layer = spatial.getLayer(tx, "layer", true);
			assertThat(((RTreeIndex) layer.getIndex()).getConfiguration().maxNodeReferences).isEqualTo(20L);
		});
	}

	private static Node legacyLayerNode(Transaction tx) {
		Node layerNode = tx.createNode(LABEL_LAYER);
		layerNode.setProperty(PROP_LAYER, "legacy");
		layerNode.setProperty(PROP_GEOMENCODER, "SimplePointEncoder");
		layerNode.setProperty(PROP_GEOMENCODER_CONFIG, "x:y:mbr");
		layerNode.setProperty(PROP_INDEX_TYPE, "rtree");
		return layerNode;
	}

	private static Map<String, Object> prefixed(Node node, String prefix) {
		Map<String, Object> result = new HashMap<>();
		node.getAllProperties().forEach((key, value) -> {
			if (key.startsWith(prefix + ".")) {
				result.put(key.substring(prefix.length() + 1), value);
			}
		});
		return result;
	}

	private static Map<String, Object> config(Object component) {
		return ConfigurationUtils.getConfigMap((Configurable<?>) component);
	}
}
