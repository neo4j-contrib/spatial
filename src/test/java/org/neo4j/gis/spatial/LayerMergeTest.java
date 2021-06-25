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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.merge.MergeUtils;
import org.neo4j.gis.spatial.osm.OSMDataset;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.osm.OSMModel;
import org.neo4j.gis.spatial.rtree.ProgressLoggingListener;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class LayerMergeTest {
    private DatabaseManagementService databases;
    private GraphDatabaseService graphDb;
    private SpatialDatabaseService spatial;

    @Before
    public void setup() throws KernelException {
        databases = new TestDatabaseManagementServiceBuilder(new File("target/layers").toPath()).impermanent().build();
        graphDb = databases.database(DEFAULT_DATABASE_NAME);
        spatial = new SpatialDatabaseService(new IndexManager((GraphDatabaseAPI) graphDb, SecurityContext.AUTH_DISABLED));
    }

    @After
    public void teardown() {
        databases.shutdown();
    }

    @Test
    public void shouldMergeEmptyWKBLayers() {
        shouldMergeLayersWithGeometries(false, 0, 0, this::makeWKBLayer);
    }

    @Test
    public void shouldMergeWKBLayersWithGeometries() {
        shouldMergeLayersWithGeometries(false, 10, 8, this::makeWKBLayer);
    }

    @Test
    public void shouldMergeEmptyWKTLayers() {
        shouldMergeLayersWithGeometries(false, 0, 0, this::makeWKTLayer);
    }

    @Test
    public void shouldMergeWKTLayersWithGeometries() {
        shouldMergeLayersWithGeometries(false, 10, 8, this::makeWKTLayer);
    }

    @Test
    public void shouldMergeWKTLayersIntoWKBLayer() {
        shouldMergeLayersWithGeometries(false, 10, 8, this::makeWKBLayer, this::makeWKTLayer);
    }

    @Test
    public void shouldMergeWKBLayersIntoWKTLayer() {
        shouldMergeLayersWithGeometries(false, 10, 8, this::makeWKTLayer, this::makeWKBLayer);
    }

    @Test
    public void shouldMergeEmptyOSMLayers() {
        shouldMergeOSMLayersWithGeometries(false, 0, 0);
    }

    @Test
    public void shouldMergeOSMLayersWithOneGeometry() {
        shouldMergeOSMLayersWithGeometries(false, 1, 8);
    }

    @Test
    public void shouldMergeOSMLayersWithManyGeometries() {
        shouldMergeOSMLayersWithGeometries(false, 1000, 8);
    }

    @Test
    public void shouldMergeOSMLayersWithOneIdenticalGeometry() {
        shouldMergeOSMLayersWithGeometries(true, 1, 8);
    }

    @Test
    public void shouldMergeOSMLayersWithManyIdenticalGeometries() {
        shouldMergeOSMLayersWithGeometries(true, 1000, 8);
    }

    private Layer makeWKBLayer(Transaction tx, String layerName) {
        return spatial.createWKBLayer(tx, layerName);
    }

    private Layer makeWKTLayer(Transaction tx, String layerName) {
        return spatial.createLayer(tx, layerName, WKTGeometryEncoder.class, EditableLayerImpl.class);
    }

    private Layer makeOSMLayer(Transaction tx, String layerName) {
        return spatial.createLayer(tx, layerName, OSMGeometryEncoder.class, OSMLayer.class);
    }

    private void createOSMIndex(OSMDataset dataset, OSMDataset.LabelHasher labelHasher, String layerName, Label label, String propertyKey) {
        Label hashed = labelHasher.getLabelHashed(label);
        String indexName = OSMDataset.indexNameFor(layerName, hashed.name(), propertyKey);
        // TODO: We should also have tests that verify correct behavior with security-context
        try (Transaction indexTx = graphDb.beginTx()) {
            indexTx.schema().indexFor(hashed).on(propertyKey).withName(indexName).create();
            indexTx.commit();
        }
        try (Transaction tx = graphDb.beginTx()) {
            String indexKey = OSMDataset.indexKeyFor(label, propertyKey);
            Node node = dataset.getDatasetNode(tx);
            node.setProperty(indexKey, indexName);
            tx.commit();
        }
    }

    private OSMDataset prepareOSMDataset(String layerName, long layerNum, boolean identicalOSMIds) {
        OSMDataset dataset;
        try (Transaction tx = graphDb.beginTx()) {
            Layer layer = spatial.getLayer(tx, layerName);
            OSMGeometryEncoder encoder = (OSMGeometryEncoder) layer.getGeometryEncoder();
            if (!identicalOSMIds) {
                encoder.configure(tx, 100000 * layerNum, 10000 * layerNum, 1000 * layerNum);
            }
            dataset = OSMDataset.fromLayer(tx, (OSMLayer) layer);
            tx.commit();
        }
        return dataset;
    }

    private void postCreateOSM(LayerConfig config, Integer layerNum) {
        config.postCreate(layerNum);
    }

    private void postCreateNone(LayerConfig config, Integer layerNum) {
    }

    private class LayerConfig {
        protected boolean identicalLayers;
        protected String[] layerNames;

        private LayerConfig(boolean identicalLayers, String... layerNames) {
            this.identicalLayers = identicalLayers;
            this.layerNames = layerNames;
        }

        protected void postCreate(int layerNum) {
        }
    }

    private class OSMLayerConfig extends LayerConfig {
        private OSMLayerConfig(boolean identicalLayers, String... layerNames) {
            super(identicalLayers, layerNames);
        }

        @Override
        protected void postCreate(int layerNum) {
            try {
                OSMDataset dataset = prepareOSMDataset(layerNames[layerNum], layerNum, identicalLayers);
                OSMDataset.LabelHasher labelHasher = new OSMDataset.LabelHasher(layerNames[layerNum]);
                createOSMIndex(dataset, labelHasher, layerNames[layerNum], OSMModel.LABEL_NODE, OSMModel.PROP_NODE_ID);
                createOSMIndex(dataset, labelHasher, layerNames[layerNum], OSMModel.LABEL_WAY, OSMModel.PROP_WAY_ID);
                createOSMIndex(dataset, labelHasher, layerNames[layerNum], OSMModel.LABEL_RELATION, OSMModel.PROP_RELATION_ID);
                try (Transaction indexTx = graphDb.beginTx()) {
                    indexTx.schema().awaitIndexesOnline(10, TimeUnit.SECONDS);
                    indexTx.commit();
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to created OSM indexes: " + e.getMessage(), e);
            }
        }
    }

    private final LayerConfig DEFAULT = new LayerConfig(false, "testA", "testB", "testC", "testD");

    @SuppressWarnings("SameParameterValue")
    private void shouldMergeLayersWithGeometries(boolean identicalLayers, int numGeoms, int geomLength, BiFunction<Transaction, String, Layer> layerMaker) {
        shouldMergeLayersWithGeometries(identicalLayers, numGeoms, geomLength, layerMaker, layerMaker, this::postCreateNone, DEFAULT);
    }

    @SuppressWarnings("SameParameterValue")
    private void shouldMergeOSMLayersWithGeometries(boolean identicalLayers, int numGeoms, int geomLength) {
        BiFunction<Transaction, String, Layer> layerMaker = this::makeOSMLayer;
        BiConsumer<LayerConfig, Integer> postCreate = this::postCreateOSM;
        LayerConfig config = new OSMLayerConfig(identicalLayers, DEFAULT.layerNames);
        shouldMergeLayersWithGeometries(identicalLayers, numGeoms, geomLength, layerMaker, layerMaker, postCreate, config);
    }

    @SuppressWarnings("SameParameterValue")
    private void shouldMergeLayersWithGeometries(boolean identicalLayers, int numGeoms, int geomLength, BiFunction<Transaction, String, Layer> mainMaker, BiFunction<Transaction, String, Layer> otherMaker) {
        shouldMergeLayersWithGeometries(identicalLayers, numGeoms, geomLength, mainMaker, otherMaker, this::postCreateNone, DEFAULT);
    }

    private void shouldMergeLayersWithGeometries(boolean identicalLayers, int numGeoms, int geomLength, BiFunction<Transaction, String, Layer> mainMaker, BiFunction<Transaction, String, Layer> otherMaker, BiConsumer<LayerConfig, Integer> postCreate, LayerConfig layerConfig) {
        double scale = 0.01;
        boolean verbose = numGeoms < 10;
        ArrayList<ArrayList<Geometry>> allAdded = new ArrayList<>();
        for (int l = 0; l < layerConfig.layerNames.length; l++) {
            String layerName = layerConfig.layerNames[l];

            // First create an empty layer
            BiFunction<Transaction, String, Layer> layerMaker = l == 0 ? mainMaker : otherMaker;
            inTx(tx -> layerMaker.apply(tx, layerName));

            // Some models (OSM) need a special post-create setup. It cannot happen in the create step, because it makes Neo4j indexes which have to happen in a different transaction
            postCreate.accept(layerConfig, l);

            // Now populate the layer with sample data
            ArrayList<Geometry> geometries = new ArrayList<>();
            if (numGeoms > 0) {
                double x = identicalLayers ? 0 : scale * scale * l;
                double y = identicalLayers ? 0 : scale * scale * l;
                inTx(tx -> {
                    Layer layer = spatial.getLayer(tx, layerName);
                    assertNotNull(layer);
                    assertThat(format("Layer %s should be editable", layerName), layer instanceof EditableLayer);
                    EditableLayer editable = (EditableLayer) layer;
                    for (int i = 0; i < numGeoms; i++) {
                        Coordinate[] coordinates = new Coordinate[geomLength];
                        for (int j = 0; j < geomLength; j++) {
                            // Make a horizontal lineString, offset by both the layer number and the geometry number
                            coordinates[j] = new CoordinateXY(x + scale * j, y + scale * i);
                        }
                        Geometry geometry = layer.getGeometryFactory().createLineString(coordinates);
                        geometries.add(geometry);
                        editable.add(tx, geometry);
                    }
                });
            }
            allAdded.add(geometries);

            // Finally check that the layer has the number of geometries that were added
            inTx(tx -> {
                Layer layer = spatial.getLayer(tx, layerName);
                int count = layer.getIndex().count(tx);
                assertThat(format("Expected layer %s to have %d geometries, but found %d", layerName, numGeoms, count), count == numGeoms);
            });
        }

        // Test that all layers can be merged into the first layer, with all geometries moved over
        for (int i = 1; i < layerConfig.layerNames.length; i++) {
            String mainName = layerConfig.layerNames[0];
            String fromName = layerConfig.layerNames[i];
            inTx(tx -> {
                EditableLayer main = (EditableLayer) spatial.getLayer(tx, mainName);
                EditableLayer from = (EditableLayer) spatial.getLayer(tx, fromName);
                IndexStateCapture other = new IndexStateCapture(tx, from);
                IndexStateCapture before = new IndexStateCapture(tx, main);
                long merged = MergeUtils.mergeLayerInto(tx, main, from);
                IndexStateCapture after = new IndexStateCapture(tx, main);
                System.out.printf("Before merge %s->%s there were %d geometries and after merge there were %d%n", fromName, mainName, before.size(), after.size());
                if (verbose) {
                    before.debug("before");
                    other.debug("other");
                    after.debug("after");
                }
                // OSM does a more complex merge and generates more changes than other storage layers
                long expected = identicalLayers ? 0 : (main instanceof OSMLayer) ? (long) numGeoms * (geomLength + 1) : numGeoms;
                assertThat(format("Expected to merge %d geometries into %s from %s, but merged %d", numGeoms, mainName, fromName, merged), merged == expected);
            });
        }

        // After completing the merger, test all layers have the expected geometry counts (all geoms in first layer, and none in the others)
        for (int i = 0; i < layerConfig.layerNames.length; i++) {
            String layerName = layerConfig.layerNames[i];
            int expected = i == 0 ? (identicalLayers ? 1 : layerConfig.layerNames.length) * numGeoms : 0;
            inTx(tx -> {
                Layer layer = spatial.getLayer(tx, layerName);
                int count = layer.getIndex().count(tx);
                if (count != expected) {
                    if (expected != 0) {
                        System.out.printf("Expected layer %s to have %d geometries, but found %d", layerName, expected, count);
                        IndexStateCapture found = new IndexStateCapture(tx, layer);
                        for (int l = 0; l < allAdded.size(); l++) {
                            found.validate(layerConfig.layerNames[l], allAdded.get(l));
                        }
                    }
                }
                assertThat(format("Expected layer %s to have %d geometries, but found %d", layerName, expected, count), count == expected);
            });
        }

        // Cleanup (delete layers)
        for (String layerName : layerConfig.layerNames) {
            inTx(tx -> spatial.deleteLayer(tx, layerName, new ProgressLoggingListener("deleting layer '" + layerName + "'", System.out)));
            inTx(tx -> assertNull(spatial.getLayer(tx, layerName)));
        }
    }

    private static class IndexStateCapture {
        ArrayList<Geometry> geometries = new ArrayList<>();
        ArrayList<Node> geomNodes = new ArrayList<>();

        private IndexStateCapture(Transaction tx, Layer layer) {
            for (Node node : layer.getIndex().getAllIndexedNodes(tx)) {
                geomNodes.add(node);
                geometries.add(layer.getGeometryEncoder().decodeGeometry(node));
            }
        }

        private boolean contains(Geometry geometry) {
            return geometries.contains(geometry);
        }

        private int size() {
            return geomNodes.size();
        }

        public void validate(String layerName, ArrayList<Geometry> added) {
            for (Geometry expectedGeometry : added) {
                if (!contains(expectedGeometry)) {
                    System.out.printf("\tFailed to find expected geometry '%s' from layer %s%n", expectedGeometry, layerName);
                }
            }
        }

        public void debug(String title) {
            System.out.printf("Index has %d nodes and geometries: %s%n", size(), title);
            for (int i = 0; i < size(); i++) {
                System.out.printf("\t%s: %s%n", geomNodes.get(i), geometries.get(i));
            }
        }
    }

    private void inTx(Consumer<Transaction> txFunction) {
        try (Transaction tx = graphDb.beginTx()) {
            txFunction.accept(tx);
            tx.commit();
        }
    }
}
