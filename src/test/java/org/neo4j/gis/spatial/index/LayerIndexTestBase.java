/*
 * Copyright (c) 2010-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.gis.spatial.index;

import org.locationtech.jts.geom.*;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.gis.spatial.*;
import org.neo4j.gis.spatial.filter.SearchIntersect;
import org.neo4j.gis.spatial.filter.SearchIntersectWindow;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.rtree.NullListener;
import org.neo4j.gis.spatial.rtree.filter.SearchResults;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class LayerIndexTestBase {

    protected GraphDatabaseService graph;
    protected SpatialDatabaseService spatial;
    protected GeometryFactory geometryFactory = new GeometryFactory();
    protected GeometryEncoder encoder = makeGeometryEncoder();

    protected abstract Class<? extends LayerIndexReader> getIndexClass();

    protected abstract Class<? extends GeometryEncoder> getEncoderClass();

    protected abstract LayerIndexReader makeIndex();

    protected abstract GeometryEncoder makeGeometryEncoder();

    protected SpatialIndexWriter mockLayerIndex() {
        Layer layer = mockLayer();
        LayerIndexReader index = makeIndex();
        try (Transaction tx = graph.beginTx()) {
            index.init(layer);
            tx.success();
        }
        when(layer.getIndex()).thenReturn(index);
        return (SpatialIndexWriter)index;
    }

    protected Layer mockLayer() {
        Node layerNode;
        try (Transaction tx = graph.beginTx()) {
            layerNode = graph.createNode();
            tx.success();
        }
        Layer layer = mock(Layer.class);
        when(layer.getSpatialDatabase()).thenReturn(spatial);
        when(layer.getGeometryEncoder()).thenReturn(encoder);
        when(layer.getLayerNode()).thenReturn(layerNode);
        when(layer.getGeometryFactory()).thenReturn(geometryFactory);
        when(layer.getCoordinateReferenceSystem()).thenReturn(DefaultGeographicCRS.WGS84);
        return layer;
    }

    protected void addSimplePoint(SpatialIndexWriter index, double x, double y) {
        try (Transaction tx = graph.beginTx()) {
            Node geomNode = graph.createNode();
            Point point = geometryFactory.createPoint(new Coordinate(x, y));
            geomNode.setProperty("x", x);//TODO Remove these?
            geomNode.setProperty("y", y);
            encoder.encodeGeometry(point, geomNode);
            index.add(geomNode);
            tx.success();
        }
    }

    @Before
    public void setup() {
        graph = new TestGraphDatabaseFactory().newImpermanentDatabase();
        spatial = new SpatialDatabaseService(graph);
    }

    @After
    public void tearDown() {
        if (graph != null) {
            graph.shutdown();
            graph = null;
            spatial = null;
        }
    }

    @Test
    public void shouldCreateAndFindIndexViaLayer() {
        SimplePointLayer layer = spatial.createPointLayer("test", getIndexClass(), getEncoderClass());
        LayerIndexReader index = layer.getIndex();
        assertThat("Should find the same index", index.getLayer().getName(), equalTo(spatial.getLayer("test").getName()));
        assertThat("Index should be of right type", spatial.getLayer("test").getIndex().getClass(), equalTo(getIndexClass()));
    }

    @Test
    public void shouldCreateAndFindAndDeleteIndexViaLayer() {
        spatial.createPointLayer("test", getIndexClass(), getEncoderClass());
        Layer layer = spatial.getLayer("test");
        LayerIndexReader index = layer.getIndex();
        assertThat("Should find the same index", index.getLayer().getName(), equalTo(spatial.getLayer("test").getName()));
        assertThat("Index should be of right type", spatial.getLayer("test").getIndex().getClass(), equalTo(getIndexClass()));
        try (Transaction tx = graph.beginTx()) {
            layer.delete(new NullListener());
            layer = spatial.getLayer("test");
            tx.success();
        }
        assertThat("Expected no layer to be found", layer, is(nullValue()));
    }

    @Test
    public void shouldFindNodeAddedToIndexViaLayer() {
        SimplePointLayer layer = spatial.createPointLayer("test", getIndexClass(), getEncoderClass());
        SpatialDatabaseRecord added = layer.add(1.0, 1.0);
        try (Transaction tx = graph.beginTx()) {
            List<GeoPipeFlow> found = layer.findClosestPointsTo(new Coordinate(1.0, 1.0), 0.5);
            assertThat("Should find one geometry node", found.size(), equalTo(1));
            assertThat("Should find same geometry node", added.getGeomNode(), equalTo(found.get(0).getGeomNode()));
            tx.success();
        }
    }

    @Test
    public void shouldFindNodeAddedDirectlyToIndex() {
        SpatialIndexWriter index = mockLayerIndex();
        addSimplePoint(index, 1.0, 1.0);
        try (Transaction tx = graph.beginTx()) {
            SearchResults results = index.searchIndex(new SearchIntersectWindow(((LayerIndexReader)index).getLayer(), new Envelope(0.0, 2.0, 0.0, 2.0)));
            List<Node> nodes = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
            assertThat("Index should contain one result", nodes.size(), equalTo(1));
            assertThat("Should find correct Geometry", encoder.decodeGeometry(nodes.get(0)), equalTo(geometryFactory.createPoint(new Coordinate(1.0, 1.0))));
            tx.success();
        }
    }

    @Test
    public void shouldFindOnlyOneOfTwoNodesAddedDirectlyToIndex() {
        SpatialIndexWriter index = mockLayerIndex();
        addSimplePoint(index, 10.0, 10.0);
        addSimplePoint(index, 1.0, 1.0);
        try (Transaction tx = graph.beginTx()) {
            SearchResults results = index.searchIndex(new SearchIntersectWindow(((LayerIndexReader)index).getLayer(), new Envelope(0.0, 2.0, 0.0, 2.0)));
            List<Node> nodes = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
            assertThat("Index should contain one result", nodes.size(), equalTo(1));
            assertThat("Should find correct Geometry", encoder.decodeGeometry(nodes.get(0)), equalTo(geometryFactory.createPoint(new Coordinate(1.0, 1.0))));
            tx.success();
        }
    }

    private Polygon makeTestPolygonInSquare(GeometryFactory geometryFactory, int length) {
        if (length < 4) {
            throw new IllegalArgumentException("Cannot create letter C in square smaller than 4x4");
        }
        int maxDim = length - 1;
        LinearRing shell = geometryFactory.createLinearRing(new Coordinate[]{
                new Coordinate(0, 1),
                new Coordinate(0, maxDim - 1),
                new Coordinate(1, maxDim),
                new Coordinate(maxDim, maxDim),
                new Coordinate(maxDim, maxDim - 1),
                new Coordinate(1, maxDim - 1),
                new Coordinate(1, 1),
                new Coordinate(maxDim, 1),
                new Coordinate(maxDim, 0),
                new Coordinate(1, 0),
                new Coordinate(0, 1)
        });
        Polygon polygon = geometryFactory.createPolygon(shell);
        return polygon;
    }

    @Test
    public void shouldFindCorrectSetOfNodesInsideAndOnPolygonEdge() {
        int length = 5;  // make 5x5 square to test on
        SimplePointLayer layer = spatial.createPointLayer("test", getIndexClass(), getEncoderClass());
        GeometryFactory geometryFactory = layer.getGeometryFactory();
        Polygon polygon = makeTestPolygonInSquare(geometryFactory, length);
        HashSet<Coordinate> notIncluded = new LinkedHashSet<>();
        HashSet<Coordinate> included = new LinkedHashSet<>();
        for (int x = 0; x < length; x++) {
            for (int y = 0; y < length; y++) {
                Coordinate coordinate = new Coordinate(x, y);
                layer.add(coordinate);
                Geometry point = geometryFactory.createPoint(coordinate);
                if (polygon.intersects(point)) {
                    included.add(coordinate);
                } else {
                    notIncluded.add(coordinate);
                }
            }
        }
        try (Transaction tx = graph.beginTx()) {
            SearchResults results = layer.getIndex().searchIndex(new SearchIntersect(layer, polygon));
            Set<Coordinate> found = StreamSupport.stream(results.spliterator(), false).map(n ->
                    layer.getGeometryEncoder().decodeGeometry(n).getCoordinate()
            ).collect(Collectors.toSet());
            assertThat("Index should contain expected number of results", found.size(), equalTo(included.size()));
            assertThat("Should find correct Geometries", found, equalTo(included));
            for (Coordinate shouldNotBeFound : notIncluded) {
                assertThat("Point should not have been found", found, not(hasItem(shouldNotBeFound)));
            }
            tx.success();
        }
    }
}
