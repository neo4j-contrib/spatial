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
package org.neo4j.gis.spatial.index;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.mockito.Mockito;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gis.spatial.GeometryEncoder;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SimplePointLayer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.filter.SearchIntersect;
import org.neo4j.gis.spatial.filter.SearchIntersectWindow;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.rtree.NullListener;
import org.neo4j.gis.spatial.rtree.filter.SearchResults;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public abstract class LayerIndexTestBase {

	protected DatabaseManagementService databases;
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
			index.init(tx, spatial.indexManager, layer);
			tx.commit();
		}
		when(layer.getIndex()).thenReturn(index);
		return (SpatialIndexWriter) index;
	}

	protected Layer mockLayer() {
		Node layerNode;
		try (Transaction tx = graph.beginTx()) {
			layerNode = tx.createNode();
			tx.commit();
		}
		Layer layer = mock(Layer.class);
		when(layer.getName()).thenReturn("test");
		when(layer.getGeometryEncoder()).thenReturn(encoder);
		when(layer.getLayerNode(Mockito.any(Transaction.class))).thenReturn(layerNode);
		when(layer.getGeometryFactory()).thenReturn(geometryFactory);
		when(layer.getCoordinateReferenceSystem(Mockito.any(Transaction.class))).thenReturn(DefaultGeographicCRS.WGS84);
		return layer;
	}

	protected void addSimplePoint(SpatialIndexWriter index, double x, double y) {
		try (Transaction tx = graph.beginTx()) {
			Node geomNode = tx.createNode();
			Point point = geometryFactory.createPoint(new Coordinate(x, y));
			encoder.encodeGeometry(tx, point, geomNode);
			index.add(tx, geomNode);
			tx.commit();
		}
	}

	@BeforeEach
	public void setup() throws IOException {
		File baseDir = new File("target/layers");
		FileUtils.deleteDirectory(baseDir.toPath());
		databases = new TestDatabaseManagementServiceBuilder(baseDir.toPath()).impermanent().build();
		graph = databases.database(DEFAULT_DATABASE_NAME);
		spatial = new SpatialDatabaseService(new IndexManager((GraphDatabaseAPI) graph, SecurityContext.AUTH_DISABLED));
	}

	@AfterEach
	public void tearDown() {
		if (graph != null) {
			databases.shutdown();
			databases = null;
			graph = null;
			spatial = null;
		}
	}

	@Test
	public void shouldCreateAndFindIndexViaLayer() {
		SimplePointLayer layer = makeTestPointLayer();
		LayerIndexReader index = layer.getIndex();
		try (Transaction tx = graph.beginTx()) {
			assertThat("Should find the same index", index.getLayer().getName(),
					equalTo(spatial.getLayer(tx, "test").getName()));
			assertThat("Index should be of right type", spatial.getLayer(tx, "test").getIndex().getClass(),
					equalTo(getIndexClass()));
		}
	}

	@Test
	public void shouldCreateAndFindAndDeleteIndexViaLayer() {
		Layer layer = makeTestPointLayer();
		LayerIndexReader index = layer.getIndex();
		try (Transaction tx = graph.beginTx()) {
			assertThat("Should find the same index", index.getLayer().getName(),
					equalTo(spatial.getLayer(tx, "test").getName()));
			assertThat("Index should be of right type", spatial.getLayer(tx, "test").getIndex().getClass(),
					equalTo(getIndexClass()));
		}
		try (Transaction tx = graph.beginTx()) {
			layer.delete(tx, new NullListener());
			layer = spatial.getLayer(tx, "test");
			tx.commit();
		}
		assertThat("Expected no layer to be found", layer, is(nullValue()));
	}

	private SimplePointLayer makeTestPointLayer() {
		try (Transaction tx = graph.beginTx()) {
			SimplePointLayer layer = spatial.createPointLayer(tx, "test", getIndexClass(), getEncoderClass(), null);
			tx.commit();
			return layer;
		}
	}

	@Test
	public void shouldFindNodeAddedToIndexViaLayer() {
		SimplePointLayer layer = makeTestPointLayer();
		SpatialDatabaseRecord added;
		try (Transaction tx = graph.beginTx()) {
			added = layer.add(tx, 1.0, 1.0);
			tx.commit();
		}
		try (Transaction tx = graph.beginTx()) {
			List<GeoPipeFlow> found = layer.findClosestPointsTo(tx, new Coordinate(1.0, 1.0), 0.5);
			assertThat("Should find one geometry node", found.size(), equalTo(1));
			assertThat("Should find same geometry node", added.getGeomNode(), equalTo(found.get(0).getGeomNode()));
			tx.commit();
		}
	}

	@Test
	public void shouldFindNodeAddedDirectlyToIndex() {
		SpatialIndexWriter index = mockLayerIndex();
		addSimplePoint(index, 1.0, 1.0);
		try (Transaction tx = graph.beginTx()) {
			SearchResults results = index.searchIndex(tx,
					new SearchIntersectWindow(((LayerIndexReader) index).getLayer(), new Envelope(0.0, 2.0, 0.0, 2.0)));
			List<Node> nodes = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
			assertThat("Index should contain one result", nodes.size(), equalTo(1));
			assertThat("Should find correct Geometry", encoder.decodeGeometry(nodes.get(0)),
					equalTo(geometryFactory.createPoint(new Coordinate(1.0, 1.0))));
			tx.commit();
		}
	}

	@Test
	public void shouldFindOnlyOneOfTwoNodesAddedDirectlyToIndex() {
		SpatialIndexWriter index = mockLayerIndex();
		addSimplePoint(index, 10.0, 10.0);
		addSimplePoint(index, 1.0, 1.0);
		try (Transaction tx = graph.beginTx()) {
			SearchResults results = index.searchIndex(tx,
					new SearchIntersectWindow(((LayerIndexReader) index).getLayer(), new Envelope(0.0, 2.0, 0.0, 2.0)));
			List<Node> nodes = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
			assertThat("Index should contain one result", nodes.size(), equalTo(1));
			assertThat("Should find correct Geometry", encoder.decodeGeometry(nodes.get(0)),
					equalTo(geometryFactory.createPoint(new Coordinate(1.0, 1.0))));
			tx.commit();
		}
	}

	private static Polygon makeTestPolygonInSquare(GeometryFactory geometryFactory, int length) {
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
		return geometryFactory.createPolygon(shell);
	}

	@Test
	public void shouldFindCorrectSetOfNodesInsideAndOnPolygonEdge() {
		int length = 5;  // make 5x5 square to test on
		SimplePointLayer layer = makeTestPointLayer();
		GeometryFactory geometryFactory = layer.getGeometryFactory();
		Polygon polygon = makeTestPolygonInSquare(geometryFactory, length);
		HashSet<Coordinate> notIncluded = new LinkedHashSet<>();
		HashSet<Coordinate> included = new LinkedHashSet<>();
		for (int x = 0; x < length; x++) {
			for (int y = 0; y < length; y++) {
				try (Transaction tx = graph.beginTx()) {
					Coordinate coordinate = new Coordinate(x, y);
					layer.add(tx, coordinate);
					Geometry point = geometryFactory.createPoint(coordinate);
					if (polygon.intersects(point)) {
						included.add(coordinate);
					} else {
						notIncluded.add(coordinate);
					}
					tx.commit();
				}
			}
		}
		try (Transaction tx = graph.beginTx()) {
			SearchResults results = layer.getIndex().searchIndex(tx, new SearchIntersect(layer, polygon));
			Set<Coordinate> found = StreamSupport.stream(results.spliterator(), false).map(n ->
					layer.getGeometryEncoder().decodeGeometry(n).getCoordinate()
			).collect(Collectors.toSet());
			assertThat("Index should contain expected number of results", found.size(), equalTo(included.size()));
			assertThat("Should find correct Geometries", found, equalTo(included));
			for (Coordinate shouldNotBeFound : notIncluded) {
				assertThat("Point should not have been found", found, not(hasItem(shouldNotBeFound)));
			}
			tx.commit();
		}
	}
}
