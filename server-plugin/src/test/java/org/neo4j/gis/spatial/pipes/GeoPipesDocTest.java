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
package org.neo4j.gis.spatial.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.geotools.api.style.Style;
import org.geotools.data.neo4j.Neo4jFeatureBuilder;
import org.geotools.data.neo4j.StyledImageExporter;
import org.geotools.feature.FeatureCollection;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.annotations.documented.Documented;
import org.neo4j.doc.tools.JavaTestDocsGenerator;
import org.neo4j.gis.spatial.AbstractJavaDocTestBase;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.EditableLayerImpl;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.filter.SearchIntersectWindow;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.pipes.filtering.FilterCQL;
import org.neo4j.gis.spatial.pipes.osm.OSMGeoPipeline;
import org.neo4j.gis.spatial.rtree.filter.SearchAll;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestData.Title;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class GeoPipesDocTest extends AbstractJavaDocTestBase {

	private static Layer osmLayer;
	private static EditableLayerImpl boxesLayer;
	private static EditableLayerImpl concaveLayer;
	private static EditableLayerImpl intersectionLayer;
	private static EditableLayerImpl equalLayer;
	private static EditableLayerImpl linesLayer;
	private Transaction tx;

	@Test
	public void find_all() {
		int count = 0;
		for (GeoPipeFlow flow : GeoPipeline.start(tx, osmLayer).createWellKnownText()) {
			count++;

			assertEquals(1, flow.getProperties().size());
			String wkt = (String) flow.getProperties().get("WellKnownText");
			assertEquals(0, wkt.indexOf("LINESTRING"));
		}

		assertEquals(2, count);
	}

	@Test
	public void filter_by_osm_attribute() {
		GeoPipeline pipeline = OSMGeoPipeline.startOsm(tx, osmLayer)
				.osmAttributeFilter("name", "Storgatan")
				.copyDatabaseRecordProperties(tx);

		GeoPipeFlow flow = pipeline.next();
		assertFalse(pipeline.hasNext());

		assertEquals("Storgatan", flow.getProperties().get("name"));
	}

	@Test
	public void filter_by_property() {
		GeoPipeline pipeline = GeoPipeline.start(tx, osmLayer)
				.copyDatabaseRecordProperties(tx, "name")
				.propertyFilter("name", "Storgatan");

		GeoPipeFlow flow = pipeline.next();
		assertFalse(pipeline.hasNext());

		assertEquals("Storgatan", flow.getProperties().get("name"));
	}

	@Test
	public void filter_by_window_intersection() {
		assertEquals(1, GeoPipeline.start(tx, osmLayer).windowIntersectionFilter(10, 40, 20, 56.0583531).count());
	}

	@Test
	@Title("Filter by cql using bbox")
	@Documented("""
			This pipe is filtering according to a CQL Bounding Box description.

			Example:
			@@s_filter_by_cql_using_bbox
			""")
	public void filter_by_cql_using_bbox() throws CQLException {
		// tag::s_filter_by_cql_using_bbox[]
		GeoPipeline cqlFilter = GeoPipeline.start(tx, osmLayer).cqlFilter(tx, "BBOX(the_geom, 10, 40, 20, 56.0583531)");
		// end::s_filter_by_cql_using_bbox[]
		assertEquals(1, cqlFilter.count());
	}

	@Test
	@Title("Search within geometry")
	@Documented("""
			This pipe performs a search within a geometry in this example,
			both OSM street geometries should be found in when searching with
			an enclosing rectangle Envelope.

			Example:
			@@s_search_within_geometry
			""")
	public void search_within_geometry() throws CQLException {
		// tag::s_search_within_geometry[]
		GeoPipeline pipeline = GeoPipeline
				.startWithinSearch(tx, osmLayer,
						osmLayer.getGeometryFactory().toGeometry(new Envelope(10, 20, 50, 60)));
		// end::s_search_within_geometry[]
		assertEquals(2, pipeline.count());
	}

	@Test
	public void filter_by_cql_using_property() throws CQLException {
		GeoPipeline pipeline = GeoPipeline.start(tx, osmLayer).cqlFilter(tx, "name = 'Storgatan'")
				.copyDatabaseRecordProperties(tx);

		GeoPipeFlow flow = pipeline.next();
		assertFalse(pipeline.hasNext());

		assertEquals("Storgatan", flow.getProperties().get("name"));
	}

	@Test
	@Title("Filter by cql using complex cql")
	@Documented("""
			This pipe is filtering according to a complex CQL description.

			Example:
			@@s_filter_by_cql_using_complex_cql
			""")
	public void filter_by_cql_using_complex_cql() throws CQLException {
		// tag::s_filter_by_cql_using_complex_cql[]
		long counter = GeoPipeline.start(tx, osmLayer)
				.cqlFilter(tx, "highway is not null and geometryType(the_geom) = 'LineString'").count();
		// end::s_filter_by_cql_using_complex_cql[]

		FilterCQL filter = new FilterCQL(tx, osmLayer, "highway is not null and geometryType(the_geom) = 'LineString'");
		filter.setStarts(GeoPipeline.start(tx, osmLayer));
		assertTrue(filter.hasNext());
		while (filter.hasNext()) {
			filter.next();
			counter--;
		}
		assertEquals(0, counter);
	}

	@Test
	@Title("Affine Transformation")
	@Documented("""
			This pipe applies an affine transformation to every geometry.

			Example:
			@@s_affine_transformation

			Output:
			@@affine_transformation
			""")
	public void translate_geometries() {
		// tag::s_affine_transformation[]
		GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer)
				.applyAffineTransform(AffineTransformation.translationInstance(2, 3));
		// end::s_affine_transformation[]
		addImageSnippet(boxesLayer, pipeline, getTitle());

		GeoPipeline original = GeoPipeline.start(tx, osmLayer).copyDatabaseRecordProperties(tx).sort(
				"name");

		GeoPipeline translated = GeoPipeline.start(tx, osmLayer).applyAffineTransform(
				AffineTransformation.translationInstance(10, 25)).copyDatabaseRecordProperties(tx).sort(
				"name");

		for (int k = 0; k < 2; k++) {
			Coordinate[] coords = original.next().getGeometry().getCoordinates();
			Coordinate[] newCoords = translated.next().getGeometry().getCoordinates();
			assertEquals(coords.length, newCoords.length);
			for (int i = 0; i < coords.length; i++) {
				assertEquals(coords[i].x + 10, newCoords[i].x, 0);
				assertEquals(coords[i].y + 25, newCoords[i].y, 0);
			}
		}
	}

	@Test
	public void calculate_area() {
		GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).calculateArea().sort("Area");

		assertEquals((Double) pipeline.next().getProperties().get("Area"), 1.0, 0);
		assertEquals((Double) pipeline.next().getProperties().get("Area"), 8.0, 0);
		pipeline.reset();
	}

	@Test
	public void calculate_length() {
		GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).calculateLength().sort("Length");

		assertEquals((Double) pipeline.next().getProperties().get("Length"), 4.0, 0);
		assertEquals((Double) pipeline.next().getProperties().get("Length"), 12.0, 0);
		pipeline.reset();
	}

	@Test
	public void get_boundary_length() {
		GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).toBoundary().createWellKnownText().calculateLength()
				.sort("Length");

		GeoPipeFlow first = pipeline.next();
		GeoPipeFlow second = pipeline.next();
		assertEquals("LINEARRING (12 26, 12 27, 13 27, 13 26, 12 26)", first.getProperties().get("WellKnownText"));
		assertEquals("LINEARRING (2 3, 2 5, 6 5, 6 3, 2 3)", second.getProperties().get("WellKnownText"));
		assertEquals((Double) first.getProperties().get("Length"), 4.0, 0);
		assertEquals((Double) second.getProperties().get("Length"), 12.0, 0);
		pipeline.reset();
	}

	@Test
	@Title("Buffer")
	@Documented("""
			This pipe applies a buffer to geometries.

			Example:
			@@s_buffer

			Output:
			@@buffer
			""")
	public void get_buffer() {
		// tag::s_buffer[]
		GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).toBuffer(0.5);
		// end::s_buffer[]
		addImageSnippet(boxesLayer, pipeline, getTitle());

		pipeline = GeoPipeline.start(tx, boxesLayer).toBuffer(0.1).createWellKnownText().calculateArea().sort("Area");

		assertTrue(((Double) pipeline.next().getProperties().get("Area")) > 1);
		assertTrue(((Double) pipeline.next().getProperties().get("Area")) > 8);
		pipeline.reset();
	}

	@Test
	@Title("Centroid")
	@Documented("""
			This pipe calculates geometry centroid.

			Example:
			@@s_centroid

			Output:
			@@centroid
			""")
	public void get_centroid() {
		// tag::s_centroid[]
		GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).toCentroid();
		// end::s_centroid[]
		addImageSnippet(boxesLayer, pipeline, getTitle(), Constants.GTYPE_POINT);

		pipeline = GeoPipeline.start(tx, boxesLayer).toCentroid().createWellKnownText().copyDatabaseRecordProperties(tx)
				.sort("name");

		assertEquals("POINT (12.5 26.5)", pipeline.next().getProperties().get("WellKnownText"));
		assertEquals("POINT (4 4)", pipeline.next().getProperties().get("WellKnownText"));
		pipeline.reset();
	}

	@Test
	@Title("Export to GML")
	@Documented("""
			This pipe exports every geometry as a http://en.wikipedia.org/wiki/Geography_Markup_Language[GML] snippet.

			Example:
			@@s_export_to_gml

			Output:
			@@exportgml
			""")
	public void export_to_GML() {
		// tag::s_export_to_gml[]
		GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).createGML();
		for (GeoPipeFlow flow : pipeline) {
			System.out.println(flow.getProperties().get("GML"));
		}
		// end::s_export_to_gml[]
		StringBuilder result = new StringBuilder();
		for (GeoPipeFlow flow : GeoPipeline.start(tx, boxesLayer).createGML()) {
			result.append(flow.getProperties().get("GML"));
		}
		gen.get().addSnippet("exportgml", "[source,xml]\n----\n" + result + "\n----\n");
	}

	@Test
	@Title("Convex Hull")
	@Documented("""
			This pipe calculates geometry convex hull.

			Example:
			@@s_convex_hull

			Output:
			@@convex_hull
			""")
	public void get_convex_hull() {
		// tag::s_convex_hull[]
		GeoPipeline pipeline = GeoPipeline.start(tx, concaveLayer).toConvexHull();
		// end::s_convex_hull[]
		addImageSnippet(concaveLayer, pipeline, getTitle());

		pipeline = GeoPipeline.start(tx, concaveLayer).toConvexHull().createWellKnownText();

		assertEquals("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", pipeline.next().getProperties().get("WellKnownText"));
		pipeline.reset();
	}

	@Test
	@Title("Densify")
	@Documented("""
			This pipe inserts extra vertices along the line segments in the geometry.
			The densified geometry contains no line segment which is longer than the given distance tolerance.

			Example:
			@@s_densify

			Output:
			@@densify
			""")
	public void densify() {
		// tag::s_densify[]
		GeoPipeline pipeline = GeoPipeline.start(tx, concaveLayer).densify(5).extractPoints();
		// end::s_densify[]
		addImageSnippet(concaveLayer, pipeline, getTitle(), Constants.GTYPE_POINT);

		pipeline = GeoPipeline.start(tx, concaveLayer).toConvexHull().densify(5).createWellKnownText();

		String wkt = (String) pipeline.next().getProperties().get("WellKnownText");
		pipeline.reset();
		assertEquals("POLYGON ((0 0, 0 5, 0 10, 5 10, 10 10, 10 5, 10 0, 5 0, 0 0))", wkt);
	}

	@Test
	public void json() {
		GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).createJson().copyDatabaseRecordProperties(tx)
				.sort("name");

		assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[12,26],[12,27],[13,27],[13,26],[12,26]]]}",
				pipeline.next().getProperties().get("GeoJSON"));
		assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[2,3],[2,5],[6,5],[6,3],[2,3]]]}",
				pipeline.next().getProperties().get("GeoJSON"));
		pipeline.reset();
	}

	@Test
	@Title("Max")
	@Documented("""
			The Max pipe computes the maximum value of the specified property and discard items with a value less than the maximum.

			Example:
			@@s_max

			Output:
			@@max
			""")
	public void get_max_area() {
		// tag::s_max[]
		GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer)
				.calculateArea()
				.getMax("Area");
		// end::s_max[]
		addImageSnippet(boxesLayer, pipeline, getTitle());

		pipeline = GeoPipeline.start(tx, boxesLayer).calculateArea().getMax("Area");
		assertEquals((Double) pipeline.next().getProperties().get("Area"), 8.0, 0);
		pipeline.reset();
	}

	@Test
	@Title("Boundary")
	@Documented("""
			The boundary pipe calculates boundary of every geometry in the pipeline.

			Example:
			@@s_boundary

			Output:
			@@boundary
			""")
	public void boundary() {
		// tag::s_boundary[]
		GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).toBoundary();
		// end::s_boundary[]
		addImageSnippet(boxesLayer, pipeline, getTitle(), Constants.GTYPE_LINESTRING);

		// TODO test?
	}

	@Test
	@Title("Difference")
	@Documented("""
			The Difference pipe computes a geometry representing the points making up item geometry that do not make up the given geometry.

			Example:
			@@s_difference

			Output:
			@@difference
			""")
	public void difference() throws Exception {
		// tag::s_difference[]
		WKTReader reader = new WKTReader(intersectionLayer.getGeometryFactory());
		Geometry geometry = reader.read("POLYGON ((3 3, 3 5, 7 7, 7 3, 3 3))");
		GeoPipeline pipeline = GeoPipeline.start(tx, intersectionLayer).difference(geometry);
		// end::s_difference[]
		addImageSnippet(intersectionLayer, pipeline, getTitle());

		// TODO test?
	}

	@Test
	@Title("Intersection")
	@Documented("""
			The Intersection pipe computes a geometry representing the intersection between item geometry and the given geometry.

			Example:
			@@s_intersection

			Output:
			@@intersection
			""")
	public void intersection() throws Exception {
		// tag::s_intersection[]
		WKTReader reader = new WKTReader(intersectionLayer.getGeometryFactory());
		Geometry geometry = reader.read("POLYGON ((3 3, 3 5, 7 7, 7 3, 3 3))");
		GeoPipeline pipeline = GeoPipeline.start(tx, intersectionLayer).intersect(geometry);
		// end::s_intersection[]
		addImageSnippet(intersectionLayer, pipeline, getTitle());

		// TODO test?
	}

	@Test
	@Title("Union")
	@Documented("""
			The Union pipe unites item geometry with a given geometry.

			Example:
			@@s_union

			Output:
			@@union
			""")
	public void union() throws Exception {
		// tag::s_union[]
		WKTReader reader = new WKTReader(intersectionLayer.getGeometryFactory());
		Geometry geometry = reader.read("POLYGON ((3 3, 3 5, 7 7, 7 3, 3 3))");
		SearchFilter filter = new SearchIntersectWindow(intersectionLayer, new Envelope(7, 10, 7, 10));
		GeoPipeline pipeline = GeoPipeline.start(tx, intersectionLayer, filter).union(geometry);
		// end::s_union[]
		addImageSnippet(intersectionLayer, pipeline, getTitle());

		// TODO test?
	}

	@Test
	@Title("Min")
	@Documented("""
			The Min pipe computes the minimum value of the specified property and discard items with a value greater than the minimum.

			Example:
			@@s_min

			Output:
			@@min
			""")
	public void get_min_area() {
		// tag::s_min[]
		GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer)
				.calculateArea()
				.getMin("Area");
		// end::s_min[]
		addImageSnippet(boxesLayer, pipeline, getTitle());

		pipeline = GeoPipeline.start(tx, boxesLayer).calculateArea().getMin("Area");
		assertEquals((Double) pipeline.next().getProperties().get("Area"), 1.0, 0);
		pipeline.reset();
	}

	@Test
	public void extract_osm_points() {
		int count = 0;
		GeoPipeline pipeline = OSMGeoPipeline.startOsm(tx, osmLayer).extractOsmPoints().createWellKnownText();
		for (GeoPipeFlow flow : pipeline) {
			count++;

			assertEquals(1, flow.getProperties().size());
			String wkt = (String) flow.getProperties().get("WellKnownText");
			assertEquals(0, wkt.indexOf("POINT"));
		}

		assertEquals(24, count);
	}

	@Test
	@Title("Break up all geometries into points and make density islands")
	@Documented("""
			This example demonstrates the some pipes chained together to make a full geoprocessing pipeline.

			Example:
			@@s_break_up_all_geometries_into_points_and_make_density_islands

			Step 1 - startOsm:
			@@step1_break_up_all_geometries_into_points_and_make_density_islands

			Step 2 - extractOsmPoints:
			@@step2_break_up_all_geometries_into_points_and_make_density_islands

			Step 3 - groupByDensityIslands:
			@@step3_break_up_all_geometries_into_points_and_make_density_islands

			Step 4 - toConvexHull:
			@@step4_break_up_all_geometries_into_points_and_make_density_islands

			Step 5- toBuffer:
			@@step5_break_up_all_geometries_into_points_and_make_density_islands
			""")

	public void break_up_all_geometries_into_points_and_make_density_islands_and_get_the_outer_linear_ring_of_the_density_islands_and_buffer_the_geometry_and_count_them() {
		// tag::s_break_up_all_geometries_into_points_and_make_density_islands[]
		//step1
		GeoPipeline pipeline = OSMGeoPipeline.startOsm(tx, osmLayer)
				//step2
				.extractOsmPoints()
				//step3
				.groupByDensityIslands(0.0005)
				//step4
				.toConvexHull()
				//step5
				.toBuffer(0.0004);
		// end::s_break_up_all_geometries_into_points_and_make_density_islands[]

		assertEquals(9, pipeline.count());

		addOsmImageSnippet(osmLayer, OSMGeoPipeline.startOsm(tx, osmLayer), "step1_" + getTitle(),
				Constants.GTYPE_LINESTRING);
		addOsmImageSnippet(osmLayer, OSMGeoPipeline.startOsm(tx, osmLayer).extractOsmPoints(), "step2_" + getTitle(),
				Constants.GTYPE_POINT);
		addOsmImageSnippet(osmLayer,
				OSMGeoPipeline.startOsm(tx, osmLayer).extractOsmPoints().groupByDensityIslands(0.0005),
				"step3_" + getTitle(), Constants.GTYPE_POLYGON);
		addOsmImageSnippet(osmLayer,
				OSMGeoPipeline.startOsm(tx, osmLayer).extractOsmPoints().groupByDensityIslands(0.0005).toConvexHull(),
				"step4_" + getTitle(), Constants.GTYPE_POLYGON);
		addOsmImageSnippet(osmLayer,
				OSMGeoPipeline.startOsm(tx, osmLayer).extractOsmPoints().groupByDensityIslands(0.0005).toConvexHull()
						.toBuffer(0.0004), "step5_" + getTitle(), Constants.GTYPE_POLYGON);
	}

	@Test
	@Title("Extract Points")
	@Documented("""
			This pipe extracts every point from a geometry.

			Example:
			@@s_extract_points

			Output:
			@@extract_points
			""")
	public void extract_points() {
		// tag::s_extract_points[]
		GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).extractPoints();
		// end::s_extract_points[]
		addImageSnippet(boxesLayer, pipeline, getTitle(), Constants.GTYPE_POINT);

		int count = 0;
		for (GeoPipeFlow flow : GeoPipeline.start(tx, boxesLayer).extractPoints().createWellKnownText()) {
			count++;

			assertEquals(1, flow.getProperties().size());
			String wkt = (String) flow.getProperties().get("WellKnownText");
			assertEquals(0, wkt.indexOf("POINT"));
		}

		// every rectangle has 5 points, the last point is in the same position of the first
		assertEquals(10, count);
	}

	@Test
	public void filter_by_null_property() {
		assertEquals(2, GeoPipeline.start(tx, boxesLayer).copyDatabaseRecordProperties(tx).propertyNullFilter("address")
				.count());
		assertEquals(0,
				GeoPipeline.start(tx, boxesLayer).copyDatabaseRecordProperties(tx).propertyNullFilter("name").count());
	}

	@Test
	public void filter_by_not_null_property() {
		assertEquals(0,
				GeoPipeline.start(tx, boxesLayer).copyDatabaseRecordProperties(tx).propertyNotNullFilter("address")
						.count());
		assertEquals(2, GeoPipeline.start(tx, boxesLayer).copyDatabaseRecordProperties(tx).propertyNotNullFilter("name")
				.count());
	}

	@Test
	public void compute_distance() throws ParseException {
		WKTReader reader = new WKTReader(boxesLayer.getGeometryFactory());

		GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).calculateDistance(
				reader.read("POINT (0 0)")).sort("Distance");

		assertEquals(4, Math.round((Double) pipeline.next().getProperty(tx, "Distance")));
		assertEquals(29, Math.round((Double) pipeline.next().getProperty(tx, "Distance")));
		pipeline.reset();
	}

	@Test
	@Title("Unite All")
	@Documented("""
			The Union All pipe unites geometries of every item contained in the pipeline.
			This pipe groups every item in the pipeline in a single item containing the geometry output
			of the union.

			Example:
			@@s_unite_all

			Output:
			@@unite_all
			""")
	public void unite_all() {
		// tag::s_unite_all[]
		GeoPipeline pipeline = GeoPipeline.start(tx, intersectionLayer).unionAll();
		// end::s_unite_all[]
		addImageSnippet(intersectionLayer, pipeline, getTitle());

		pipeline = GeoPipeline.start(tx, intersectionLayer)
				.unionAll()
				.createWellKnownText();

		assertEquals("POLYGON ((2 5, 2 6, 4 6, 4 10, 10 10, 10 4, 6 4, 6 2, 5 2, 5 0, 0 0, 0 5, 2 5))",
				pipeline.next().getProperty(tx, "WellKnownText"));

		try {
			pipeline.next();
			fail();
		} catch (NoSuchElementException ignored) {
		}
	}

	@Test
	@Title("Intersect All")
	@Documented("""
			The Intersect All pipe intersects geometries of every item contained in the pipeline.
			This pipe groups every item in the pipeline in a single item containing the geometry output
			of the intersection.

			Example:
			@@s_intersect_all

			Output:
			@@intersect_all
			""")
	public void intersect_all() {
		// tag::s_intersect_all[]
		GeoPipeline pipeline = GeoPipeline.start(tx, intersectionLayer).intersectAll();
		// end::s_intersect_all[]
		addImageSnippet(intersectionLayer, pipeline, getTitle());

		pipeline = GeoPipeline.start(tx, intersectionLayer)
				.intersectAll()
				.createWellKnownText();

		assertEquals("POLYGON ((4 5, 5 5, 5 4, 4 4, 4 5))", pipeline.next().getProperty(tx, "WellKnownText"));

		try {
			pipeline.next();
			fail();
		} catch (NoSuchElementException ignored) {
		}
	}

	@Test
	@Title("Intersecting Windows")
	@Documented("""
			The FilterIntersectWindow pipe finds geometries that intersects a given rectangle.

			Example:
			@@s_intersecting_windows

			Output:
			@@intersecting_windows
			""")
	public void intersecting_windows() {
		// tag::s_intersecting_windows[]
		GeoPipeline pipeline = GeoPipeline
				.start(tx, boxesLayer)
				.windowIntersectionFilter(new Envelope(0, 10, 0, 10));
		// end::s_intersecting_windows[]
		addImageSnippet(boxesLayer, pipeline, getTitle());

		// TODO test?
	}

	@Test
	@Title("Start Point")
	@Documented("""
			The StartPoint pipe finds the starting point of item geometry.

			Example:
			@@s_start_point

			Output:
			@@start_point""")
	public void start_point() {
		// tag::s_start_point[]
		GeoPipeline pipeline = GeoPipeline
				.start(tx, linesLayer)
				.toStartPoint();
		// end::s_start_point[]
		addImageSnippet(linesLayer, pipeline, getTitle(), Constants.GTYPE_POINT);

		pipeline = GeoPipeline
				.start(tx, linesLayer)
				.toStartPoint()
				.createWellKnownText();

		assertEquals("POINT (12 26)", pipeline.next().getProperty(tx, "WellKnownText"));
		pipeline.reset();
	}

	@Test
	@Title("End Point")
	@Documented("""
			The EndPoint pipe finds the ending point of item geometry.

			Example:
			@@s_end_point

			Output:
			@@end_point
			""")
	public void end_point() {
		// tag::s_end_point[]
		GeoPipeline pipeline = GeoPipeline
				.start(tx, linesLayer)
				.toEndPoint();
		// end::s_end_point[]
		addImageSnippet(linesLayer, pipeline, getTitle(), Constants.GTYPE_POINT);

		pipeline = GeoPipeline
				.start(tx, linesLayer)
				.toEndPoint()
				.createWellKnownText();

		assertEquals("POINT (23 34)", pipeline.next().getProperty(tx, "WellKnownText"));
		pipeline.reset();
	}

	@Test
	@Title("Envelope")
	@Documented("""
			The Envelope pipe computes the minimum bounding box of item geometry.

			Example:
			@@s_envelope

			Output:
			@@envelope
			""")
	public void envelope() {
		// tag::s_envelope[]
		GeoPipeline pipeline = GeoPipeline
				.start(tx, linesLayer)
				.toEnvelope();
		// end::s_envelope[]
		addImageSnippet(linesLayer, pipeline, getTitle(), Constants.GTYPE_POLYGON);

		// TODO test
	}

	@Test
	public void test_equality() throws Exception {
		WKTReader reader = new WKTReader(equalLayer.getGeometryFactory());
		Geometry geom = reader.read("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))");

		GeoPipeline pipeline = GeoPipeline.startEqualExactSearch(tx, equalLayer, geom, 0)
				.copyDatabaseRecordProperties(tx);
		assertEquals("equal", pipeline.next().getProperty(tx, "name"));
		assertFalse(pipeline.hasNext());

		pipeline = GeoPipeline.startEqualExactSearch(tx, equalLayer, geom, 0.1).copyDatabaseRecordProperties(tx)
				.sort("id");
		assertEquals("equal", pipeline.next().getProperty(tx, "name"));
		assertEquals("tolerance", pipeline.next().getProperty(tx, "name"));
		assertFalse(pipeline.hasNext());

		pipeline = GeoPipeline.startIntersectWindowSearch(tx, equalLayer,
				geom.getEnvelopeInternal()).equalNormFilter(geom, 0.1).copyDatabaseRecordProperties(tx).sort("id");
		assertEquals("equal", pipeline.next().getProperty(tx, "name"));
		assertEquals("tolerance", pipeline.next().getProperty(tx, "name"));
		assertEquals("different order", pipeline.next().getProperty(tx, "name"));
		assertFalse(pipeline.hasNext());

		pipeline = GeoPipeline.startIntersectWindowSearch(tx, equalLayer,
				geom.getEnvelopeInternal()).equalTopoFilter(geom).copyDatabaseRecordProperties(tx).sort("id");
		assertEquals("equal", pipeline.next().getProperty(tx, "name"));
		assertEquals("different order", pipeline.next().getProperty(tx, "name"));
		assertEquals("topo equal", pipeline.next().getProperty(tx, "name"));
		assertFalse(pipeline.hasNext());
		pipeline.reset();
	}

	private String getTitle() {
		return gen.get().getTitle().replace(" ", "_").toLowerCase();
	}

	private void addImageSnippet(
			Layer layer,
			GeoPipeline pipeline,
			String imgName) {
		addImageSnippet(layer, pipeline, imgName, null);
	}

	private void addOsmImageSnippet(
			Layer layer,
			GeoPipeline pipeline,
			String imgName,
			Integer geomType) {
		addImageSnippet(layer, pipeline, imgName, geomType, 0.002);
	}

	private void addImageSnippet(
			Layer layer,
			GeoPipeline pipeline,
			String imgName,
			Integer geomType) {
		addImageSnippet(layer, pipeline, imgName, geomType, 1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private void addImageSnippet(
			Layer layer,
			GeoPipeline pipeline,
			String imgName,
			Integer geomType,
			double boundsDelta) {
		gen.get().addSnippet(imgName, "image::generated/" + imgName + ".png[]\n");

		try {
			FeatureCollection layerCollection = GeoPipeline.start(tx, layer, new SearchAll()).toFeatureCollection(tx);
			FeatureCollection pipelineCollection;
			if (geomType == null) {
				pipelineCollection = pipeline.toFeatureCollection(tx);
			} else {
				pipelineCollection = pipeline.toFeatureCollection(tx,
						Neo4jFeatureBuilder.getType(layer.getName(), geomType, layer.getCoordinateReferenceSystem(tx),
								layer.getExtraPropertyNames(tx)));
			}

			ReferencedEnvelope bounds = layerCollection.getBounds();
			bounds.expandToInclude(pipelineCollection.getBounds());
			bounds.expandBy(boundsDelta, boundsDelta);

			StyledImageExporter exporter = new StyledImageExporter(db);
			exporter.setExportDir("../docs/docs/modules/ROOT/images/generated");
			exporter.saveImage(
					new FeatureCollection[]{
							layerCollection,
							pipelineCollection,
					},
					new Style[]{
							StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN),
							StyledImageExporter.createDefaultStyle(Color.RED, Color.ORANGE)
					},
					new File(imgName + ".png"),
					bounds);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void load() throws Exception {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) db, SecurityContext.AUTH_DISABLED));

		try (Transaction tx = db.beginTx()) {
			loadTestOsmData("two-street.osm", 100);
			osmLayer = spatial.getLayer(tx, "two-street.osm");

			boxesLayer = (EditableLayerImpl) spatial.getOrCreateEditableLayer(tx, "boxes", null, null);
			boxesLayer.setExtraPropertyNames(new String[]{"name"}, tx);
			boxesLayer.setCoordinateReferenceSystem(tx, DefaultEngineeringCRS.GENERIC_2D);
			WKTReader reader = new WKTReader(boxesLayer.getGeometryFactory());
			boxesLayer.add(tx,
					reader.read("POLYGON ((12 26, 12 27, 13 27, 13 26, 12 26))"),
					new String[]{"name"}, new Object[]{"A"});
			boxesLayer.add(tx,
					reader.read("POLYGON ((2 3, 2 5, 6 5, 6 3, 2 3))"),
					new String[]{"name"}, new Object[]{"B"});

			concaveLayer = (EditableLayerImpl) spatial.getOrCreateEditableLayer(tx, "concave", null, null);
			concaveLayer.setCoordinateReferenceSystem(tx, DefaultEngineeringCRS.GENERIC_2D);
			reader = new WKTReader(concaveLayer.getGeometryFactory());
			concaveLayer.add(tx, reader.read("POLYGON ((0 0, 2 5, 0 10, 10 10, 10 0, 0 0))"));

			intersectionLayer = (EditableLayerImpl) spatial.getOrCreateEditableLayer(tx, "intersection", null, null);
			intersectionLayer.setCoordinateReferenceSystem(tx, DefaultEngineeringCRS.GENERIC_2D);
			reader = new WKTReader(intersectionLayer.getGeometryFactory());
			intersectionLayer.add(tx, reader.read("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))"));
			intersectionLayer.add(tx, reader.read("POLYGON ((4 4, 4 10, 10 10, 10 4, 4 4))"));
			intersectionLayer.add(tx, reader.read("POLYGON ((2 2, 2 6, 6 6, 6 2, 2 2))"));

			equalLayer = (EditableLayerImpl) spatial.getOrCreateEditableLayer(tx, "equal", null, null);
			equalLayer.setExtraPropertyNames(new String[]{"id", "name"}, tx);
			equalLayer.setCoordinateReferenceSystem(tx, DefaultEngineeringCRS.GENERIC_2D);
			reader = new WKTReader(intersectionLayer.getGeometryFactory());
			equalLayer.add(tx, reader.read("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))"),
					new String[]{"id", "name"}, new Object[]{1, "equal"});
			equalLayer.add(tx, reader.read("POLYGON ((0 0, 0.1 5, 5 5, 5 0, 0 0))"),
					new String[]{"id", "name"}, new Object[]{2, "tolerance"});
			equalLayer.add(tx, reader.read("POLYGON ((0 5, 5 5, 5 0, 0 0, 0 5))"),
					new String[]{"id", "name"}, new Object[]{3,
							"different order"});
			equalLayer.add(tx,
					reader.read("POLYGON ((0 0, 0 2, 0 4, 0 5, 5 5, 5 3, 5 2, 5 0, 0 0))"),
					new String[]{"id", "name"}, new Object[]{4, "topo equal"});

			linesLayer = (EditableLayerImpl) spatial.getOrCreateEditableLayer(tx, "lines", null, null);
			linesLayer.setCoordinateReferenceSystem(tx, DefaultEngineeringCRS.GENERIC_2D);
			reader = new WKTReader(intersectionLayer.getGeometryFactory());
			linesLayer.add(tx, reader.read("LINESTRING (12 26, 15 27, 18 32, 20 38, 23 34)"));

			tx.commit();
		}
	}

	@SuppressWarnings("SameParameterValue")
	private static void loadTestOsmData(String layerName, int commitInterval)
			throws Exception {
		String osmPath = "./" + layerName;
		System.out.println("\n=== Loading layer " + layerName + " from "
				+ osmPath + " ===");
		OSMImporter importer = new OSMImporter(layerName);
		importer.setCharset(StandardCharsets.UTF_8);
		importer.importFile(db, osmPath);
		importer.reIndex(db, commitInterval);
	}

	@BeforeEach
	public void setUp() {
		gen.get().setGraph(db);
		try (Transaction tx = db.beginTx()) {
			StyledImageExporter exporter = new StyledImageExporter(db);
			exporter.setExportDir("../docs/docs/modules/ROOT/images/generated/layers");
			exporter.saveImage(GeoPipeline.start(tx, intersectionLayer).toFeatureCollection(tx),
					StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN), new File(
							"intersectionLayer.png"));

			exporter.saveImage(GeoPipeline.start(tx, boxesLayer).toFeatureCollection(tx),
					StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN), new File(
							"boxesLayer.png"));

			exporter.saveImage(GeoPipeline.start(tx, concaveLayer).toFeatureCollection(tx),
					StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN), new File(
							"concaveLayer.png"));

			exporter.saveImage(GeoPipeline.start(tx, equalLayer).toFeatureCollection(tx),
					StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN), new File(
							"equalLayer.png"));
			exporter.saveImage(GeoPipeline.start(tx, linesLayer).toFeatureCollection(tx),
					StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN), new File(
							"linesLayer.png"));
			exporter.saveImage(GeoPipeline.start(tx, osmLayer).toFeatureCollection(tx),
					StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN), new File(
							"osmLayer.png"));
			tx.commit();
		} catch (IOException e) {
			e.printStackTrace();
		}
		tx = db.beginTx();
	}

	@AfterEach
	public void doc() {
		JavaTestDocsGenerator docsGenerator = gen.get();
		docsGenerator.addTestSourceSnippets(GeoPipesDocTest.class, "s_" + getTitle().toLowerCase());
		docsGenerator.document("../docs/docs/modules/ROOT/pages/geo-pipes", "examples");
		if (tx != null) {
			tx.commit();
			tx.close();
		}
	}

	@BeforeAll
	public static void init() {
		databases = new TestDatabaseManagementServiceBuilder(new File("target/docs").toPath()).impermanent().build();
		db = databases.database(DEFAULT_DATABASE_NAME);
		try {
			load();
		} catch (Exception e) {
			e.printStackTrace();
		}

		StyledImageExporter exporter = new StyledImageExporter(db);
		exporter.setExportDir("target/docs/images/");
	}

	@AfterAll
	public static void writeIndexAdoc() throws IOException {
		// write an index.adoc file including all generated snippets
		var adocs = new TreeSet<>();
		Path path = Paths.get("../docs/docs/modules/ROOT/pages/geo-pipes/examples/generated");
		try (Stream<Path> paths = Files.walk(path)) {
			paths.filter(Files::isRegularFile)
					.filter(p -> p.toString().endsWith(".adoc"))
					.forEach(p -> adocs.add(p.getFileName().toString()));
		}
		adocs.remove("index.adoc");
		Files.write(path.resolve("index.adoc"), Stream.concat(
						Stream.of("// DO NOT MODIFY, THIS FILE IS AUTO GENERATED!"),
						adocs.stream().map(s -> "include::" + s + "[]"))
				.collect(Collectors.toList()));
	}

	private static GeoPipeFlow print(GeoPipeFlow pipeFlow) {
		System.out.println("GeoPipeFlow:");
		for (String key : pipeFlow.getProperties().keySet()) {
			System.out.println(key + "=" + pipeFlow.getProperties().get(key));
		}
		System.out.println("-");
		return pipeFlow;
	}
}
