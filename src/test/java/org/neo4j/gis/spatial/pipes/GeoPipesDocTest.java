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

import org.geotools.data.neo4j.Neo4jFeatureBuilder;
import org.geotools.data.neo4j.StyledImageExporter;
import org.geotools.feature.FeatureCollection;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.styling.Style;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.annotations.documented.Documented;
import org.neo4j.gis.spatial.*;
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

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

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
        assertEquals(
                1,
                GeoPipeline.start(tx, osmLayer).windowIntersectionFilter(10, 40, 20,
                        56.0583531).count());
    }

    /**
     * This pipe is filtering according to a CQL Bounding Box description.
     * <p>
     * Example:
     *
     * @@s_filter_by_cql_using_bbox
     */
    @Documented("filter_by_cql_using_bbox")
    @Test
    public void filter_by_cql_using_bbox() throws CQLException {
        // tag::filter_by_cql_using_bbox[]
        GeoPipeline cqlFilter = GeoPipeline.start(tx, osmLayer).cqlFilter(tx, "BBOX(the_geom, 10, 40, 20, 56.0583531)");
        // end::filter_by_cql_using_bbox[]
        assertEquals(1, cqlFilter.count());
    }

    /**
     * This pipe performs a search within a geometry in this example,
     * both OSM street geometries should be found in when searching with
     * an enclosing rectangle Envelope.
     * <p>
     * Example:
     *
     * @@s_search_within_geometry
     */
    @Test
    @Documented("search_within_geometry")
    public void search_within_geometry() {
        // tag::search_within_geometry[]
        GeoPipeline pipeline = GeoPipeline
                .startWithinSearch(tx, osmLayer, osmLayer.getGeometryFactory().toGeometry(new Envelope(10, 20, 50, 60)));
        // end::search_within_geometry[]
        assertEquals(2, pipeline.count());
    }

    @Test
    public void filter_by_cql_using_property() throws CQLException {
        GeoPipeline pipeline = GeoPipeline.start(tx, osmLayer).cqlFilter(tx, "name = 'Storgatan'").copyDatabaseRecordProperties(tx);

        GeoPipeFlow flow = pipeline.next();
        assertFalse(pipeline.hasNext());

        assertEquals("Storgatan", flow.getProperties().get("name"));
    }

    /**
     * This filter will apply the provided CQL expression to the different
     * geometries and only let the matching ones pass.
     * <p>
     * Example:
     *
     * @@s_filter_by_cql_using_complex_cql
     */
    @Documented("filter_by_cql_using_complex_cql")
    @Test
    public void filter_by_cql_using_complex_cql() throws CQLException {
        // tag::filter_by_cql_using_complex_cql[]
        long counter = GeoPipeline.start(tx, osmLayer).cqlFilter(tx, "highway is not null and geometryType(the_geom) = 'LineString'").count();
        // end::filter_by_cql_using_complex_cql[]

        FilterCQL filter = new FilterCQL(tx, osmLayer, "highway is not null and geometryType(the_geom) = 'LineString'");
        filter.setStarts(GeoPipeline.start(tx, osmLayer));
        assertTrue(filter.hasNext());
        while (filter.hasNext()) {
            filter.next();
            counter--;
        }
        assertEquals(0, counter);
    }

    /**
     * Affine Transformation
     * <p>
     * The ApplyAffineTransformation pipe applies an affine transformation to every geometry.
     * <p>
     * Example:
     *
     * @@s_affine_transformation Output:
     * @@affine_transformation
     */
    @Documented("translate_geometries")
    @Test
    public void translate_geometries() {
        // tag::affine_transformation[]
        GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer)
                .applyAffineTransform(AffineTransformation.translationInstance(2, 3));
        // end::affine_transformation[]
        addImageSnippet(boxesLayer, pipeline, getTitle());

        GeoPipeline original = GeoPipeline.start(tx, osmLayer).copyDatabaseRecordProperties(tx).sort("name");

        GeoPipeline translated = GeoPipeline.start(tx, osmLayer).applyAffineTransform(
                AffineTransformation.translationInstance(10, 25)).copyDatabaseRecordProperties(tx).sort("name");

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
    }

    @Test
    public void calculate_length() {
        GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).calculateLength().sort("Length");

        assertEquals((Double) pipeline.next().getProperties().get("Length"), 4.0, 0);
        assertEquals((Double) pipeline.next().getProperties().get("Length"), 12.0, 0);
    }

    @Test
    public void get_boundary_length() {
        GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).toBoundary().createWellKnownText().calculateLength().sort("Length");

        GeoPipeFlow first = pipeline.next();
        GeoPipeFlow second = pipeline.next();
        assertEquals("LINEARRING (12 26, 12 27, 13 27, 13 26, 12 26)", first.getProperties().get("WellKnownText"));
        assertEquals("LINEARRING (2 3, 2 5, 6 5, 6 3, 2 3)", second.getProperties().get("WellKnownText"));
        assertEquals((Double) first.getProperties().get("Length"), 4.0, 0);
        assertEquals((Double) second.getProperties().get("Length"), 12.0, 0);
    }

    /**
     * Buffer
     * <p>
     * The Buffer pipe applies a buffer to geometries.
     * <p>
     * Example:
     *
     * @@s_buffer Output:
     * @@buffer
     */
    @Documented("get_buffer")
    @Test
    public void get_buffer() {
        // tag::buffer[]
        GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).toBuffer(0.5);
        // end::buffer[]
        addImageSnippet(boxesLayer, pipeline, getTitle());

        pipeline = GeoPipeline.start(tx, boxesLayer).toBuffer(0.1).createWellKnownText().calculateArea().sort("Area");

        assertTrue(((Double) pipeline.next().getProperties().get("Area")) > 1);
        assertTrue(((Double) pipeline.next().getProperties().get("Area")) > 8);
    }

    /**
     * Centroid
     * <p>
     * The Centroid pipe calculates geometry centroid.
     * <p>
     * Example:
     *
     * @@s_centroid Output:
     * @@centroid
     */
    @Documented("get_centroid")
    @Test
    public void get_centroid() {
        // tag::centroid[]
        GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).toCentroid();
        // end::centroid[]
        addImageSnippet(boxesLayer, pipeline, getTitle(), Constants.GTYPE_POINT);

        pipeline = GeoPipeline.start(tx, boxesLayer).toCentroid().createWellKnownText().copyDatabaseRecordProperties(tx).sort("name");

        assertEquals("POINT (12.5 26.5)", pipeline.next().getProperties().get("WellKnownText"));
        assertEquals("POINT (4 4)", pipeline.next().getProperties().get("WellKnownText"));
    }

    /**
     * This pipe exports every geometry as a
     * http://en.wikipedia.org/wiki/Geography_Markup_Language[GML] snippet.
     * <p>
     * Example:
     *
     * @@s_export_to_gml Output:
     * @@exportgml
     */
    @Documented("export_to_GML")
    @Test
    public void export_to_GML() {
        // tag::export_to_gml[]
        GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).createGML();
        for (GeoPipeFlow flow : pipeline) {
            System.out.println(flow.getProperties().get("GML"));
        }
        // end::export_to_gml[]
        String result = "";
        for (GeoPipeFlow flow : GeoPipeline.start(tx, boxesLayer).createGML()) {
            result = result + flow.getProperties().get("GML");
        }
        gen.get().addSnippet("exportgml", "[source,xml]\n----\n" + result + "\n----\n");
    }

    /**
     * Convex Hull
     * <p>
     * The ConvexHull pipe calculates geometry convex hull.
     * <p>
     * Example:
     *
     * @@s_convex_hull Output:
     * @@convex_hull
     */
    @Documented("get_convex_hull")
    @Test
    public void get_convex_hull() {
        // tag::convex_hull[]
        GeoPipeline pipeline = GeoPipeline.start(tx, concaveLayer).toConvexHull();
        // end::convex_hull[]
        addImageSnippet(concaveLayer, pipeline, getTitle());

        pipeline = GeoPipeline.start(tx, concaveLayer).toConvexHull().createWellKnownText();

        assertEquals("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", pipeline.next().getProperties().get("WellKnownText"));
    }

    /**
     * Densify
     * <p>
     * The Densify pipe inserts extra vertices along the line segments in the geometry.
     * The densified geometry contains no line segment which is longer than the given distance tolerance.
     * <p>
     * Example:
     *
     * @@s_densify Output:
     * @@densify
     */
    @Documented("densify")
    @Test
    public void densify() {
        // tag::densify[]
        GeoPipeline pipeline = GeoPipeline.start(tx, concaveLayer).densify(5).extractPoints();
        // end::densify[]
        addImageSnippet(concaveLayer, pipeline, getTitle(), Constants.GTYPE_POINT);


        pipeline = GeoPipeline.start(tx, concaveLayer).toConvexHull().densify(10).createWellKnownText();

        assertEquals(
                "POLYGON ((0 0, 0 5, 0 10, 5 10, 10 10, 10 5, 10 0, 5 0, 0 0))",
                pipeline.next().getProperties().get("WellKnownText"));
    }

    @Test
    public void json() {
        GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).createJson().copyDatabaseRecordProperties(tx).sort("name");

        assertEquals(
                "{\"type\":\"Polygon\",\"coordinates\":[[[12,26],[12,27],[13,27],[13,26],[12,26]]]}",
                pipeline.next().getProperties().get("GeoJSON"));
        assertEquals(
                "{\"type\":\"Polygon\",\"coordinates\":[[[2,3],[2,5],[6,5],[6,3],[2,3]]]}",
                pipeline.next().getProperties().get("GeoJSON"));
    }

    /**
     * Max
     * <p>
     * The Max pipe computes the maximum value of the specified property and
     * discard items with a value less than the maximum.
     * <p>
     * Example:
     *
     * @@s_max Output:
     * @@max
     */
    @Documented("get_max_area")
    @Test
    public void get_max_area() {
        // tag::max[]
        GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer)
                .calculateArea()
                .getMax("Area");
        // end::max[]
        addImageSnippet(boxesLayer, pipeline, getTitle());

        pipeline = GeoPipeline.start(tx, boxesLayer).calculateArea().getMax("Area");
        assertEquals((Double) pipeline.next().getProperties().get("Area"), 8.0, 0);
    }

    /**
     * The boundary pipe calculates boundary of every geometry in the pipeline.
     * <p>
     * Example:
     *
     * @@s_boundary Output:
     * @@boundary
     */
    @Documented("boundary")
    @Test
    public void boundary() {
        // tag::boundary[]
        GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).toBoundary();
        // end::boundary[]
        addImageSnippet(boxesLayer, pipeline, getTitle(), Constants.GTYPE_LINESTRING);

        // TODO test?
    }

    /**
     * Difference
     * <p>
     * The Difference pipe computes a geometry representing the points making
     * up item geometry that do not make up the given geometry.
     * <p>
     * Example:
     *
     * @@s_difference Output:
     * @@difference
     */
    @Documented("difference.")
    @Test
    public void difference() throws Exception {
        // tag::difference[]
        WKTReader reader = new WKTReader(intersectionLayer.getGeometryFactory());
        Geometry geometry = reader.read("POLYGON ((3 3, 3 5, 7 7, 7 3, 3 3))");
        GeoPipeline pipeline = GeoPipeline.start(tx, intersectionLayer).difference(geometry);
        // end::difference[]
        addImageSnippet(intersectionLayer, pipeline, getTitle());

        // TODO test?
    }

    /**
     * Intersection
     * <p>
     * The Intersection pipe computes a geometry representing the intersection
     * between item geometry and the given geometry.
     * <p>
     * Example:
     *
     * @@s_intersection Output:
     * @@intersection
     */
    @Documented("intersection")
    @Test
    public void intersection() throws Exception {
        // tag::intersection[]
        WKTReader reader = new WKTReader(intersectionLayer.getGeometryFactory());
        Geometry geometry = reader.read("POLYGON ((3 3, 3 5, 7 7, 7 3, 3 3))");
        GeoPipeline pipeline = GeoPipeline.start(tx, intersectionLayer).intersect(geometry);
        // end::intersection[]
        addImageSnippet(intersectionLayer, pipeline, getTitle());

        // TODO test?
    }

    /**
     * Union
     * <p>
     * The Union pipe unites item geometry with a given geometry.
     * <p>
     * Example:
     *
     * @@s_union Output:
     * @@union
     */
    @Documented("union")
    @Test
    public void union() throws Exception {
        // tag::union[]
        WKTReader reader = new WKTReader(intersectionLayer.getGeometryFactory());
        Geometry geometry = reader.read("POLYGON ((3 3, 3 5, 7 7, 7 3, 3 3))");
        SearchFilter filter = new SearchIntersectWindow(intersectionLayer, new Envelope(7, 10, 7, 10));
        GeoPipeline pipeline = GeoPipeline.start(tx, intersectionLayer, filter).union(geometry);
        // end::union[]
        addImageSnippet(intersectionLayer, pipeline, getTitle());

        // TODO test?
    }

    /**
     * Min
     * <p>
     * The Min pipe computes the minimum value of the specified property and
     * discard items with a value greater than the minimum.
     * <p>
     * Example:
     *
     * @@s_min Output:
     * @@min
     */
    @Documented("get_min_area")
    @Test
    public void get_min_area() {
        // tag::min[]
        GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer)
                .calculateArea()
                .getMin("Area");
        // end::min[]
        addImageSnippet(boxesLayer, pipeline, getTitle());

        pipeline = GeoPipeline.start(tx, boxesLayer).calculateArea().getMin("Area");
        assertEquals((Double) pipeline.next().getProperties().get("Area"), 1.0, 0);
    }

    @Test
    public void extract_osm_points() {
        int count = 0;
        GeoPipeline pipeline = OSMGeoPipeline.startOsm(tx, osmLayer).extractOsmPoints().createWellKnownText();
        for (GeoPipeFlow flow : pipeline) {
            count++;

            assertEquals(1, flow.getProperties().size());
            String wkt = (String) flow.getProperties().get("WellKnownText");
            assertTrue(wkt.indexOf("POINT") == 0);
        }

        assertEquals(24, count);
    }

    /**
     * A more complex Open Street Map example.
     * <p>
     * This example demostrates the some pipes chained together to make a full
     * geoprocessing pipeline.
     * <p>
     * Example:
     *
     * @@s_break_up_all_geometries_into_points_and_make_density_islands _Step1_
     * @@step1_break_up_all_geometries_into_points_and_make_density_islands _Step2_
     * @@step2_break_up_all_geometries_into_points_and_make_density_islands _Step3_
     * @@step3_break_up_all_geometries_into_points_and_make_density_islands _Step4_
     * @@step4_break_up_all_geometries_into_points_and_make_density_islands _Step5_
     * @@step5_break_up_all_geometries_into_points_and_make_density_islands
     */
    @Documented("break_up_all_geometries_into_points_and_make_density_islands_and_get_the_outer_linear_ring_of_the_density_islands_and_buffer_the_geometry_and_count_them")
    @Title("break_up_all_geometries_into_points_and_make_density_islands")
    @Test
    public void break_up_all_geometries_into_points_and_make_density_islands_and_get_the_outer_linear_ring_of_the_density_islands_and_buffer_the_geometry_and_count_them() {
        // tag::break_up_all_geometries_into_points_and_make_density_islands[]
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
        // end::break_up_all_geometries_into_points_and_make_density_islands[]

        assertEquals(9, pipeline.count());

        addOsmImageSnippet(osmLayer, OSMGeoPipeline.startOsm(tx, osmLayer), "step1_" + getTitle(), Constants.GTYPE_LINESTRING);
        addOsmImageSnippet(osmLayer, OSMGeoPipeline.startOsm(tx, osmLayer).extractOsmPoints(), "step2_" + getTitle(), Constants.GTYPE_POINT);
        addOsmImageSnippet(osmLayer, OSMGeoPipeline.startOsm(tx, osmLayer).extractOsmPoints().groupByDensityIslands(0.0005), "step3_" + getTitle(), Constants.GTYPE_POLYGON);
        addOsmImageSnippet(osmLayer, OSMGeoPipeline.startOsm(tx, osmLayer).extractOsmPoints().groupByDensityIslands(0.0005).toConvexHull(), "step4_" + getTitle(), Constants.GTYPE_POLYGON);
        addOsmImageSnippet(osmLayer, OSMGeoPipeline.startOsm(tx, osmLayer).extractOsmPoints().groupByDensityIslands(0.0005).toConvexHull().toBuffer(0.0004), "step5_" + getTitle(), Constants.GTYPE_POLYGON);
    }

    /**
     * Extract Points
     * <p>
     * The Extract Points pipe extracts every point from a geometry.
     * <p>
     * Example:
     *
     * @@s_extract_points Output:
     * @@extract_points
     */
    @Documented("extract_points")
    @Test
    public void extract_points() {
        // tag::extract_points[]
        GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).extractPoints();
        // end::extract_points[]
        addImageSnippet(boxesLayer, pipeline, getTitle(), Constants.GTYPE_POINT);

        int count = 0;
        for (GeoPipeFlow flow : GeoPipeline.start(tx, boxesLayer).extractPoints().createWellKnownText()) {
            count++;

            assertEquals(1, flow.getProperties().size());
            String wkt = (String) flow.getProperties().get("WellKnownText");
            assertTrue(wkt.indexOf("POINT") == 0);
        }

        // every rectangle has 5 points, the last point is in the same position of the first
        assertEquals(10, count);
    }

    @Test
    public void filter_by_null_property() {
        assertEquals(
                2,
                GeoPipeline.start(tx, boxesLayer).copyDatabaseRecordProperties(tx).propertyNullFilter("address").count());
        assertEquals(
                0,
                GeoPipeline.start(tx, boxesLayer).copyDatabaseRecordProperties(tx).propertyNullFilter("name").count());
    }

    @Test
    public void filter_by_not_null_property() {
        assertEquals(
                0,
                GeoPipeline.start(tx, boxesLayer).copyDatabaseRecordProperties(tx).propertyNotNullFilter("address").count());
        assertEquals(
                2,
                GeoPipeline.start(tx, boxesLayer).copyDatabaseRecordProperties(tx).propertyNotNullFilter("name").count());
    }

    @Test
    public void compute_distance() throws ParseException {
        WKTReader reader = new WKTReader(boxesLayer.getGeometryFactory());

        GeoPipeline pipeline = GeoPipeline.start(tx, boxesLayer).calculateDistance(
                reader.read("POINT (0 0)")).sort("Distance");

        assertEquals(
                4, Math.round((Double) pipeline.next().getProperty(tx, "Distance")));
        assertEquals(
                29, Math.round((Double) pipeline.next().getProperty(tx, "Distance")));
    }

    /**
     * Unite All
     * <p>
     * The Union All pipe unites geometries of every item contained in the pipeline.
     * This pipe groups every item in the pipeline in a single item containing the geometry output
     * of the union.
     * <p>
     * Example:
     *
     * @@s_unite_all Output:
     * @@unite_all
     */
    @Documented("unite_all")
    @Test
    public void unite_all() throws ParseException {
        // tag::unite_all[]
        GeoPipeline pipeline = GeoPipeline.start(tx, intersectionLayer).unionAll();
        // end::unite_all[]
        addImageSnippet(intersectionLayer, pipeline, getTitle());

        pipeline = GeoPipeline.start(tx, intersectionLayer)
                .unionAll()
                .createWellKnownText();

        assertWKTGeometryEquals(intersectionLayer, pipeline, "POLYGON ((0 0, 0 5, 2 5, 2 6, 4 6, 4 10, 10 10, 10 4, 6 4, 6 2, 5 2, 5 0, 0 0))");

        try {
            pipeline.next();
            fail();
        } catch (NoSuchElementException ignored) {
        }
    }

    /**
     * Intersect All
     * <p>
     * The IntersectAll pipe intersects geometries of every item contained in the pipeline.
     * It groups every item in the pipeline in a single item containing the geometry output
     * of the intersection.
     * <p>
     * Example:
     *
     * @@s_intersect_all Output:
     * @@intersect_all
     */
    @Documented("intersect_all")
    @Test
    public void intersect_all() throws ParseException {
        // tag::intersect_all[]
        GeoPipeline pipeline = GeoPipeline.start(tx, intersectionLayer).intersectAll();
        // end::intersect_all[]
        addImageSnippet(intersectionLayer, pipeline, getTitle());

        pipeline = GeoPipeline.start(tx, intersectionLayer)
                .intersectAll()
                .createWellKnownText();

        assertWKTGeometryEquals(intersectionLayer, pipeline, "POLYGON ((4 5, 5 5, 5 4, 4 4, 4 5))");

        try {
            pipeline.next();
            fail();
        } catch (NoSuchElementException ignored) {
        }
    }

    /**
     * Intersecting windows
     * <p>
     * The FilterIntersectWindow pipe finds geometries that intersects a given rectangle.
     * This pipeline:
     *
     * @@s_intersecting_windows will output:
     * @@intersecting_windows
     */
    @Documented("intersecting_windows")
    @Test
    public void intersecting_windows() {
        // tag::intersecting_windows[]
        GeoPipeline pipeline = GeoPipeline
                .start(tx, boxesLayer)
                .windowIntersectionFilter(new Envelope(0, 10, 0, 10));
        // end::intersecting_windows[]
        addImageSnippet(boxesLayer, pipeline, getTitle());

        // TODO test?
    }

    /**
     * Start Point
     * <p>
     * The StartPoint pipe finds the starting point of item geometry.
     * <p>
     * Example:
     *
     * @@s_start_point Output:
     * @@start_point
     */
    @Documented("start_point")
    @Test
    public void start_point() {
        // tag::start_point[]
        GeoPipeline pipeline = GeoPipeline
                .start(tx, linesLayer)
                .toStartPoint();
        // end::start_point[]
        addImageSnippet(linesLayer, pipeline, getTitle(), Constants.GTYPE_POINT);

        pipeline = GeoPipeline
                .start(tx, linesLayer)
                .toStartPoint()
                .createWellKnownText();

        assertEquals("POINT (12 26)", pipeline.next().getProperty(tx, "WellKnownText"));
    }

    /**
     * End Point
     * <p>
     * The EndPoint pipe finds the ending point of item geometry.
     * <p>
     * Example:
     *
     * @@s_end_point Output:
     * @@end_point
     */
    @Documented("end_point")
    @Test
    public void end_point() {
        // tag::end_point[]
        GeoPipeline pipeline = GeoPipeline
                .start(tx, linesLayer)
                .toEndPoint();
        // end::end_point[]
        addImageSnippet(linesLayer, pipeline, getTitle(), Constants.GTYPE_POINT);

        pipeline = GeoPipeline
                .start(tx, linesLayer)
                .toEndPoint()
                .createWellKnownText();

        assertEquals("POINT (23 34)", pipeline.next().getProperty(tx, "WellKnownText"));
    }

    /**
     * Envelope
     * <p>
     * The Envelope pipe computes the minimum bounding box of item geometry.
     * <p>
     * Example:
     *
     * @@s_envelope Output:
     * @@envelope
     */
    @Documented("envelope")
    @Test
    public void envelope() {
        // tag::envelope[]
        GeoPipeline pipeline = GeoPipeline
                .start(tx, linesLayer)
                .toEnvelope();
        // end::envelope[]
        addImageSnippet(linesLayer, pipeline, getTitle(), Constants.GTYPE_POLYGON);

        // TODO test
    }

    @Test
    public void test_equality() throws Exception {
        WKTReader reader = new WKTReader(equalLayer.getGeometryFactory());
        Geometry geom = reader.read("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))");

        GeoPipeline pipeline = GeoPipeline.startEqualExactSearch(tx, equalLayer, geom, 0).copyDatabaseRecordProperties(tx);
        assertEquals("equal", pipeline.next().getProperty(tx, "name"));
        assertFalse(pipeline.hasNext());

        pipeline = GeoPipeline.startEqualExactSearch(tx, equalLayer, geom, 0.1).copyDatabaseRecordProperties(tx).sort("id");
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

    @SuppressWarnings({"unchecked", "rawtypes"})
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
        gen.get().addSnippet(imgName, "\nimage::" + imgName + ".png[scaledwidth=\"75%\"]\n");

        try {
            FeatureCollection layerCollection = GeoPipeline.start(tx, layer, new SearchAll()).toFeatureCollection(tx);
            FeatureCollection pipelineCollection;
            if (geomType == null) {
                pipelineCollection = pipeline.toFeatureCollection(tx);
            } else {
                pipelineCollection = pipeline.toFeatureCollection(tx,
                        Neo4jFeatureBuilder.getType(layer.getName(), geomType, layer.getCoordinateReferenceSystem(tx), layer.getExtraPropertyNames(tx)));
            }

            ReferencedEnvelope bounds = layerCollection.getBounds();
            bounds.expandToInclude(pipelineCollection.getBounds());
            bounds.expandBy(boundsDelta, boundsDelta);

            StyledImageExporter exporter = new StyledImageExporter(db);
            exporter.setExportDir("target/docs/images/");
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
        SpatialDatabaseService spatial = new SpatialDatabaseService(new IndexManager((GraphDatabaseAPI) db, SecurityContext.AUTH_DISABLED));

        try (Transaction tx = db.beginTx()) {
            loadTestOsmData("two-street.osm", 100);
            osmLayer = spatial.getLayer(tx, "two-street.osm");

            boxesLayer = (EditableLayerImpl) spatial.getOrCreateEditableLayer(tx, "boxes");
            boxesLayer.setExtraPropertyNames(new String[]{"name"}, tx);
            boxesLayer.setCoordinateReferenceSystem(tx, DefaultEngineeringCRS.GENERIC_2D);
            WKTReader reader = new WKTReader(boxesLayer.getGeometryFactory());
            boxesLayer.add(tx,
                    reader.read("POLYGON ((12 26, 12 27, 13 27, 13 26, 12 26))"),
                    new String[]{"name"}, new Object[]{"A"});
            boxesLayer.add(tx,
                    reader.read("POLYGON ((2 3, 2 5, 6 5, 6 3, 2 3))"),
                    new String[]{"name"}, new Object[]{"B"});

            concaveLayer = (EditableLayerImpl) spatial.getOrCreateEditableLayer(tx, "concave");
            concaveLayer.setCoordinateReferenceSystem(tx, DefaultEngineeringCRS.GENERIC_2D);
            reader = new WKTReader(concaveLayer.getGeometryFactory());
            concaveLayer.add(tx, reader.read("POLYGON ((0 0, 2 5, 0 10, 10 10, 10 0, 0 0))"));

            intersectionLayer = (EditableLayerImpl) spatial.getOrCreateEditableLayer(tx, "intersection");
            intersectionLayer.setCoordinateReferenceSystem(tx, DefaultEngineeringCRS.GENERIC_2D);
            reader = new WKTReader(intersectionLayer.getGeometryFactory());
            intersectionLayer.add(tx, reader.read("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))"));
            intersectionLayer.add(tx, reader.read("POLYGON ((4 4, 4 10, 10 10, 10 4, 4 4))"));
            intersectionLayer.add(tx, reader.read("POLYGON ((2 2, 2 6, 6 6, 6 2, 2 2))"));

            equalLayer = (EditableLayerImpl) spatial.getOrCreateEditableLayer(tx, "equal");
            equalLayer.setExtraPropertyNames(new String[]{"id", "name"}, tx);
            equalLayer.setCoordinateReferenceSystem(tx, DefaultEngineeringCRS.GENERIC_2D);
            reader = new WKTReader(equalLayer.getGeometryFactory());
            equalLayer.add(tx, reader.read("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))"), new String[]{"id", "name"}, new Object[]{1, "equal"});
            equalLayer.add(tx, reader.read("POLYGON ((0 0, 0.1 5, 5 5, 5 0, 0 0))"), new String[]{"id", "name"}, new Object[]{2, "tolerance"});
            equalLayer.add(tx, reader.read("POLYGON ((0 5, 5 5, 5 0, 0 0, 0 5))"), new String[]{"id", "name"}, new Object[]{3, "different order"});
            equalLayer.add(tx, reader.read("POLYGON ((0 0, 0 2, 0 4, 0 5, 5 5, 5 3, 5 2, 5 0, 0 0))"), new String[]{"id", "name"}, new Object[]{4, "topo equal"});

            linesLayer = (EditableLayerImpl) spatial.getOrCreateEditableLayer(tx, "lines");
            linesLayer.setCoordinateReferenceSystem(tx, DefaultEngineeringCRS.GENERIC_2D);
            reader = new WKTReader(intersectionLayer.getGeometryFactory());
            linesLayer.add(tx, reader.read("LINESTRING (12 26, 15 27, 18 32, 20 38, 23 34)"));

            tx.commit();
        }
    }

    private static void loadTestOsmData(String layerName, int commitInterval) throws Exception {
        String osmPath = "./" + layerName;
        System.out.println("\n=== Loading layer " + layerName + " from " + osmPath + " ===");
        OSMImporter importer = new OSMImporter(layerName);
        importer.setCharset(StandardCharsets.UTF_8);
        importer.importFile(db, osmPath);
        importer.reIndex(db, commitInterval);
    }

    private void assertWKTGeometryEquals(EditableLayerImpl layer, GeoPipeline pipeline, String expectedWKT) throws ParseException {
        WKTReader reader = new WKTReader(layer.getGeometryFactory());
        Geometry expected = reader.read(expectedWKT);
        Geometry actual = reader.read(pipeline.next().getProperty(tx, "WellKnownText").toString());
        assertEquals("Expected matching geometry types", expected.getGeometryType(), actual.getGeometryType());
        assertEquals("Expected matching geometry areas", expected.getArea(), actual.getArea(), 0.000001);
        // JTS will handle different starting coordinates for matching geometries, so we check with JTS first, and only if that fails run the assertion to get the appropriate error message
        if (!expected.equals(actual)) {
            assertEquals("Expected matching geometries", expected, actual);
        }
    }

    @Before
    public void setUp() {
        gen.get().setGraph(db);
        try (Transaction tx = db.beginTx()) {
            StyledImageExporter exporter = new StyledImageExporter(db);
            exporter.setExportDir("target/docs/images/");
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

    @After
    public void doc() {
        // gen.get().addSnippet( "graph", AsciidocHelper.createGraphViz( imgName , graphdb(), "graph"+getTitle() ) );
        gen.get().addTestSourceSnippets(GeoPipesDocTest.class, "s_" + getTitle().toLowerCase());
        gen.get().document("target/docs", "examples");
        if (tx != null) {
            tx.commit();
            tx.close();
        }
    }

    @BeforeClass
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

    private GeoPipeFlow print(GeoPipeFlow pipeFlow) {
        System.out.println("GeoPipeFlow:");
        for (String key : pipeFlow.getProperties().keySet()) {
            System.out.println(key + "=" + pipeFlow.getProperties().get(key));
        }
        System.out.println("-");
        return pipeFlow;
    }
}
