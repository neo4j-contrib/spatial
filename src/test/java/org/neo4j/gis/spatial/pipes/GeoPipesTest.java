/**
 * Copyright (c) 2010-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.pipes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.NoSuchElementException;

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
import org.neo4j.gis.spatial.rtree.filter.SearchAll;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.examples.AbstractJavaDocTestBase;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.EditableLayerImpl;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.filter.SearchIntersectWindow;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.pipes.filtering.FilterCQL;
import org.neo4j.gis.spatial.pipes.osm.OSMGeoPipeline;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestData.Title;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.util.AffineTransformation;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.neo4j.test.TestGraphDatabaseFactory;

public class GeoPipesTest extends AbstractJavaDocTestBase
{

    private static Layer osmLayer;
    private static EditableLayerImpl boxesLayer;
    private static EditableLayerImpl concaveLayer;
    private static EditableLayerImpl intersectionLayer;
    private static EditableLayerImpl equalLayer;
    private static EditableLayerImpl linesLayer;
    private Transaction tx;

    @Test
    public void find_all()
    {
        int count = 0;
        for ( GeoPipeFlow flow : GeoPipeline.start( osmLayer ).createWellKnownText() )
        {
            count++;

            assertEquals( 1, flow.getProperties().size() );
            String wkt = (String) flow.getProperties().get( "WellKnownText" );
            assertTrue( wkt.indexOf( "LINESTRING" ) == 0 );
        }

        assertEquals( 2, count );
    }

    @Test
    public void filter_by_osm_attribute()
    {
        GeoPipeline pipeline = OSMGeoPipeline.startOsm( osmLayer )
        	.osmAttributeFilter( "name", "Storgatan" )
        	.copyDatabaseRecordProperties();

        GeoPipeFlow flow = pipeline.next();
        assertFalse( pipeline.hasNext() );

        assertEquals( "Storgatan", flow.getProperties().get( "name" ) );
    }

    @Test
    public void filter_by_property()
    {
        GeoPipeline pipeline = GeoPipeline.start( osmLayer )
        	.copyDatabaseRecordProperties( "name" )
        	.propertyFilter( "name", "Storgatan" );

        GeoPipeFlow flow = pipeline.next();
        assertFalse( pipeline.hasNext() );

        assertEquals( "Storgatan", flow.getProperties().get( "name" ) );
    }


    @Test
    public void filter_by_window_intersection()
    {
        assertEquals(
                1,
                GeoPipeline.start( osmLayer ).windowIntersectionFilter( 10, 40, 20,
                        56.0583531 ).count() );
    }
    
    /**
     * This pipe is filtering according
     * to a CQL Bounding Box description
     * 
     * Example:
     * 
     * @@s_filter_by_cql_using_bbox
     */
    @Documented
    @Test
    public void filter_by_cql_using_bbox() throws CQLException
    {
        // START SNIPPET: s_filter_by_cql_using_bbox
        GeoPipeline cqlFilter = GeoPipeline.start( osmLayer ).cqlFilter(
                "BBOX(the_geom, 10, 40, 20, 56.0583531)" );
        // END SNIPPET: s_filter_by_cql_using_bbox
        assertEquals(
                1,
                cqlFilter.count() );
    }
    
    /**
     * This pipe performs a search within a
     * geometry in this example, both OSM street
     * geometries should be found in when searching with
     * an enclosing rectangle Envelope.
     * 
     * Example:
     * 
     * @@s_search_within_geometry
     */
    @Test
    @Documented
    public void search_within_geometry() throws CQLException
    {
        // START SNIPPET: s_search_within_geometry
        GeoPipeline pipeline = GeoPipeline
        .startWithinSearch(osmLayer, osmLayer.getGeometryFactory().toGeometry(new Envelope(10, 20, 50, 60)));
        // END SNIPPET: s_search_within_geometry
        assertEquals(
                2,
                pipeline.count() );
    }

    @Test
    public void filter_by_cql_using_property() throws CQLException
    {
        GeoPipeline pipeline = GeoPipeline.start( osmLayer ).cqlFilter(
                "name = 'Storgatan'" ).copyDatabaseRecordProperties();

        GeoPipeFlow flow = pipeline.next();
        assertFalse( pipeline.hasNext() );

        assertEquals( "Storgatan", flow.getProperties().get( "name" ) );
    }

    /**
     * This filter will apply the
     * provided CQL expression to the different geometries and only
     * let the matching ones pass.
     * 
     * Example:
     * 
     * @@s_filter_by_cql_using_complex_cql
     */
    @Documented
    @Test
    public void filter_by_cql_using_complex_cql() throws CQLException
    {
        // START SNIPPET: s_filter_by_cql_using_complex_cql
        long counter  = GeoPipeline.start( osmLayer ).cqlFilter(
                        "highway is not null and geometryType(the_geom) = 'LineString'" ).count();
        // END SNIPPET: s_filter_by_cql_using_complex_cql
        
        FilterCQL filter = new FilterCQL(osmLayer,"highway is not null and geometryType(the_geom) = 'LineString'" ); 
        filter.setStarts( GeoPipeline.start( osmLayer ));
        assertTrue( filter.hasNext() );
        while(filter.hasNext())
        {
            filter.next();
            counter --;
        }
        assertEquals( 0, counter );
    }
    
    /**
     * Affine Transformation
     * 
     * The ApplyAffineTransformation pipe applies an affine transformation to every geometry.
     * 
     * Example:
     * 
     * @@s_affine_transformation
     * 
     * Output:
     * 
     * @@affine_transformation
     */
    @Documented     
    @Test
    public void traslate_geometries()
    {
    	// START SNIPPET: s_affine_transformation
        GeoPipeline pipeline = GeoPipeline.start( boxesLayer )
        	.applyAffineTransform(AffineTransformation.translationInstance( 2, 3 ));
        // END SNIPPET: s_affine_transformation
        addImageSnippet(boxesLayer, pipeline, getTitle());    	
    	
        GeoPipeline original = GeoPipeline.start( osmLayer ).copyDatabaseRecordProperties().sort(
                "name" );

        GeoPipeline translated = GeoPipeline.start( osmLayer ).applyAffineTransform(
                AffineTransformation.translationInstance( 10, 25 ) ).copyDatabaseRecordProperties().sort(
                "name" );

        for ( int k = 0; k < 2; k++ )
        {
            Coordinate[] coords = original.next().getGeometry().getCoordinates();
            Coordinate[] newCoords = translated.next().getGeometry().getCoordinates();
            assertEquals( coords.length, newCoords.length );
            for ( int i = 0; i < coords.length; i++ )
            {
                assertEquals( coords[i].x + 10, newCoords[i].x, 0 );
                assertEquals( coords[i].y + 25, newCoords[i].y, 0 );
            }
        }
    }

    @Test
    public void calculate_area()
    {
        GeoPipeline pipeline = GeoPipeline.start( boxesLayer ).calculateArea().sort(
                "Area" );

        assertEquals( (Double) pipeline.next().getProperties().get( "Area" ),
                1.0, 0 );
        assertEquals( (Double) pipeline.next().getProperties().get( "Area" ),
                8.0, 0 );
    }

    @Test
    public void calculate_length()
    {
        GeoPipeline pipeline = GeoPipeline.start( boxesLayer ).calculateLength().sort(
                "Length" );

        assertEquals( (Double) pipeline.next().getProperties().get( "Length" ),
                4.0, 0 );
        assertEquals( (Double) pipeline.next().getProperties().get( "Length" ),
                12.0, 0 );
    }

    @Test
    public void get_boundary_length()
    {
        GeoPipeline pipeline = GeoPipeline.start( boxesLayer ).toBoundary().createWellKnownText().calculateLength().sort(
                "Length" );

        GeoPipeFlow first = pipeline.next();
        GeoPipeFlow second = pipeline.next();
        assertEquals( "LINEARRING (12 26, 12 27, 13 27, 13 26, 12 26)",
                first.getProperties().get( "WellKnownText" ) );
        assertEquals( "LINEARRING (2 3, 2 5, 6 5, 6 3, 2 3)",
                second.getProperties().get( "WellKnownText" ) );
        assertEquals( (Double) first.getProperties().get( "Length" ), 4.0, 0 );
        assertEquals( (Double) second.getProperties().get( "Length" ), 12.0, 0 );
    }

    /**
     * Buffer
     * 
     * The Buffer pipe applies a buffer to geometries.
     * 
     * Example:
     * 
     * @@s_buffer
     * 
     * Output:
     * 
     * @@buffer
     */
    @Documented      
    @Test
    public void get_buffer()
    {
    	// START SNIPPET: s_buffer
        GeoPipeline pipeline = GeoPipeline.start( boxesLayer ).toBuffer( 0.5 );
        // END SNIPPET: s_buffer
        addImageSnippet(boxesLayer, pipeline, getTitle());    	
    	
        pipeline = GeoPipeline.start( boxesLayer ).toBuffer( 0.1 ).createWellKnownText().calculateArea().sort(
                "Area" );

        assertTrue( ( (Double) pipeline.next().getProperties().get( "Area" ) ) > 1 );
        assertTrue( ( (Double) pipeline.next().getProperties().get( "Area" ) ) > 8 );
    }

    /**
     * Centroid
     * 
     * The Centroid pipe calculates geometry centroid.
     * 
     * Example:
     * 
     * @@s_centroid
     * 
     * Output:
     * 
     * @@centroid
     */
    @Documented     
    @Test
    public void get_centroid()
    {
    	// START SNIPPET: s_centroid
        GeoPipeline pipeline = GeoPipeline.start( boxesLayer ).toCentroid();
        // END SNIPPET: s_centroid
        addImageSnippet(boxesLayer, pipeline, getTitle(), Constants.GTYPE_POINT);
    	
        pipeline = GeoPipeline.start( boxesLayer ).toCentroid().createWellKnownText().copyDatabaseRecordProperties().sort(
                "name" );

        assertEquals( "POINT (12.5 26.5)",
                pipeline.next().getProperties().get( "WellKnownText" ) );
        assertEquals( "POINT (4 4)",
                pipeline.next().getProperties().get( "WellKnownText" ) );
    }
    
    /**
     * This pipe exports every 
     * geometry as a http://en.wikipedia.org/wiki/Geography_Markup_Language[GML] snippet.
     * 
     * Example:
     * 
     * @@s_export_to_gml
     * 
     * Output:
     * 
     * @@exportgml
     */
    @Documented     
    @Test
    public void export_to_GML()
    {
        // START SNIPPET: s_export_to_gml
        GeoPipeline pipeline = GeoPipeline.start( boxesLayer ).createGML();
        for ( GeoPipeFlow flow : pipeline ) {
            System.out.println(flow.getProperties().get( "GML" ));
         }
        // END SNIPPET: s_export_to_gml
        String result = "";
        for ( GeoPipeFlow flow : GeoPipeline.start( boxesLayer ).createGML() ) {
            result = result + flow.getProperties().get( "GML" );
        }
        gen.get().addSnippet( "exportgml", "[source,xml]\n----\n"+result+"\n----\n" );
    }

    /**
     * Convex Hull
     * 
     * The ConvexHull pipe calculates geometry convex hull.
     * 
     * Example:
     * 
     * @@s_convex_hull
     * 
     * Output:
     * 
     * @@convex_hull
     */
    @Documented     
    @Test
    public void get_convex_hull()
    {
    	// START SNIPPET: s_convex_hull
        GeoPipeline pipeline = GeoPipeline.start( concaveLayer ).toConvexHull();
        // END SNIPPET: s_convex_hull
        addImageSnippet(concaveLayer, pipeline, getTitle());
        
        pipeline = GeoPipeline.start( concaveLayer ).toConvexHull().createWellKnownText();

        assertEquals( "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
                pipeline.next().getProperties().get( "WellKnownText" ) );
    }

    /**
     * Densify 
     * 
     * The Densify pipe inserts extra vertices along the line segments in the geometry. 
	 * The densified geometry contains no line segment which is longer than the given distance tolerance.
	 * 
     * Example:
     * 
     * @@s_densify
     * 
     * Output:
     * 
     * @@densify
     */
    @Documented    
    @Test
    public void densify()
    {
    	// START SNIPPET: s_densify
        GeoPipeline pipeline = GeoPipeline.start( concaveLayer ).densify( 5 ).extractPoints();
        // END SNIPPET: s_densify
        addImageSnippet(concaveLayer, pipeline, getTitle(), Constants.GTYPE_POINT);
    	
    	
        pipeline = GeoPipeline.start( concaveLayer ).toConvexHull().densify( 10 ).createWellKnownText();

        assertEquals(
                "POLYGON ((0 0, 0 5, 0 10, 5 10, 10 10, 10 5, 10 0, 5 0, 0 0))",
                pipeline.next().getProperties().get( "WellKnownText" ) );
    }

    @Test
    public void json()
    {
        GeoPipeline pipeline = GeoPipeline.start( boxesLayer ).createJson().copyDatabaseRecordProperties().sort(
                "name" );

        assertEquals(
                "{\"type\":\"Polygon\",\"coordinates\":[[[12,26],[12,27],[13,27],[13,26],[12,26]]]}",
                pipeline.next().getProperties().get( "GeoJSON" ) );
        assertEquals(
                "{\"type\":\"Polygon\",\"coordinates\":[[[2,3],[2,5],[6,5],[6,3],[2,3]]]}",
                pipeline.next().getProperties().get( "GeoJSON" ) );
    }

    /**
     * Max
     * 
     * The Max pipe computes the maximum value of the specified property and
     * discard items with a value less than the maximum.
	 * 
     * Example:
     * 
     * @@s_max
     * 
     * Output:
     * 
     * @@max
     */
    @Documented
    @Test
    public void get_max_area()
    {
    	// START SNIPPET: s_max
        GeoPipeline pipeline = GeoPipeline.start( boxesLayer )
        	.calculateArea()
        	.getMax( "Area" );
        // END SNIPPET: s_max
        addImageSnippet( boxesLayer, pipeline, getTitle() );
    	
        pipeline = GeoPipeline.start( boxesLayer ).calculateArea().getMax(
                "Area" );
        assertEquals( (Double) pipeline.next().getProperties().get( "Area" ),
                8.0, 0 );
    }

    /**
     * The 
     * boundary pipe calculates boundary of every geometry in the pipeline.
	 * 
     * Example:
     * 
     * @@s_boundary
     * 
     * Output:
     * 
     * @@boundary
     */
    @Documented    
    @Test
    public void boundary()
    {
    	// START SNIPPET: s_boundary
        GeoPipeline pipeline = GeoPipeline.start( boxesLayer ).toBoundary();
        // END SNIPPET: s_boundary
        addImageSnippet( boxesLayer, pipeline, getTitle(), Constants.GTYPE_LINESTRING );

        // TODO test?
    }

    /**
     * Difference
     * 
     * The Difference pipe computes a geometry representing 
     * the points making up item geometry that do not make up the given geometry.
	 * 
     * Example:
     * 
     * @@s_difference
     * 
     * Output:
     * 
     * @@difference
     */
    @Documented    
    @Test
    public void difference() throws Exception 
    {
    	// START SNIPPET: s_difference
    	WKTReader reader = new WKTReader( intersectionLayer.getGeometryFactory() );
        Geometry geometry = reader.read( "POLYGON ((3 3, 3 5, 7 7, 7 3, 3 3))" );
        GeoPipeline pipeline = GeoPipeline.start( intersectionLayer ).difference( geometry );
        // END SNIPPET: s_difference
        addImageSnippet( intersectionLayer, pipeline, getTitle() );

        // TODO test?
    }
     
    /**
     * Intersection
     * 
     * The Intersection pipe computes a geometry representing the intersection between item geometry and the given geometry.
	 * 
     * Example:
     * 
     * @@s_intersection
     * 
     * Output:
     * 
     * @@intersection
     */
    @Documented    
    @Test
    public void intersection() throws Exception 
    {
    	// START SNIPPET: s_intersection
    	WKTReader reader = new WKTReader( intersectionLayer.getGeometryFactory() );
        Geometry geometry = reader.read( "POLYGON ((3 3, 3 5, 7 7, 7 3, 3 3))" );
        GeoPipeline pipeline = GeoPipeline.start( intersectionLayer ).intersect( geometry );
        // END SNIPPET: s_intersection
        addImageSnippet( intersectionLayer, pipeline, getTitle() );

        // TODO test?
    }    
    
    /**
     * Union
     * 
     * The Union pipe unites item geometry with a given geometry.
	 * 
     * Example:
     * 
     * @@s_union
     * 
     * Output:
     * 
     * @@union
     */
    @Documented    
    @Test
    public void union() throws Exception 
    {
    	// START SNIPPET: s_union
    	WKTReader reader = new WKTReader( intersectionLayer.getGeometryFactory() );
        Geometry geometry = reader.read( "POLYGON ((3 3, 3 5, 7 7, 7 3, 3 3))" );
        SearchFilter filter = new SearchIntersectWindow( intersectionLayer, new Envelope( 7, 10, 7, 10 ) );
        GeoPipeline pipeline = GeoPipeline.start( intersectionLayer, filter ).union( geometry );
        // END SNIPPET: s_union
        addImageSnippet( intersectionLayer, pipeline, getTitle() );

        // TODO test?
    }    
        
    /**
     * Min
     * 
     * The Min pipe computes the minimum value of the specified property and
     * discard items with a value greater than the minimum.
	 * 
     * Example:
     * 
     * @@s_min
     * 
     * Output:
     * 
     * @@min
     */
    @Documented    
    @Test
    public void get_min_area()
    {
    	// START SNIPPET: s_min
        GeoPipeline pipeline = GeoPipeline.start( boxesLayer )
        	.calculateArea()
        	.getMin( "Area" );
        // END SNIPPET: s_min
        addImageSnippet( boxesLayer, pipeline, getTitle() );
        
        pipeline = GeoPipeline.start( boxesLayer ).calculateArea().getMin(
                "Area" );
        assertEquals( (Double) pipeline.next().getProperties().get( "Area" ),
                1.0, 0 );
    }

    @Test
    public void extract_osm_points()
    {
        int count = 0;
        GeoPipeline pipeline = OSMGeoPipeline.startOsm( osmLayer ).extractOsmPoints().createWellKnownText();
        for ( GeoPipeFlow flow : pipeline )
        {
            count++;

            assertEquals( 1, flow.getProperties().size() );
            String wkt = (String) flow.getProperties().get( "WellKnownText" );
            assertTrue( wkt.indexOf( "POINT" ) == 0 );
        }

        assertEquals( 24, count );
    }
    
    /**
     * A more complex Open Street Map example.
     * 
     * This example demostrates the some pipes chained together to make a full
     * geoprocessing pipeline.
     * 
     * Example:
     * 
     * @@s_break_up_all_geometries_into_points_and_make_density_islands
     * 
     * _Step1_
     * 
     * @@step1_break_up_all_geometries_into_points_and_make_density_islands
     * 
     * _Step2_
     * 
     * @@step2_break_up_all_geometries_into_points_and_make_density_islands
     * 
     * _Step3_
     * 
     * @@step3_break_up_all_geometries_into_points_and_make_density_islands
     * 
     * _Step4_
     * 
     * @@step4_break_up_all_geometries_into_points_and_make_density_islands
     *
     * _Step5_
     * 
     * @@step5_break_up_all_geometries_into_points_and_make_density_islands
     */
    @Documented  
    @Title("break_up_all_geometries_into_points_and_make_density_islands")
    @Test
    public void break_up_all_geometries_into_points_and_make_density_islands_and_get_the_outer_linear_ring_of_the_density_islands_and_buffer_the_geometry_and_count_them()
    {
        // START SNIPPET: s_break_up_all_geometries_into_points_and_make_density_islands
        //step1
        GeoPipeline pipeline = OSMGeoPipeline.startOsm( osmLayer )
                //step2
        	.extractOsmPoints()
        	//step3
        	.groupByDensityIslands( 0.0005 )
        	//step4
        	.toConvexHull()
        	//step5
        	.toBuffer( 0.0004 );
        // END SNIPPET: s_break_up_all_geometries_into_points_and_make_density_islands
        	
        assertEquals( 9, pipeline.count() );
       
        addOsmImageSnippet( osmLayer, OSMGeoPipeline.startOsm( osmLayer ), "step1_"+getTitle(), Constants.GTYPE_LINESTRING );        
        addOsmImageSnippet( osmLayer, OSMGeoPipeline.startOsm( osmLayer ).extractOsmPoints(), "step2_"+getTitle(), Constants.GTYPE_POINT );
        addOsmImageSnippet( osmLayer, OSMGeoPipeline.startOsm( osmLayer ).extractOsmPoints().groupByDensityIslands( 0.0005 ), "step3_"+getTitle(), Constants.GTYPE_POLYGON );
        addOsmImageSnippet( osmLayer, OSMGeoPipeline.startOsm( osmLayer ).extractOsmPoints().groupByDensityIslands( 0.0005 ).toConvexHull(), "step4_"+getTitle(), Constants.GTYPE_POLYGON );
        addOsmImageSnippet( osmLayer, OSMGeoPipeline.startOsm( osmLayer ).extractOsmPoints().groupByDensityIslands( 0.0005 ).toConvexHull().toBuffer( 0.0004 ), "step5_"+getTitle(), Constants.GTYPE_POLYGON );
    }

    /**
     * Extract Points
     * 
     * The Extract Points pipe extracts every point from a geometry.
     * 
     * Example:
     * 
     * @@s_extract_points
     * 
     * Output:
     * 
     * @@extract_points
     */
    @Documented    
    @Test
    public void extract_points()
    {
    	// START SNIPPET: s_extract_points
        GeoPipeline pipeline = GeoPipeline.start( boxesLayer ).extractPoints();
        // END SNIPPET: s_extract_points
        addImageSnippet(boxesLayer, pipeline, getTitle(), Constants.GTYPE_POINT);
    	
        int count = 0;
        for ( GeoPipeFlow flow : GeoPipeline.start( boxesLayer ).extractPoints().createWellKnownText() )
        {
            count++;

            assertEquals( 1, flow.getProperties().size() );
            String wkt = (String) flow.getProperties().get( "WellKnownText" );
            assertTrue( wkt.indexOf( "POINT" ) == 0 );
        }

        // every rectangle has 5 points, the last point is in the same position of the first
        assertEquals( 10, count );
    }

    @Test
    public void filter_by_null_property()
    {
        assertEquals(
                2,
                GeoPipeline.start( boxesLayer ).copyDatabaseRecordProperties().propertyNullFilter(
                        "address" ).count() );
        assertEquals(
                0,
                GeoPipeline.start( boxesLayer ).copyDatabaseRecordProperties().propertyNullFilter(
                        "name" ).count() );
    }

    @Test
    public void filter_by_not_null_property()
    {
        assertEquals(
                0,
                GeoPipeline.start( boxesLayer ).copyDatabaseRecordProperties().propertyNotNullFilter(
                        "address" ).count() );
        assertEquals(
                2,
                GeoPipeline.start( boxesLayer ).copyDatabaseRecordProperties().propertyNotNullFilter(
                        "name" ).count() );
    }

    @Test
    public void compute_distance() throws ParseException
    {
        WKTReader reader = new WKTReader( boxesLayer.getGeometryFactory() );

        GeoPipeline pipeline = GeoPipeline.start( boxesLayer ).calculateDistance(
                reader.read( "POINT (0 0)" ) ).sort( "Distance" );

        assertEquals(
                4, Math.round( (Double) pipeline.next().getProperty( "Distance" ) ) );
        assertEquals(
                29, Math.round( (Double) pipeline.next().getProperty( "Distance" ) ) );
    }

    /**
     * Unite All 
     * 
     * The Union All pipe unites geometries of every item contained in the pipeline.
	 * This pipe groups every item in the pipeline in a single item containing the geometry output
	 * of the union.
     * 
     * Example:
     * 
     * @@s_unite_all
     * 
     * Output:
     * 
     * @@unite_all
     */
    @Documented
    @Test
    public void unite_all()
    {
    	// START SNIPPET: s_unite_all
        GeoPipeline pipeline = GeoPipeline.start( intersectionLayer ).unionAll();
        // END SNIPPET: s_unite_all
        addImageSnippet( intersectionLayer, pipeline, getTitle() );
        
        pipeline = GeoPipeline.start( intersectionLayer )
	    	.unionAll()
	    	.createWellKnownText();
        
        assertEquals(
                "POLYGON ((0 0, 0 5, 2 5, 2 6, 4 6, 4 10, 10 10, 10 4, 6 4, 6 2, 5 2, 5 0, 0 0))",
                pipeline.next().getProperty( "WellKnownText" ) );

        try
        {
            pipeline.next();
            fail();
        }
        catch ( NoSuchElementException e )
        {
        }
    }
    
    /**
     * Intersect All
     * 
     * The IntersectAll pipe intersects geometries of every item contained in the pipeline.
     * It groups every item in the pipeline in a single item containing the geometry output
     * of the intersection.
     * 
     * Example:  
     * 
     * @@s_intersect_all
     * 
     * Output:
     * 
     * @@intersect_all
     */
    @Documented    
    @Test
    public void intersect_all()
    {
    	// START SNIPPET: s_intersect_all
        GeoPipeline pipeline = GeoPipeline.start( intersectionLayer ).intersectAll();
        // END SNIPPET: s_intersect_all
        addImageSnippet( intersectionLayer, pipeline, getTitle() );
        
        pipeline = GeoPipeline.start( intersectionLayer )
	    	.intersectAll()
	    	.createWellKnownText();
        
        assertEquals( "POLYGON ((4 5, 5 5, 5 4, 4 4, 4 5))",
                pipeline.next().getProperty( "WellKnownText" ) );
        
        try
        {
            pipeline.next();
            fail();
        }
        catch ( NoSuchElementException e )
        {
        }
    }

    /**
     * Intersecting windows
     * 
     * The FilterIntersectWindow pipe finds geometries that intersects a given rectangle.
     * This pipeline:
     * 
     * @@s_intersecting_windows
     * 
     * will output:
     * 
     * @@intersecting_windows
     */
    @Documented
    @Test
    public void intersecting_windows()
    {
        // START SNIPPET: s_intersecting_windows
        GeoPipeline pipeline = GeoPipeline
        	.start( boxesLayer )
        	.windowIntersectionFilter(new Envelope( 0, 10, 0, 10 ) );
        // END SNIPPET: s_intersecting_windows
        addImageSnippet( boxesLayer, pipeline, getTitle() );
        
        // TODO test?
    }

    /**
     * Start Point
     * 
     * The StartPoint pipe finds the starting point of item geometry.
     * Example:
     * 
     * @@s_start_point
     * 
     * Output:
     * 
     * @@start_point
     */
    @Documented
    @Test
    public void start_point()
    {
        // START SNIPPET: s_start_point
        GeoPipeline pipeline = GeoPipeline
        	.start( linesLayer )
        	.toStartPoint();
        // END SNIPPET: s_start_point
        addImageSnippet( linesLayer, pipeline, getTitle(), Constants.GTYPE_POINT );
        
        pipeline = GeoPipeline
	    	.start( linesLayer )
	    	.toStartPoint()
	    	.createWellKnownText();
        
        assertEquals("POINT (12 26)", pipeline.next().getProperty("WellKnownText"));
    }    
    
    /**
     * End Point
     * 
     * The EndPoint pipe finds the ending point of item geometry.
     * Example:
     * 
     * @@s_end_point
     * 
     * Output:
     * 
     * @@end_point
     */
    @Documented
    @Test
    public void end_point()
    {
        // START SNIPPET: s_end_point
        GeoPipeline pipeline = GeoPipeline
        	.start( linesLayer )
        	.toEndPoint();
        // END SNIPPET: s_end_point
        addImageSnippet( linesLayer, pipeline, getTitle(), Constants.GTYPE_POINT );
        
        pipeline = GeoPipeline
	    	.start( linesLayer )
	    	.toEndPoint()
	    	.createWellKnownText();
    
	    assertEquals("POINT (23 34)", pipeline.next().getProperty("WellKnownText"));
    }    
    
    /**
     * Envelope
     * 
     * The Envelope pipe computes the minimum bounding box of item geometry.
     * Example:
     * 
     * @@s_envelope
     * 
     * Output:
     * 
     * @@envelope
     */
    @Documented
    @Test
    public void envelope()
    {
        // START SNIPPET: s_envelope
        GeoPipeline pipeline = GeoPipeline
        	.start( linesLayer )
        	.toEnvelope();
        // END SNIPPET: s_envelope
        addImageSnippet( linesLayer, pipeline, getTitle(), Constants.GTYPE_POLYGON );
        
        // TODO test
    } 
    
    @Test
    public void test_equality() throws Exception
    {
        WKTReader reader = new WKTReader( equalLayer.getGeometryFactory() );
        Geometry geom = reader.read( "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))" );

        GeoPipeline pipeline = GeoPipeline.startEqualExactSearch( equalLayer,
                geom, 0 ).copyDatabaseRecordProperties();
        assertEquals( "equal", pipeline.next().getProperty( "name" ) );
        assertFalse( pipeline.hasNext() );

        pipeline = GeoPipeline.startEqualExactSearch( equalLayer, geom, 0.1 ).copyDatabaseRecordProperties().sort(
                "id" );
        assertEquals( "equal", pipeline.next().getProperty( "name" ) );
        assertEquals( "tolerance", pipeline.next().getProperty( "name" ) );
        assertFalse( pipeline.hasNext() );

        pipeline = GeoPipeline.startIntersectWindowSearch( equalLayer,
                geom.getEnvelopeInternal() ).equalNormFilter( geom, 0.1 ).copyDatabaseRecordProperties().sort(
                "id" );
        assertEquals( "equal", pipeline.next().getProperty( "name" ) );
        assertEquals( "tolerance", pipeline.next().getProperty( "name" ) );
        assertEquals( "different order", pipeline.next().getProperty( "name" ) );
        assertFalse( pipeline.hasNext() );

        pipeline = GeoPipeline.startIntersectWindowSearch( equalLayer,
                geom.getEnvelopeInternal() ).equalTopoFilter( geom ).copyDatabaseRecordProperties().sort(
                "id" );
        assertEquals( "equal", pipeline.next().getProperty( "name" ) );
        assertEquals( "different order", pipeline.next().getProperty( "name" ) );
        assertEquals( "topo equal", pipeline.next().getProperty( "name" ) );
        assertFalse( pipeline.hasNext() );
    }

    private String getTitle()
    {
        return gen.get().getTitle().replace( " ", "_" ).toLowerCase();
    }    

	private void addImageSnippet(
    		Layer layer,
            GeoPipeline pipeline,
            String imgName )
    {
		addImageSnippet( layer, pipeline, imgName, null );
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private void addOsmImageSnippet(
    		Layer layer,
            GeoPipeline pipeline,
            String imgName,
            Integer geomType )
    {
		addImageSnippet( layer, pipeline, imgName, geomType, 0.002 );
    }

	private void addImageSnippet(
    		Layer layer,
            GeoPipeline pipeline,
            String imgName,
            Integer geomType )
    {
		addImageSnippet( layer, pipeline, imgName, geomType, 1 );
    }
	
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private void addImageSnippet(
    		Layer layer,
            GeoPipeline pipeline,
            String imgName,
            Integer geomType, 
            double boundsDelta )
    {
        gen.get().addSnippet( imgName, "\nimage::" + imgName + ".png[scaledwidth=\"75%\"]\n" );
        
        try
        {            
        	FeatureCollection layerCollection = GeoPipeline.start(layer, new SearchAll()).toFeatureCollection();
        	FeatureCollection pipelineCollection;
        	if (geomType == null) {
        		pipelineCollection = pipeline.toFeatureCollection();
        	} else {
        		pipelineCollection = pipeline.toFeatureCollection(
        				Neo4jFeatureBuilder.getType(layer.getName(), geomType, layer.getCoordinateReferenceSystem(), layer.getExtraPropertyNames()));
        	}
        	
        	ReferencedEnvelope bounds = layerCollection.getBounds();
        	bounds.expandToInclude(pipelineCollection.getBounds());
        	bounds.expandBy(boundsDelta, boundsDelta);
        	
            StyledImageExporter exporter = new StyledImageExporter( db );
            exporter.setExportDir( "target/docs/images/" );
            exporter.saveImage(
            		new FeatureCollection[] {
            				layerCollection,
            				pipelineCollection,
            		},
                    new Style[] { 
            			StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN), 
            			StyledImageExporter.createDefaultStyle(Color.RED, Color.ORANGE)
            		},
            		new File( imgName + ".png" ), 
            		bounds);
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    private static void load() throws Exception
    {
        SpatialDatabaseService spatialService = new SpatialDatabaseService( db );

        try (Transaction tx = db.beginTx()) {
            loadTestOsmData( "two-street.osm", 100 );
            osmLayer = spatialService.getLayer( "two-street.osm" );
        
	        boxesLayer = (EditableLayerImpl) spatialService.getOrCreateEditableLayer( "boxes" );
	        boxesLayer.setExtraPropertyNames( new String[] { "name" } );
	        boxesLayer.setCoordinateReferenceSystem(DefaultEngineeringCRS.GENERIC_2D);
	        WKTReader reader = new WKTReader( boxesLayer.getGeometryFactory() );
	        boxesLayer.add(
	                reader.read( "POLYGON ((12 26, 12 27, 13 27, 13 26, 12 26))" ),
	                new String[] { "name" }, new Object[] { "A" } );
	        boxesLayer.add( reader.read( "POLYGON ((2 3, 2 5, 6 5, 6 3, 2 3))" ),
	                new String[] { "name" }, new Object[] { "B" } );
	
	        concaveLayer = (EditableLayerImpl) spatialService.getOrCreateEditableLayer( "concave" );
	        concaveLayer.setCoordinateReferenceSystem(DefaultEngineeringCRS.GENERIC_2D);
	        reader = new WKTReader( concaveLayer.getGeometryFactory() );
	        concaveLayer.add( reader.read( "POLYGON ((0 0, 2 5, 0 10, 10 10, 10 0, 0 0))" ) );
	
	        intersectionLayer = (EditableLayerImpl) spatialService.getOrCreateEditableLayer( "intersection" );
	        intersectionLayer.setCoordinateReferenceSystem(DefaultEngineeringCRS.GENERIC_2D);
	        reader = new WKTReader( intersectionLayer.getGeometryFactory() );
	        intersectionLayer.add( reader.read( "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))" ) );
	        intersectionLayer.add( reader.read( "POLYGON ((4 4, 4 10, 10 10, 10 4, 4 4))" ) );
	        intersectionLayer.add( reader.read( "POLYGON ((2 2, 2 6, 6 6, 6 2, 2 2))" ) );
	
	        equalLayer = (EditableLayerImpl) spatialService.getOrCreateEditableLayer( "equal" );
	        equalLayer.setExtraPropertyNames( new String[] { "id", "name" } );
	        equalLayer.setCoordinateReferenceSystem(DefaultEngineeringCRS.GENERIC_2D);	        
	        reader = new WKTReader( intersectionLayer.getGeometryFactory() );
	        equalLayer.add( reader.read( "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))" ),
	                new String[] { "id", "name" }, new Object[] { 1, "equal" } );
	        equalLayer.add( reader.read( "POLYGON ((0 0, 0.1 5, 5 5, 5 0, 0 0))" ),
	                new String[] { "id", "name" }, new Object[] { 2, "tolerance" } );
	        equalLayer.add( reader.read( "POLYGON ((0 5, 5 5, 5 0, 0 0, 0 5))" ),
	                new String[] { "id", "name" }, new Object[] { 3,
	                        "different order" } );
	        equalLayer.add(
	                reader.read( "POLYGON ((0 0, 0 2, 0 4, 0 5, 5 5, 5 3, 5 2, 5 0, 0 0))" ),
	                new String[] { "id", "name" }, new Object[] { 4, "topo equal" } );

	        linesLayer = (EditableLayerImpl) spatialService.getOrCreateEditableLayer( "lines" );
	        linesLayer.setCoordinateReferenceSystem(DefaultEngineeringCRS.GENERIC_2D);	        
	        reader = new WKTReader( intersectionLayer.getGeometryFactory() );
	        linesLayer.add( reader.read( "LINESTRING (12 26, 15 27, 18 32, 20 38, 23 34)" ) );
	        
	        tx.success();
        }
    }

    private static void loadTestOsmData( String layerName, int commitInterval )
            throws Exception
    {
        String osmPath = "./" + layerName;
        System.out.println( "\n=== Loading layer " + layerName + " from "
                            + osmPath + " ===" );
        OSMImporter importer = new OSMImporter( layerName );
        importer.setCharset( Charset.forName( "UTF-8" ) );
        importer.importFile( db, osmPath );
        importer.reIndex( db, commitInterval );
    }

    @Before
    public void setUp()
    {
        gen.get().setGraph( db );
        try (Transaction tx = db.beginTx())
        {
            StyledImageExporter exporter = new StyledImageExporter( db );
            exporter.setExportDir( "target/docs/images/" );
            exporter.saveImage( GeoPipeline.start( intersectionLayer ).toFeatureCollection(),
                    StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN), new File(
                            "intersectionLayer.png" ) );
            
            exporter.saveImage( GeoPipeline.start( boxesLayer ).toFeatureCollection(),
                    StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN), new File(
                            "boxesLayer.png" ) );

            exporter.saveImage( GeoPipeline.start( concaveLayer ).toFeatureCollection(),
                    StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN), new File(
                            "concaveLayer.png" ) );

            exporter.saveImage( GeoPipeline.start( equalLayer ).toFeatureCollection(),
                    StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN), new File(
                            "equalLayer.png" ) );
            exporter.saveImage( GeoPipeline.start( linesLayer ).toFeatureCollection(),
                    StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN), new File(
                            "linesLayer.png" ) );
            exporter.saveImage( GeoPipeline.start( osmLayer ).toFeatureCollection(),
                    StyledImageExporter.createDefaultStyle(Color.BLUE, Color.CYAN), new File(
                            "osmLayer.png" ) );
            tx.success();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        tx = db.beginTx();
    }

    @After
    public void doc()
    {
       // gen.get().addSnippet( "graph", AsciidocHelper.createGraphViz( imgName , graphdb(), "graph"+getTitle() ) );
       gen.get().addTestSourceSnippets( GeoPipesTest.class, "s_"+getTitle().toLowerCase() );
       gen.get().document( "target/docs", "examples" );
       if (tx!=null) {
           tx.success(); tx.close();
       }
    }

    @BeforeClass
    public static void init()
    {
        db = new TestGraphDatabaseFactory( ).newImpermanentDatabase();
        ((ImpermanentGraphDatabase)db).cleanContent(  );
        try
        {
            load();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        
        StyledImageExporter exporter = new StyledImageExporter( db );
        exporter.setExportDir( "target/docs/images/" );
    }

    private GeoPipeFlow print( GeoPipeFlow pipeFlow )
    {
        System.out.println( "GeoPipeFlow:" );
        for ( String key : pipeFlow.getProperties().keySet() )
        {
            System.out.println( key + "=" + pipeFlow.getProperties().get( key ) );
        }
        System.out.println( "-" );
        return pipeFlow;
    }
}
