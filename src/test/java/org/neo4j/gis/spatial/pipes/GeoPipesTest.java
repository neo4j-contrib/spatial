/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.JavaTestDocsGenerator;
import org.neo4j.test.TestData;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

import com.tinkerpop.pipes.filter.FilterPipe.Filter;

public class GeoPipesTest implements GraphHolder
{
    public @Rule
    TestData<JavaTestDocsGenerator> gen = TestData.producedThrough( JavaTestDocsGenerator.PRODUCER );
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );

    private static ImpermanentGraphDatabase graphdb;
    private static Layer layer = null;
    public final static String LAYER_NAME = "two-street.osm";
    public final static int COMMIT_INTERVAL = 100;

    /**
     * this is the documentation
     * for this test.
     * 
     * @@graph
     * 
     * This is the pipe executed over the above graph.
     * 
     * @@pipe1
     * 
     * Returning all geometries in this layer.
     * 
     */
    @Documented
    @Test
    public void count_all_geometries_in_a_layer()
    {
        data.get();
        gen.get().addSnippet( "pipe1", gen.get().createSourceSnippet("pipe1", this.getClass()) );
        gen.get().addSnippet( "graph", AsciidocHelper.createGraphViz("graph1", graphdb(), gen.get().getTitle()) );
        // START SNIPPET: pipe1
        long allGeometries = layer.filter().all().count();
        assertEquals( 2, allGeometries );
        // END SNIPPET: pipe1
    }
    @Test
    public void count_number_of_points_in_all_geometries_in_a_layer()
    {
        assertEquals( 24, layer.filter().all().process().countPoints() );
    }
    
    @Test
    public void break_up_all_geometries_into_points_and_count_them()
    {
        assertEquals( 24, layer.filter().all().process().toPoints().count() );
    }
    
    @Test
    public void count_all_ways_with_a_specific_name()
    {
    	assertEquals( 1, layer.filter().all().attributes("name", "Storgatan", Filter.EQUAL).count() );
    }

    @Test
    public void count_all_geometries_with_in_a_specific_bbox()
    {
    	assertEquals( 1, layer.filter().all().bbox(10, 40, 20, 56.0583531).count());
    }
    
    @Test
    public void count_all_geometries_with_in_a_specific_bbox_with_cql()
    {
    	assertEquals( 2, layer.filter().all().cql("BBOX(the_geom, 10, 40, 20, 57)").count());
    }
    
    @Test
    public void break_up_all_geometries_into_points_and_make_density_islands_and_get_the_outer_linear_ring_of_the_density_islands_and_buffer_the_geometry_and_count_them()
    {
    	assertEquals( 1, layer.filter().all().process().toPoints().toDensityIslands(0.1).toOutherLinearRing().buffer(10).count());
    	System.out.println(layer.filter().all().process().toPoints().toDensityIslands(0.1).toOutherLinearRing().buffer(10).next());
    }
    
    public static void load( ) throws Exception
    {
        try
        {
            loadTestOsmData( LAYER_NAME, COMMIT_INTERVAL );
            SpatialDatabaseService spatialService = new SpatialDatabaseService(
                    graphdb );
            layer = spatialService.getLayer( LAYER_NAME );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    public static void loadTestOsmData( String layerName, int commitInterval )
            throws Exception
    {
        String osmPath = "./" + layerName;
        System.out.println( "\n=== Loading layer " + layerName + " from "
                            + osmPath + " ===" );
        OSMImporter importer = new OSMImporter( layerName );
        importer.importFile( graphdb, osmPath );
        importer.reIndex( graphdb, commitInterval );
    }

    @BeforeClass
    public static void setUpCLass() throws Exception
    {
        graphdb = new ImpermanentGraphDatabase();
        load();
    }
    @Override
    public GraphDatabaseService graphdb()
    {
        return graphdb;
    }
    
    @After
    public void doc() {
        gen.get().document("target/docs","examples");
    }
    
    @Before
    public void setUp() {
        graphdb.cleanContent();
        gen.get().setGraph( graphdb );
    }

}
