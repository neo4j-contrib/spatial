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

import org.geotools.filter.text.cql2.CQLException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.collections.rtree.filter.SearchAll;
import org.neo4j.collections.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.pipes.osm.OSMGeoPipeline;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestData;


public class GeoPipesTest implements GraphHolder {
	
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough(GraphDescription.createGraphFor(this, true));

    private static ImpermanentGraphDatabase graphdb;
    private static Layer layer = null;
    public final static String LAYER_NAME = "two-street.osm";
    public final static int COMMIT_INTERVAL = 100;
    
    private OSMGeoPipeline startPipeline() {
    	return startPipeline(new SearchAll());
    }
    
    private OSMGeoPipeline startPipeline(SearchFilter filter) {
    	return OSMGeoPipeline.start(layer, layer.getIndex().search(filter));
    }    
    
    @Test
    public void count_all_geometries_in_a_layer() {
        assertEquals(2, startPipeline().count());
    }    
    
    @Test
    public void break_up_all_geometries_into_points_and_count_them() {
        assertEquals(24, startPipeline().extractOsmPoints().count());
    }
    
    @Test
    public void count_all_ways_with_a_specific_name() {
    	assertEquals(1, startPipeline().filterByOsmAttribute("name", "Storgatan").count());
    }

    @Test
    public void count_all_geometries_with_in_a_specific_bbox() {
    	assertEquals(1, startPipeline().filterByWindowIntersection(10, 40, 20, 56.0583531).count());
    }
    
    @Test
    public void count_all_geometries_with_in_a_specific_bbox_with_cql() throws CQLException {
    	assertEquals(2, startPipeline().filterByCQL("BBOX(the_geom, 10, 40, 20, 57)").count());
    }
    
    @Test
    public void break_up_all_geometries_into_points_and_make_density_islands_and_get_the_outer_linear_ring_of_the_density_islands_and_buffer_the_geometry_and_count_them() {
    	assertEquals(1, startPipeline()
    			.extractOsmPoints().groupByDensityIslands(0.1).toConvexHull().buffer(10).count());
    	System.out.println(startPipeline()
    			.extractOsmPoints().groupByDensityIslands(0.1).toConvexHull().buffer(10).count());
    }
    
    public static void load() throws Exception {
        loadTestOsmData(LAYER_NAME, COMMIT_INTERVAL);
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphdb);
        layer = spatialService.getLayer(LAYER_NAME);
    }

    public static void loadTestOsmData(String layerName, int commitInterval) throws Exception {
        String osmPath = "./" + layerName;
        System.out.println("\n=== Loading layer " + layerName + " from " + osmPath + " ===" );
        OSMImporter importer = new OSMImporter(layerName);
        importer.importFile(graphdb, osmPath);
        importer.reIndex(graphdb, commitInterval);
    }

    @BeforeClass
    public static void setUpCLass() throws Exception {
        graphdb = new ImpermanentGraphDatabase("target/db");
        load();
    }
    
    @AfterClass
    public static void afterCLass() throws Exception {
        graphdb.shutdown();
    }
    
    @Override
    public GraphDatabaseService graphdb() {
        return graphdb;
    }
}
