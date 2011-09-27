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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.geotools.filter.text.cql2.CQLException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.collections.rtree.filter.SearchAll;
import org.neo4j.collections.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.EditableLayerImpl;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.pipes.osm.OSMGeoPipeline;
import org.neo4j.test.ImpermanentGraphDatabase;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.util.AffineTransformation;
import com.vividsolutions.jts.io.WKTReader;


public class GeoPipesTest {
	
    private static ImpermanentGraphDatabase graphdb;
    private static Layer osmLayer;
    private static EditableLayerImpl boxesLayer;
    private static EditableLayerImpl concaveLayer;
    
    private OSMGeoPipeline startPipeline(Layer layer) {
    	return startPipeline(layer, new SearchAll());
    }
    
    private OSMGeoPipeline startPipeline(Layer layer, SearchFilter filter) {
    	return OSMGeoPipeline.start(layer, layer.getIndex().search(filter));
    }    
    
    @Test
    public void find_all() {
    	int count = 0;
    	for (GeoPipeFlow flow : startPipeline(osmLayer).createWellKnownText()) {
    		count++;
    		
    		assertEquals(1, flow.getProperties().size());
    		String wkt = (String) flow.getProperties().get("WellKnownText");
    		assertTrue(wkt.indexOf("LINESTRING") == 0);
    	}
    	
        assertEquals(2, count);
    }    
        
    @Test
    public void filter_by_osm_attribute() {
    	GeoPipeline pipeline = startPipeline(osmLayer).osmAttributeFilter("name", "Storgatan")
    		.copyDatabaseRecordProperties();
    	
    	GeoPipeFlow flow = pipeline.next();
    	assertFalse(pipeline.hasNext());

    	assertEquals("Storgatan", flow.getProperties().get("name"));
    }

    @Test
    public void filter_by_property() {
    	GeoPipeline pipeline = startPipeline(osmLayer)
    		.copyDatabaseRecordProperties()
    		.propertyFilter("name", "Storgatan");
	
		GeoPipeFlow flow = pipeline.next();
		assertFalse(pipeline.hasNext());
	
		assertEquals("Storgatan", flow.getProperties().get("name"));    	
    }
    
    @Test
    public void filter_by_window_intersection() {
    	assertEquals(1, startPipeline(osmLayer).windowIntersectionFilter(10, 40, 20, 56.0583531).count());
    }
    
    @Test
    public void filter_by_cql_using_bbox() throws CQLException {
    	assertEquals(1, startPipeline(osmLayer).cqlFilter("BBOX(the_geom, 10, 40, 20, 56.0583531)").count());
    }
    
    @Test    
    public void filter_by_cql_using_property() throws CQLException {
    	GeoPipeline pipeline = startPipeline(osmLayer).cqlFilter("name = 'Storgatan'")
			.copyDatabaseRecordProperties();
	
		GeoPipeFlow flow = pipeline.next();
		assertFalse(pipeline.hasNext());
	
		assertEquals("Storgatan", flow.getProperties().get("name"));    	
    }
    
    @Test
    public void traslate_geometries() {
    	GeoPipeline original = startPipeline(osmLayer)
    		.copyDatabaseRecordProperties()
    		.sort("name");
    	
    	GeoPipeline translated = startPipeline(osmLayer)
    		.applyAffineTransform(AffineTransformation.translationInstance(10, 25))
    		.copyDatabaseRecordProperties()
    		.sort("name");

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
    	GeoPipeline pipeline = startPipeline(boxesLayer)
    		.calculateArea()
    		.sort("Area");
    	
    	assertEquals((Double) pipeline.next().getProperties().get("Area"), 1.0, 0);
    	assertEquals((Double) pipeline.next().getProperties().get("Area"), 8.0, 0);    	
    }
    
    @Test
    public void calculate_length() {
    	GeoPipeline pipeline = startPipeline(boxesLayer)
    		.calculateLength()
    		.sort("Length");
    	
    	assertEquals((Double) pipeline.next().getProperties().get("Length"), 4.0, 0);
    	assertEquals((Double) pipeline.next().getProperties().get("Length"), 12.0, 0);    	    	
    }
    
    @Test
    public void get_boundary_length() {
    	GeoPipeline pipeline = startPipeline(boxesLayer)
    		.toBoundary()
    		.createWellKnownText()
    		.calculateLength()
    		.sort("Length");

    	GeoPipeFlow first = pipeline.next();
    	GeoPipeFlow second = pipeline.next();
    	assertEquals("LINEARRING (12 56, 12 57, 13 57, 13 56, 12 56)", first.getProperties().get("WellKnownText"));
    	assertEquals("LINEARRING (2 3, 2 5, 6 5, 6 3, 2 3)", second.getProperties().get("WellKnownText"));    	
    	assertEquals((Double) first.getProperties().get("Length"), 4.0, 0);
    	assertEquals((Double) second.getProperties().get("Length"), 12.0, 0);    	    	
    }    
    
    @Test
    public void get_buffer() {
    	GeoPipeline pipeline = startPipeline(boxesLayer)
    		.toBuffer(0.1)
    		.createWellKnownText()
    		.calculateArea()
    		.sort("Area");
    	
    	assertTrue(((Double) pipeline.next().getProperties().get("Area")) > 1);
    	assertTrue(((Double) pipeline.next().getProperties().get("Area")) > 8);    	
    }
    
    @Test
    public void get_centroid() {
    	GeoPipeline pipeline = startPipeline(boxesLayer)
    		.toCentroid()
    		.createWellKnownText()
    		.copyDatabaseRecordProperties()
    		.sort("name");
    	
    	assertEquals("POINT (12.5 56.5)", pipeline.next().getProperties().get("WellKnownText"));
    	assertEquals("POINT (4 4)", pipeline.next().getProperties().get("WellKnownText"));    	
    }
    
    @Test
    public void get_convex_hull() {
    	GeoPipeline pipeline = startPipeline(concaveLayer)
    		.toConvexHull()
    		.createWellKnownText();
    	
    	assertEquals("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", pipeline.next().getProperties().get("WellKnownText"));
    	
    	print(pipeline);
    }
    
    @Test
    public void densify() {
    	GeoPipeline pipeline = startPipeline(concaveLayer)
    		.toConvexHull()
    		.densify(10)
    		.createWellKnownText();
    	
    	assertEquals("POLYGON ((0 0, 0 5, 0 10, 5 10, 10 10, 10 5, 10 0, 5 0, 0 0))", 
    			pipeline.next().getProperties().get("WellKnownText"));
    }
    
    @Test
    public void json() {
    	GeoPipeline pipeline = startPipeline(boxesLayer)
			.createJson()
			.copyDatabaseRecordProperties()
			.sort("name");

    	assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[12,56],[12,57],[13,57],[13,56],[12,56]]]}", 
    			pipeline.next().getProperties().get("GeoJSON"));
    	assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[2,3],[2,5],[6,5],[6,3],[2,3]]]}", 
    			pipeline.next().getProperties().get("GeoJSON"));
    }
    
    @Test
    public void get_max_area() {
    	GeoPipeline pipeline = startPipeline(boxesLayer)
    		.calculateArea()
    		.getMax("Area");
    	
    	assertEquals((Double) pipeline.next().getProperties().get("Area"), 8.0, 0);    	
    }   
    
    @Test
    public void get_min_area() {
    	GeoPipeline pipeline = startPipeline(boxesLayer)
    		.calculateArea()
    		.getMin("Area");
    	
    	assertEquals((Double) pipeline.next().getProperties().get("Area"), 1.0, 0);    	
    }   
        
    @Test
    public void extract_osm_points() {
    	int count = 0;
    	for (GeoPipeFlow flow : startPipeline(osmLayer).extractOsmPoints().createWellKnownText()) {
    		count++;
    		
    		assertEquals(1, flow.getProperties().size());
    		String wkt = (String) flow.getProperties().get("WellKnownText");
    		assertTrue(wkt.indexOf("POINT") == 0);
    	}
    	
        assertEquals(24, count);
    }
        
    @Test
    public void break_up_all_geometries_into_points_and_make_density_islands_and_get_the_outer_linear_ring_of_the_density_islands_and_buffer_the_geometry_and_count_them() {
    	assertEquals(1, startPipeline(osmLayer)
    			.extractOsmPoints().groupByDensityIslands(0.1).toConvexHull().toBuffer(10).count());
    	System.out.println(startPipeline(osmLayer)
    			.extractOsmPoints().groupByDensityIslands(0.1).toConvexHull().toBuffer(10).count());
    }
    
    private static void load() throws Exception {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphdb);
        
        loadTestOsmData("two-street.osm", 100);
        osmLayer = spatialService.getLayer("two-street.osm");
        
        boxesLayer = (EditableLayerImpl) spatialService.getOrCreateEditableLayer("boxes");
        boxesLayer.setExtraPropertyNames(new String[] { "name" });
        WKTReader reader = new WKTReader(boxesLayer.getGeometryFactory());
        boxesLayer.add(reader.read("POLYGON ((12 56, 12 57, 13 57, 13 56, 12 56))"),
        		new String[] { "name" }, new Object[] { "A" });
        boxesLayer.add(reader.read("POLYGON ((2 3, 2 5, 6 5, 6 3, 2 3))"),
        		new String[] { "name" }, new Object[] { "B" });
        
        concaveLayer = (EditableLayerImpl) spatialService.getOrCreateEditableLayer("concave");
        reader = new WKTReader(concaveLayer.getGeometryFactory());
        concaveLayer.add(reader.read("POLYGON ((0 0, 2 5, 0 10, 10 10, 10 0, 0 0))"));
    }

    private static void loadTestOsmData(String layerName, int commitInterval) throws Exception {
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
       
    private void print(GeoPipeline pipeline) {
    	while (pipeline.hasNext()) {
    		print(pipeline.next());
    	}
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