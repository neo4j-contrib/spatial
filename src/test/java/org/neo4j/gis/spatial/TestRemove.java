package org.neo4j.gis.spatial;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;


public class TestRemove extends Neo4jTestCase {

	public void testAddMoreThanMaxNodeRefThenDeleteAll() throws Exception {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());

        EditableLayer layer = (EditableLayer) spatialService
        	.createLayer("TestRemove", WKTGeometryEncoder.class, EditableLayerImpl.class);
        GeometryFactory geomFactory = layer.getGeometryFactory();
        
        int rtreeMaxNodeReferences = 100;
        
        long[] ids = new long[rtreeMaxNodeReferences + 1];
        for (int i = 0; i < ids.length; i++) {
        	ids[i] = layer.add(geomFactory.createPoint(new Coordinate(i, i))).getId();
        }

        ((RTreeIndex) layer.getIndex()).debugIndexTree();        
        
        for (long id : ids) {
        	layer.delete(id);
        }
        
        ((RTreeIndex) layer.getIndex()).debugIndexTree();  
    }		
}