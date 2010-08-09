package org.neo4j.gis.spatial;

import org.junit.Test;


public class LayerTests extends Neo4jTestCase {

	@Test
	public void testGetLayer() {
		
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        Layer layer = spatialService.getLayer("test");
        assertNull(layer);
        layer = spatialService.createLayer("test");
        assertNotNull(layer);
        System.out.println("hej");
	}
}
