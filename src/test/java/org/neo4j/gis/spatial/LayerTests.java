package org.neo4j.gis.spatial;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;


public class LayerTests extends Neo4jTestCase {

	
	@Test
	public void testBasicLayerOperations() {
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        Layer layer = spatialService.getLayer("test");
        assertNull(layer);
        layer = spatialService.createLayer("test");
        assertNotNull(layer);
        assertTrue("Should be a default layer", layer instanceof DefaultLayer);
        spatialService.deleteLayer(layer.getName(), new Listener() {
			
			@Override
			public void worked(int workedSinceLastNotification) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void done() {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void begin(int unitsOfWork) {
				// TODO Auto-generated method stub
				
			}
		});
        assertNull(spatialService.getLayer(layer.getName()));
	}
	@Test
	public void testEditableLayer() {
		SpatialDatabaseService db = new SpatialDatabaseService(graphDb());
		EditableLayer layer = (EditableLayer) db.getOrCreateEditableLayer("test");
		assertNotNull(layer);
        layer.add(layer.getGeometryFactory().createPoint(new Coordinate(15.3, 56.2)));

	}
}
