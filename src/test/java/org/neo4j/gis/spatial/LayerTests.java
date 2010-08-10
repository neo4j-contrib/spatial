package org.neo4j.gis.spatial;

import java.util.List;

import org.junit.Test;
import org.neo4j.gis.spatial.query.SearchContain;
import org.neo4j.gis.spatial.query.SearchIntersect;
import org.neo4j.graphdb.RelationshipType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.Envelope;

public class LayerTests extends Neo4jTestCase {

	@Test
	public void testBasicLayerOperations() {
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		Layer layer = spatialService.getLayer("test");
		assertNull(layer);
		layer = spatialService.createLayer("test");
		assertNotNull(layer);
		assertTrue("Should be a default layer", layer instanceof DefaultLayer);
		spatialService.deleteLayer(layer.getName(), new NullListener());
		assertNull(spatialService.getLayer(layer.getName()));
	}

	@Test
	public void testEditableLayer() {
		SpatialDatabaseService db = new SpatialDatabaseService(graphDb());
		EditableLayer layer = (EditableLayer) db.getOrCreateEditableLayer("test");
		assertNotNull(layer);
		SpatialDatabaseRecord record = layer.add(layer.getGeometryFactory().createPoint(new Coordinate(15.3, 56.2)));
		assertNotNull(record);
		//get back our point
		SearchContain searchQuery = new SearchContain(layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)));
		layer.getIndex().executeSearch(searchQuery);
        List<SpatialDatabaseRecord> results = searchQuery.getResults();
        assertEquals(1, results.size());
	}

	@Test
	public void testDynamicLayer() {
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		testSpecificDynamicLayer(spatialService, (DynamicLayer)spatialService.createLayer("test dynamic layer with property encoder", SimplePropertyEncoder.class, DynamicLayer.class));
		testSpecificDynamicLayer(spatialService, (DynamicLayer)spatialService.createLayer("test dynamic layer with graph encoder", SimpleGraphEncoder.class, DynamicLayer.class));
	}

	private void testSpecificDynamicLayer(SpatialDatabaseService spatialService, DynamicLayer layer){
		assertNotNull(layer);
		assertTrue("Should be a dynamic layer", layer instanceof DynamicLayer);
		layer = (DynamicLayer)spatialService.getLayer(layer.getName());
		assertNotNull(layer);
		assertTrue("Should be a dynamic layer", layer instanceof DynamicLayer);

		CoordinateList coordinates = new CoordinateList();
		coordinates.add(new Coordinate(13.1, 56.2), false);
		coordinates.add(new Coordinate(13.2, 56.1), false);
		coordinates.add(new Coordinate(13.3, 56.0), false);
		coordinates.add(new Coordinate(13.2, 56.1), false);
		coordinates.add(new Coordinate(13.1, 56.2), false);
		coordinates.add(new Coordinate(13.0, 56.3), false);
		layer.add(layer.getGeometryFactory().createLineString(coordinates.toCoordinateArray()));

		coordinates = new CoordinateList();
		coordinates.add(new Coordinate(14.1, 56.2), false);
		coordinates.add(new Coordinate(14.3, 56.0), false);
		coordinates.add(new Coordinate(14.2, 56.1), false);
		coordinates.add(new Coordinate(14.0, 56.3), false);
		layer.add(layer.getGeometryFactory().createLineString(coordinates.toCoordinateArray()));

        doSearch(layer, new SearchIntersect(layer.getGeometryFactory().toGeometry(new Envelope(13.2, 14.1, 56.1, 56.2))));
        doSearch(layer, new SearchContain(layer.getGeometryFactory().toGeometry(new Envelope(12.0, 15.0, 55.0, 57.0))));

//		spatialService.deleteLayer(layer.getName(), new NullListener());
//		assertNull(spatialService.getLayer(layer.getName()));
	}

	private void doSearch(DynamicLayer layer, Search searchQuery) {
	    System.out.println("Testing search intersection:");
        layer.index.executeSearch(searchQuery);
        List<SpatialDatabaseRecord> results = searchQuery.getResults();
        System.out.println("\tTesting layer '" + layer.getName() +"' (class "+layer.getClass() + "), found results: " + results.size());
        for (SpatialDatabaseRecord r : results) {
        	System.out.println("\t\tGeometry: "+r);
        }
    }

	enum TestRelationshipTypes implements RelationshipType {
		FIRST, NEXT;
	}
}
