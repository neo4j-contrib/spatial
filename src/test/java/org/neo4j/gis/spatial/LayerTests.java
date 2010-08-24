package org.neo4j.gis.spatial;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.neo4j.gis.spatial.geotools.data.Neo4jSpatialDataStore;
import org.neo4j.gis.spatial.osm.OSMGeometryEncoder;
import org.neo4j.gis.spatial.osm.OSMLayer;
import org.neo4j.gis.spatial.query.SearchContain;
import org.neo4j.gis.spatial.query.SearchIntersect;
import org.neo4j.gis.spatial.query.SearchWithin;
import org.neo4j.graphdb.RelationshipType;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.Envelope;

import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.FeatureStore;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

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
        // finds geometries that contain the given geometry
		SearchContain searchQuery = new SearchContain(layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)));
		layer.getIndex().executeSearch(searchQuery);
        List<SpatialDatabaseRecord> results = searchQuery.getResults();
        // should not be contained
        assertEquals(0, results.size());
		SearchWithin withinQuery = new SearchWithin(layer.getGeometryFactory().toGeometry(new Envelope(15.0, 16.0, 56.0, 57.0)));
		layer.getIndex().executeSearch(withinQuery);
        results = withinQuery.getResults();
        assertEquals(1, results.size());
	}

	@Test
	public void testDynamicLayer() {
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		testSpecificDynamicLayer(spatialService, (DynamicLayer)spatialService.createLayer("test dynamic layer with property encoder", SimplePropertyEncoder.class, DynamicLayer.class));
		testSpecificDynamicLayer(spatialService, (DynamicLayer)spatialService.createLayer("test dynamic layer with graph encoder", SimpleGraphEncoder.class, DynamicLayer.class));
		testSpecificDynamicLayer(spatialService, (DynamicLayer)spatialService.createLayer("test dynamic layer with OSM encoder", OSMGeometryEncoder.class, OSMLayer.class));
	}

	private Layer testSpecificDynamicLayer(SpatialDatabaseService spatialService, DynamicLayer layer){
		assertNotNull(layer);
		assertTrue("Should be a dynamic layer", layer instanceof DynamicLayer);
		layer = (DynamicLayer)spatialService.getLayer(layer.getName());
		assertNotNull(layer);
		assertTrue("Should be a dynamic layer", layer instanceof DynamicLayer);

		CoordinateList coordinates = new CoordinateList();
		coordinates.add(new Coordinate(13.1, 56.2), false);
		coordinates.add(new Coordinate(13.2, 56.0), false);
		coordinates.add(new Coordinate(13.3, 56.2), false);
		coordinates.add(new Coordinate(13.2, 56.0), false);
		coordinates.add(new Coordinate(13.1, 56.2), false);
		coordinates.add(new Coordinate(13.0, 56.0), false);
		layer.add(layer.getGeometryFactory().createLineString(coordinates.toCoordinateArray()));

		coordinates = new CoordinateList();
		coordinates.add(new Coordinate(14.1, 56.0), false);
		coordinates.add(new Coordinate(14.3, 56.1), false);
		coordinates.add(new Coordinate(14.2, 56.1), false);
		coordinates.add(new Coordinate(14.0, 56.0), false);
		layer.add(layer.getGeometryFactory().createLineString(coordinates.toCoordinateArray()));

        doSearch(layer, new SearchIntersect(layer.getGeometryFactory().toGeometry(new Envelope(13.2, 14.1, 56.1, 56.2))));
        doSearch(layer, new SearchContain(layer.getGeometryFactory().toGeometry(new Envelope(12.0, 15.0, 55.0, 57.0))));

//		spatialService.deleteLayer(layer.getName(), new NullListener());
//		assertNull(spatialService.getLayer(layer.getName()));
        
        return layer;
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
	
	@Test
	public void testShapefileExport() throws Exception {
		SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
		exportShapefile(testSpecificDynamicLayer(spatialService, (DynamicLayer)spatialService.createLayer("test dynamic layer with property encoder", SimplePropertyEncoder.class, DynamicLayer.class)).getName());
		exportShapefile(testSpecificDynamicLayer(spatialService, (DynamicLayer)spatialService.createLayer("test dynamic layer with graph encoder", SimpleGraphEncoder.class, DynamicLayer.class)).getName());
		exportShapefile(testSpecificDynamicLayer(spatialService, (DynamicLayer)spatialService.createLayer("test dynamic layer with OSM encoder", OSMGeometryEncoder.class, OSMLayer.class)).getName());
	}
	
	private void exportShapefile(String layerName) throws Exception {
		ShapefileDataStoreFactory factory = new ShapefileDataStoreFactory();
		Map<String, Serializable> create = new HashMap<String, Serializable>();
		String fileName = layerName.replace(" ", "-");
        File file = new File("target/export/"+fileName+".shp");
        file.getParentFile().mkdirs();
        if(file.exists()) {
        	System.out.println("Deleting previous file: "+file);
        	file.delete();
        }
        URL url = file.toURI().toURL();
		create.put("url", url);
        create.put("create spatial index", Boolean.TRUE);
        ShapefileDataStore shpDataStore = (ShapefileDataStore) factory.createNewDataStore(create);
        Neo4jSpatialDataStore neo4jDataStore = new Neo4jSpatialDataStore(graphDb());
        SimpleFeatureType featureType = neo4jDataStore.getSchema(layerName);
        GeometryDescriptor geometryType = featureType.getGeometryDescriptor();
        CoordinateReferenceSystem crs = geometryType.getCoordinateReferenceSystem();
        //crs = neo4jDataStore.getFeatureSource(layerName).getInfo().getCRS();
        
        shpDataStore.createSchema(featureType);
        FeatureStore store = (FeatureStore) shpDataStore.getFeatureSource();
        store.addFeatures( neo4jDataStore.getFeatureSource(layerName).getFeatures() );
        if(crs!=null) shpDataStore.forceSchemaCRS( crs );

        assertTrue("Shapefile was not created: "+file,file.exists());
        assertTrue("Shapefile was unexpectedly small, only "+file.length()+" bytes: "+file,file.length()>100);
	}

	enum TestRelationshipTypes implements RelationshipType {
		FIRST, NEXT;
	}
}
