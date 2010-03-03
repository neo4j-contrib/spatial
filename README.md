Neo4j Spatial
=============
 
This is a first prototype of a library to store spatial data in the [Neo4j open source graph database](http://neo4j.org/).
You can import geometries from a shapefile and perform spatial searches.

The most common 2D geometries are supported:

* (multi)point
* (multi)linestring
* (multi)polygon

Spatial queries implemented:

* Contain
* Cover
* Covered By
* Cross
* Disjoint
* Intersect
* Intersect Window
* Overlap
* Touch
* Within
* Within Distance
 
 
Building
--------
 
You need a Java 6 environment, [Neo4J](http://neo4j.org/), [JTS](http://tsusiatsoftware.net/jts/main.html) and [GeoTools](http://www.geotools.org/):

* jta-1.1.jar
* neo4j-kernel-1.0-rc.jar
* neo4j-commons-0.4.jar
* jts-1.10.jar
* geoapi-2.3-M1.jar
* gt-api-2.6.1.jar
* gt-shapefile-2.6.1.jar
* gt-main-2.6.1.jar
* gt-metadata-2.6.1.jar

 
Importing a shapefile
---------------------

Spatial data is divided in Layers and indexed by a RTree.

    GraphDatabaseService database = new EmbeddedGraphDatabase(storeDir);
	try {
		ShapefileImporter importer = new ShapefileImporter(database);
	    importer.importShapefile("roads.shp", "layer_roads");
	} finally {
		database.shutdown();
	}


Executing a spatial query
-------------------------

	GraphDatabaseService database = new EmbeddedGraphDatabase(storeDir);
	try {
		Transaction tx = database.beginTx();
	    try {
	    	SpatialDatabaseService spatialService = new SpatialDatabaseService(database);
	        Layer layer = spatialService.getLayer("layer_roads");
	        SpatialIndexReader spatialIndex = layer.getIndex();
	        	
	        Search searchQuery = new SearchIntersectWindow(new Envelope(xmin, xmax, ymin, ymax));
	        spatialIndex.executeSearch(searchQuery);
    	    List<SpatialDatabaseRecord> results = searchQuery.getResults();
    	       	
			tx.success();
		} finally {
	    	tx.finish();
	    }	        	        	
	} finally {
		database.shutdown();
	}
