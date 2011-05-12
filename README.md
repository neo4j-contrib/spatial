Neo4j Spatial
=============
 
Neo4j Spatial is a library facilitating the import, storage and querying of spatial data in the [Neo4j open source graph database](http://neo4j.org/).

![OpenStreetMap](https://github.com/neo4j/neo4j-spatial/raw/master/src/site/pics/one-street.png)


Some key features include:

* Utilities for importing from ESRI Shapefile as well as Open Street Map files
* Support for all the common geometry types
* An RTree index for fast searches on geometries
* Support for topology operations during the search (contains, within, intersects, covers, disjoint, etc.) 
* The possibility to enable spatial operations on any graph of data, regardless of the way the spatial data is stored, as long as an adapter is provided to map from the graph to the geometries.
* Ability to split a single layer or dataset into multiple sub-layers or views with pre-configured filters

Index and Querying
------------------

The current index is an RTree index, but it has been developed in an extensible way allowing for other indices to be added if necessary.
The spatial queries implemented are:

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

The simplest way to build Neo4j Spatial is by using maven. Just clone the git repository and run 
  
  mvn install
  
This will download all dependencies, compiled the library, run the tests and install the artifact in your local repository.
The main dependencies are:

* Neo4j 1.3M02 (at the time of writing, with trunk build of 0.4-SNAPSHOT)
* JTS 1.10
* Geotools 2.7 (not all of geotools, but at least api, main, shapefile, metadata, render)

Layers and GeometryEncoders
---------------------------

The primary type that defines a collection of geometries is the Layer. A layer contains an index for querying. In addition a Layer can be an EditableLayer if it is possible to add and modify geometries in the layer. The next most important interface is the GeometryEncoder.

The DefaultLayer is the standard layer, making use of the WKBGeometryEncoder for storing all geometry types as byte[] properties of one node per geometry instance.

The OSMLayer is a special layer supporting Open Street Map and storing the OSM model as a single fully connected graph. The set of Geometries provided by this layer includes Points, LineStrings and Polygons, and as such cannot be exported to Shapefile format, since that format only allows a single Geometry per layer. However, OMSLayer extends DynamicLayer, which allow it to provide any number of sub-layers, each with a specific geometry type and in addition based on a OSM tag filter. For example you can have a layer providing all cycle paths as LineStrings, or a layer providing all lakes as Polygons. Underneath these are all still backed by the same fully connected graph, but exposed dynamically as apparently separate geometry layers.

Examples
========

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

Importing an Open Street Map file
---------------------------------

This is more complex because the current OSMImporter class runs in two phases, the first requiring a batch-inserter on the database. The is ongoing work to allow for a non-batch-inserter on the entire process, and possibly when you have read this that will already be available. Refer to the unit tests in classes TestDynamicLayers and TestOSMImport for the latest code for importing OSM data. At the time of writing the following worked:

	OSMImporter importer = new OSMImporter("sweden");

	BatchInserter batchInserter = new BatchInserterImpl(dir, config);
	importer.importFile(batchInserter, "sweden.osm", false);
	batchInserter.shutdown();

    GraphDatabaseService db = new EmbeddedGraphDatabase(dir);
	importer.reIndex(db, 10000);
	db.shutdown();


Executing a spatial query
-------------------------

	GraphDatabaseService database = new EmbeddedGraphDatabase(storeDir);
	try {
    	SpatialDatabaseService spatialService = new SpatialDatabaseService(database);
        Layer layer = spatialService.getLayer("layer_roads");
        SpatialIndexReader spatialIndex = layer.getIndex();
        	
        Search searchQuery = new SearchIntersectWindow(new Envelope(xmin, xmax, ymin, ymax));
        spatialIndex.executeSearch(searchQuery);
   	    List<SpatialDatabaseRecord> results = searchQuery.getResults();
	} finally {
		database.shutdown();
	}

Refer to the test code in the LayerTest and the SpatialTest classes for more examples of query code. Also review the classes in the org.neo4j.gis.spatial.query package for the full range or search queries currently implemented.

Using Neo4j Spatial with GeoServer
----------------------------------
For more info head over to [Neo4j Wiki on Geoserver](http://wiki.neo4j.org/content/Neo4j_Spatial_in_GeoServer)


Using Neo4j Spatial with uDig
----------------------------------
For more info head over to [Neo4j Wiki on uDig](http://wiki.neo4j.org/content/Neo4j_Spatial_in_uDig)

Using the Neo4j Spatial Server plugin
-------------------------------------

Neo4j Spatial is also packaged as a ZIP file that can be unzipped into the Neo4j Server /plugin directory. After restarting the server, you should be able to do things liek the following REST calls (here illustrated using `curl`)

    curl http://localhost:7474/db/data/
  
    {
      "relationship_index" : "http://localhost:7474/db/data/index/relationship",
      "node" : "http://localhost:7474/db/data/node",
      "relationship_types" : "http://localhost:7474/db/data/relationship/types",
      "extensions_info" : "http://localhost:7474/db/data/ext",
      "node_index" : "http://localhost:7474/db/data/index/node",
      "reference_node" : "http://localhost:7474/db/data/node/0",
      "extensions" : {
      "SpatialPlugin" : {
          "addSimplePointLayer" : "http://localhost:7474/db/data/ext/SpatialPlugin/graphdb/addSimplePointLayer",
          "addNodeToLayer" : "http://localhost:7474/db/data/ext/SpatialPlugin/graphdb/addNodeToLayer"
        }
      }
    }
  
    curl -d "layer=test" http://localhost:7474/db/data/ext/SpatialPlugin/graphdb/addSimplePointLayer
  
    Creating new layer 'test' unless it already exists
    [ {
      "outgoing_relationships" : "http://localhost:7474/db/data/node/2/relationships/out",
      "data" : {
        "layer_class" : "org.neo4j.gis.spatial.EditableLayerImpl",
        "layer" : "test",
        "geomencoder" : "org.neo4j.gis.spatial.encoders.SimplePointEncoder",
        "ctime" : 1304444390349
      },
      "traverse" : "http://localhost:7474/db/data/node/2/traverse/{returnType}",
      "all_typed_relationships" : "http://localhost:7474/db/data/node/2/relationships/all/{-list|&|types}",
      "property" : "http://localhost:7474/db/data/node/2/properties/{key}",
      "self" : "http://localhost:7474/db/data/node/2",
      "properties" : "http://localhost:7474/db/data/node/2/properties",
      "outgoing_typed_relationships" : "http://localhost:7474/db/data/node/2/relationships/out/{-list|&|types}",
      "incoming_relationships" : "http://localhost:7474/db/data/node/2/relationships/in",
      "extensions" : {
      },
      "create_relationship" : "http://localhost:7474/db/data/node/2/relationships",
      "all_relationships" : "http://localhost:7474/db/data/node/2/relationships/all",
      "incoming_typed_relationships" : "http://localhost:7474/db/data/node/2/relationships/in/{-list|&|types}"
    } ]
    
Building Neo4j spatial
----------------------
  
    git clone https://github.com/neo4j/neo4j-spatial.git
    mvn clean package

Integration-Testing the Neo4j-Spatial Server Plugin

    export DOWNLOAD_PLUGIN_LOCATION=file:/path/to/neo4j-spatial/target/neo4j-spatial-0.6-SNAPSHOT-server-plugin.zip
    export NEO4J_VERSION=1.4.M01
    export NEO4J_PRODUCT=community
    
    rake

or

    mvn test -P integration-tests