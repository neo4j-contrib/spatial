# Neo4j Spatial

[![Build Status](https://travis-ci.org/neo4j-contrib/spatial.png)](https://travis-ci.org/neo4j-contrib/spatial)

Neo4j Spatial is a library facilitating the import, storage and querying of spatial data in the [Neo4j open source graph database](http://neo4j.org/).

This projects manual is deployed as part of the local build as the [Neo4j Spatial Manual](http://neo4j-contrib.github.io/spatial).

![Open Street Map](http://neo4j-contrib.github.io/spatial/0.24-neo4j-3.1/images/one-street.png "Open Street Map")

## History

This library began as a collaborative vision between Neo-Technology and [Craig Taverner](https://github.com/craigtaverner) in early 2010.
The bulk of the initial work was done by [Davide Savazzi](https://github.com/svzdvd) as part of his 2010 Google Summer of Code (GSoC) project
with Craig as mentor, as a project within the OSGeo GSoC program.
In 2011 and 2012 two further GSoC projects contributed, the last of which saw Davide return as mentor.

The original vision for the library was a comprehensive suite of GIS capabilities somewhat inspired by PostGIS,
while remaining aligned with the graph-nature of the underlying Neo4j database.
To achieve this lofty goal the JTS and GeoTools libraries were used, giving a large suite of capabilities very early on.

However, back in 2010 Neo4j was an embedded database with deployments that saw a low level of concurrent operation.
However, for many years now, Neo4j has been primarily deployed in high concurrent server environments.
Over the years there have been various efforts to make the library more appropriate for use in these environments:

* REST API (early Neo4j 1.x servers had no Cypher)
* IndexProvider mechanism (used during Neo4j 1.x and 2.x server, and removed for Neo4j 3.0)
* 0.23: Addition of Cypher procedures for Neo4j 3.0
* 0.24: Addition of a high performance bulk importer to the in-graph RTree index
* 0.25: Addition of GeoHash indexes for point layers 
* 0.26: Support for native Neo4j point types
* 0.27: Major port to Neo4j 4.x which deprecated many of the Neo4j API's the library depended on

However, despite all these improvements, the core of the library only exposes the rich capabilities of JTS and GeoTools
if used in an embedded environment.
The large effort required to port the library to Neo4j 4.0 resulted in many backwards incompatibilities and
highlighted the need for a new approach to spatial libraries for Neo4j.

## Neo4j 4.x Support

Since this library was originally written in 2010 when Neo4j was still releasing early 1.x versions,
it made use of internal Java API's that were deprecated over the years, some as early as Neo4j 2.x.
When Neo4j 4.0 was released, it entirely removed a number of deprecated API's, and completely changed
the Transaction API.
This meant that the spatial library needed a major refactoring to work with Neo4j 4.x:

* The library previously depended on outer and inner transactions, so code that was called
  within a transaction (like procedures) could be common with code that was not (Java API).
  The removal of this support required that all internal API's needed to include parameters
  for the current transaction, and only the specific surface designed for embedded use not.
* The library made use of Lucene based explicit indexes in many places.
  The removal of support for explicit indexes required new solutions in sevaral places:
  * The `OSMImporter` used instead normal Neo4j schema indexes (introduced in 2.0), but these
    can only be created in separate index transactions. Due to the new transaction model this
    required stopping the import transaction, starting an index transaction, and then restarting
    the import transaction. All of this is incompatible with procedures, which already have
    an incoming, non-stoppable transaction. The solution was to run the actual import in another
    thread, retaining the original batch-processing capabilities, but requiring modifying the 
    security model of the procedure context.
  * The `ExplicitIndexBackedPointIndex` needed to be modified to instead use a schema index.
    This required similar tricks as employed in the `OSMImporter`.
* Neo4j 4.0 runs only in Java 11, and GeoTools only supported up to Java 1.8 until recently.
  This required an upgrade to the GeoTools and JTS libraries to version 24.2.
  This in turn required a re-write of the Neo4jDataStore interface since the older API had
  long been deprecated, and was entirely unavailable in newer versions.

Consequences of this port:

* The large number of changes mean that the 0.27.0 version should be considered very alpha.
* Many API's have changed and client code might need to be adapted to take the changes into account.
* The new GeoServer API is entirely untested, besides the existing unit and integration tests.
* The need to manage threads and create schema indexes results in the procedures requiring
  unrestricted access to internal API's of Neo4j.
  
This part point means that you need to set the following in your `neo4j.conf` file:

```
dbms.security.procedures.unrestricted=spatial.*
```

The rest of the README might have information that is no longer accurate for the current version of this library.
Please report any mistakes as issues, or consider raising a pull-request with an appropriate fix.

## Concept Overview

The key concepts of this library include:

* Allow the user to model geograph data in whatever way they wish, through providing an adapter (extend `GeometryEncoder`).
  Built-in encoders include:
  * WKT and WKB stored as properties of nodes
  * Simple points as properties of nodes (two doubles, or a double[] or a native Neo4j `Point`)
  * OpenStreetMap with complex geometries stored as sub-graphs to reflect the original topology of the OSM model
* Multile CoordinationReferenceSystem support using GeoTools
* Support the concept of multiple geographic layers, each with its own CRS and Index
* Include an index capable of searching for complex geometries (in-graph RTree index)
* Support import and export in a number of known formats (eg. Shapefile and OSM)
* Embed the library and Neo4j within GIS tools like uDig and GeoServer

Some key features include:

* Utilities for importing from ESRI Shapefile as well as Open Street Map files
* Support for all the common geometry types
* An RTree index for fast searches on geometries
* Support for topology operations during the search (contains, within, intersects, covers, disjoint, etc.) 
* The possibility to enable spatial operations on any graph of data, regardless of the way the spatial data is stored, as long as an adapter is provided to map from the graph to the geometries.
* Ability to split a single layer or dataset into multiple sub-layers or views with pre-configured filters
* Server Plugin for Neo4j Server 2.x and 3.x
  * REST API for creating layers and adding nodes or geometries to layers
  * IndexProvider API (2.x only) for Cypher access using `START node=node:geom({query})`
  * Procedures (3.x only) for much more comprehensive access to spatial from Cypher

## Index and Querying ##


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
 
## Building ##

The simplest way to build Neo4j Spatial is by using maven. Just clone the git repository and run 

~~~bash
    mvn install
~~~

This will download all dependencies, compiled the library, run the tests and install the artifact in your local repository.
The spatial plugin will also be created in the `target` directory, and can be copied to your local server using instructions on the spatial server plugin below.

## Layers and GeometryEncoders ##

The primary type that defines a collection of geometries is the Layer. A layer contains an index for querying. In addition a Layer can be an EditableLayer if it is possible to add and modify geometries in the layer. The next most important interface is the GeometryEncoder.

The DefaultLayer is the standard layer, making use of the WKBGeometryEncoder for storing all geometry types as byte[] properties of one node per geometry instance.

The OSMLayer is a special layer supporting Open Street Map and storing the OSM model as a single fully connected graph. The set of Geometries provided by this layer includes Points, LineStrings and Polygons, and as such cannot be exported to Shapefile format, since that format only allows a single Geometry per layer. However, OMSLayer extends DynamicLayer, which allow it to provide any number of sub-layers, each with a specific geometry type and in addition based on a OSM tag filter. For example you can have a layer providing all cycle paths as LineStrings, or a layer providing all lakes as Polygons. Underneath these are all still backed by the same fully connected graph, but exposed dynamically as apparently separate geometry layers.

## Examples ##

### Importing a shapefile ###

Spatial data is divided in Layers and indexed by a RTree.

~~~java
    GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabase(storeDir);
    try {
        ShapefileImporter importer = new ShapefileImporter(database);
        importer.importFile("roads.shp", "layer_roads");
    } finally {
        database.shutdown();
    }
~~~

If using the server, the same can be achieved with spatial procedures (3.x only):

~~~cypher
CALL spatial.addWKTLayer('layer_roads','geometry')
CALL spatial.importShapefileToLayer('layer_roads','roads.shp')
~~~

### Importing an Open Street Map file ###

This is more complex because the current OSMImporter class runs in two phases, the first requiring a batch-inserter on the database. There is ongoing work to allow for a non-batch-inserter on the entire process, and possibly when you have read this that will already be available. Refer to the unit tests in classes TestDynamicLayers and TestOSMImport for the latest code for importing OSM data. At the time of writing the following worked:

~~~java
    OSMImporter importer = new OSMImporter("sweden");
    Map<String, String> config = new HashMap<String, String>();
    config.put("neostore.nodestore.db.mapped_memory", "90M" );
    config.put("dump_configuration", "true");
    config.put("use_memory_mapped_buffers", "true");
    BatchInserter batchInserter = BatchInserters.inserter(new File(dir), config);
    importer.importFile(batchInserter, "sweden.osm", false);
    batchInserter.shutdown();

    GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dir);
    importer.reIndex(db, 10000);
    db.shutdown();
~~~

### Executing a spatial query ###

~~~java
    GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabase(storeDir);
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
~~~

If using the server, the same can be achieved with spatial procedures (3.x only):

~~~cypher
CALL spatial.bbox('layer_roads', {lon:15.0,lat:60.0}, {lon:15.3, lat:61.0})
~~~

Or using a polygon:

~~~cypher
WITH "POLYGON((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2))" as polygon
CALL spatial.intersects('layer_roads',polygon) YIELD node RETURN node.name as name
~~~

For further Java examples, refer to the test code in the [LayersTest](https://github.com/neo4j-contrib/spatial/blob/master/src/test/java/org/neo4j/gis/spatial/LayersTest.java) and the [TestSpatial](https://github.com/neo4j-contrib/spatial/blob/master/src/test/java/org/neo4j/gis/spatial/TestSpatial.java) classes.

For further Procedures examples, refer to the code in the [SpatialProceduresTest](https://github.com/neo4j-contrib/spatial/blob/master/src/test/java/org/neo4j/gis/spatial/procedures/SpatialProceduresTest.java) class.

## Neo4j Spatial Geoserver Plugin ##

*IMPORTANT*: Examples in this readme were originally tested with GeoServer 2.1.1.
However, regular testing of new releases of Neo4j Spatial against GeoServer is not done,
and so we welcome feedback on which versions are known to work, and which ones do not,
and perhaps some hints as to the errors or problems encountered.

Each release of Neo4j Spatial builds against a specific version of GeoTools and should then be used in the version of GeoServer that corresponds to that.
The list of releases below starting at Neo4j 2.0.8 were built with GeoTools 9.0 for GeoServer 2.3.2,
but most release for Neo4j 3.x were ported to GeoTools 14.4 for GeoServer 2.8.4.

For the port to Neo4j 4.0 we needed to upgrade GeoTools to 24.x to avoid bugs with older GeoTools in Java 11.
This also required a complete re-write of the Neo4jDataStore and related classes.
This has not been tested at all in any GeoTools enabled application, but could perhaps work with GeoServer 2.18.

### Building ###

~~~bash
    mvn clean install
~~~

### Deployment into Geoserver ###

* unzip the `target/xxxx-server-plugin.zip` and the Neo4j libraries from your Neo4j download under `$NEO4J_HOME/lib` into `$GEOSERVER_HOME/webapps/geoserver/WEB-INF/lib`
* restart geoserver
* configure a new workspace
* configure a new datasource neo4j in your workspace. Point the "The directory path of the Neo4j database:" parameter to the relative (form the GeoServer working dir) or aboslute path to a Neo4j Spatial database with layers (see [Neo4j Spatial](https://github.com/neo4j/spatial))
* in Layers, do "Add new resource" and choose your Neo4j datastore to see the exisitng Neo4j Spatial layers and add them.

### Testing in GeoServer trunk ###

* check out the geoserver source

~~~bash
    svn co https://svn.codehaus.org/geoserver/trunk geoserver-trunk
~~~

* build the source

~~~bash
    cd geoserver-trunk
    mvn clean install
~~~

* check that you can run the web app as of [The GeoServer Maven build guide](http://docs.geoserver.org/latest/en/developer/maven-guide/index.html#running-the-web-module-with-jetty)

~~~bash
    cd src/web/app
    mvn jetty:run
~~~

* in `$GEOSERVER_SOURCE/src/web/app/pom.xml` https://svn.codehaus.org/geoserver/trunk/src/web/app/pom.xml, add the following lines under the profiles section:

~~~xml
    <profile>
      <id>neo4j</id>
      <dependencies>
        <dependency>
          <groupId>org.neo4j</groupId>
          <artifactId>neo4j-spatial</artifactId>
          <version>0.19-neo4j-3.0.3</version>
        </dependency>
      </dependencies>
    </profile>
~~~

The version specified on the version line can be changed to match the version you wish to work with (based on the version of Neo4j itself you are using). Too see which versions are available see the list at [Neo4j Spatial Releases](https://github.com/neo4j-contrib/m2/tree/master/releases/org/neo4j/neo4j-spatial).

* start the GeoServer webapp again with the added neo4j profile

~~~bash
    cd $GEOSERVER_SRC/src/web/app
    mvn jetty:run -Pneo4j
~~~

* find Neo4j installed as a datasource under http://localhost:8080


## Using Neo4j Spatial with uDig ##

For more info head over to [Neo4j Wiki on uDig](http://wiki.neo4j.org/content/Neo4j_Spatial_in_uDig) (This wiki is currently dead, but there appears to be a working mirror in Japan at http://oss.infoscience.co.jp/neo4j/wiki.neo4j.org/content/Neo4j_Spatial_in_uDig.html).

## Using the Neo4j Spatial Server plugin ##

The Neo4j Spatial Plugin is available for inclusion in the server version of Neo4j 2.x, Neo4j 3.x and Neo4j 4.x.

* Using GeoTools 9.0 (for GeoServer 2.3.2):
  * [v0.12 for Neo4j 2.0.4](https://github.com/neo4j-contrib/m2/blob/master/releases/org/neo4j/neo4j-spatial/0.12-neo4j-2.0.4/neo4j-spatial-0.12-neo4j-2.0.4-server-plugin.zip?raw=true)
  * [v0.13 for Neo4j 2.1.8](https://github.com/neo4j-contrib/m2/blob/master/releases/org/neo4j/neo4j-spatial/0.13-neo4j-2.1.8/neo4j-spatial-0.13-neo4j-2.1.8-server-plugin.zip?raw=true)
  * [v0.14 for Neo4j 2.2.7](https://github.com/neo4j-contrib/m2/blob/master/releases/org/neo4j/neo4j-spatial/0.14-neo4j-2.2.7/neo4j-spatial-0.14-neo4j-2.2.7-server-plugin.zip?raw=true)
  * [v0.15.2 for Neo4j 2.3.4](https://github.com/neo4j-contrib/m2/blob/master/releases/org/neo4j/neo4j-spatial/0.15.2-neo4j-2.3.4/neo4j-spatial-0.15.2-neo4j-2.3.4-server-plugin.zip?raw=true)
  * [v0.19 for Neo4j 3.0.3](https://github.com/neo4j-contrib/m2/blob/master/releases/org/neo4j/neo4j-spatial/0.19-neo4j-3.0.3/neo4j-spatial-0.19-neo4j-3.0.3-server-plugin.jar?raw=true)
* Using GeoTools 14.4 (for GeoServer 2.8.4):
  * [v0.25.1 for Neo4j 3.0.8](https://github.com/neo4j-contrib/m2/blob/master/releases/org/neo4j/neo4j-spatial/0.25.1-neo4j-3.0.8/neo4j-spatial-0.25.1-neo4j-3.0.8-server-plugin.jar?raw=true)
  * [v0.25.3 for Neo4j 3.1.4](https://github.com/neo4j-contrib/m2/blob/master/releases/org/neo4j/neo4j-spatial/0.25.3-neo4j-3.1.4/neo4j-spatial-0.25.3-neo4j-3.1.4-server-plugin.jar?raw=true)
  * [v0.25.4 for Neo4j 3.2.8](https://github.com/neo4j-contrib/m2/blob/master/releases/org/neo4j/neo4j-spatial/0.25.4-neo4j-3.2.8/neo4j-spatial-0.25.4-neo4j-3.2.8-server-plugin.jar?raw=true)
  * [v0.25.5 for Neo4j 3.3.5](https://github.com/neo4j-contrib/m2/blob/master/releases/org/neo4j/neo4j-spatial/0.25.5-neo4j-3.3.5/neo4j-spatial-0.25.5-neo4j-3.3.5-server-plugin.jar?raw=true)
  * [v0.26.2 for Neo4j 3.4.12](https://github.com/neo4j-contrib/m2/blob/master/releases/org/neo4j/neo4j-spatial/0.26.2-neo4j-3.4.12/neo4j-spatial-0.26.2-neo4j-3.4.12-server-plugin.jar?raw=true)
  * [v0.26.2 for Neo4j 3.5.2](https://github.com/neo4j-contrib/m2/blob/master/releases/org/neo4j/neo4j-spatial/0.26.2-neo4j-3.5.2/neo4j-spatial-0.26.2-neo4j-3.5.2-server-plugin.jar?raw=true)
* Using GeoTools 24.2 (for GeoServer 2.18.x):
  * [v0.27.0 for Neo4j 4.0.3](https://github.com/neo4j-contrib/m2/blob/master/releases/org/neo4j/neo4j-spatial/0.27.0-neo4j-4.0.3/neo4j-spatial-0.27.0-neo4j-4.0.3-server-plugin.jar?raw=true)

For versions up to 0.15-neo4j-2.3.4:

~~~bash
    #install the plugin
    unzip neo4j-spatial-XXXX-server-plugin.zip -d $NEO4J_HOME/plugins
    
    #start the server
    $NEO4J_HOME/bin/neo4j start

    #list REST API (edit to correct password)
    curl -u neo4j:neo4j http://localhost:7474/db/data/
~~~

For versions for neo4j 3.0 and later:

~~~bash
    #install the plugin
    cp neo4j-spatial-XXXX-server-plugin.jar $NEO4J_HOME/plugins/
    
    #start the server
    $NEO4J_HOME/bin/neo4j start

    #list REST API (edit to correct password)
    curl -u neo4j:neo4j http://localhost:7474/db/data/

    #list spatial procedures (edit to correct password)
    curl -u neo4j:neo4j -H "Content-Type: application/json" -X POST -d '{"query":"CALL spatial.procedures"}' http://localhost:7474/db/data/cypher
~~~

The server plugin provides access to the internal spatial capabilities using three APIs:
* A REST API for creating layers and adding nodes or geometries to layers.
  * For usage information see [Neo4j Spatial Manual REST](http://neo4j-contrib.github.io/spatial/#spatial-server-plugin)
  * Note that this API provides only limited access to Spatial, with no access the the GeoPipes or import utilities
  * This API was entirely removed when support for Neo4j 4.0 was added (version 0.27)
* An IndexProvider API (2.x only) for Cypher access using START node=node:geom({query})
  * It is only possible to add nodes and query for nodes, and the resulting graph structure is not compatible with any other spatial API (not compatible with Java API, REST or Procedures), so if you use this approach, do not blend it with the other approaches.
  * There is some brief documentation at [Finding geometries within distance using cypher](http://neo4j-contrib.github.io/spatial/#rest-api-find-geometries-within--distance-using-cypher)
  * This API was removed for 3.0 releases, and so is only available for Neo4j 2.x
* Procedures for much more comprehensive access to spatial from Cypher
  * Documentation is not yet available, but you can list the available procedures within Neo4j using the query `CALL spatial.procedures`
  * This API uses the _Procedures_ capabilities released in Neo4j 3.0, and is therefor not available for Neo4j 2.x

During the Neo4j 3.x releases, support for spatial procedures changed a little,
through the addition of various new capabilities.
They were very quickly much more capable than the older REST API,
making them by far the best option for accessing Neo4j remotely or through Cypher.

The Java API (the original API for Neo4j Spatial) still remains, however, the most feature rich,
and therefor we recommend that if you need to access Neo4j server remotely, and want deeper access to Spatial functions,
consider writing your own Procedures. The Neo4j 3.0 documentation provides some good information on how to do this,
and you can also refer to the [Neo4j Spatial procedures source code](https://github.com/neo4j-contrib/spatial/blob/master/src/main/java/org/neo4j/gis/spatial/procedures/SpatialProcedures.java) for examples.

## Building Neo4j spatial ##

~~~bash  
    git clone https://github.com/neo4j/spatial.git
    cd spatial
    mvn clean package
~~~

### Building Neo4j Spatial Documentation ###

Add your Github credentials in your `~/.m2/settings.xml`

~~~xml
<settings>
    <servers>
      <server>
        <id>github</id>
        <username>xxx@xxx.xx</username>
        <password>secret</password>
      </server>
    </servers>
</settings>
~~~

To build and deploy:

~~~bash
    git clone https://github.com/neo4j/spatial.git
    cd spatial
    mvn clean install site -Pneo-docs-build  
~~~

## Using Neo4j spatial in your Java project with Maven ##

Add the following repositories and dependency to your project's pom.xml:

~~~xml
    <repositories>
        <repository>
            <id>neo4j-contrib-releases</id>
            <url>https://raw.github.com/neo4j-contrib/m2/master/releases</url>
            <releases>
                <enabled>true</enabled>
            </releases>
            <snapshots>
                <enabled>false</enabled>
            </snapshots>
        </repository>
        <repository>
            <id>neo4j-contrib-snapshots</id>
            <url>https://raw.github.com/neo4j-contrib/m2/master/snapshots</url>
            <releases>
                <enabled>false</enabled>
            </releases>
            <snapshots>
                <enabled>true</enabled>
            </snapshots>
        </repository>
    </repositories>
    [...]
    <dependency>
        <groupId>org.neo4j</groupId>
        <artifactId>neo4j-spatial</artifactId>
        <version>0.27.0-neo4j-4.0.3</version>
    </dependency>
~~~

The version specified on the last version line can be changed to match the version you wish to work with (based on the version of Neo4j itself you are using). Too see which versions are available see the list at [Neo4j Spatial Releases](https://github.com/neo4j-contrib/m2/tree/master/releases/org/neo4j/neo4j-spatial).

## Running Neo4j spatial code from the command-line ##

Some of the classes in Neoj4-Spatial include main() methods and can be run on the command-line.
For example there are command-line options for importing SHP and OSM data. See the main methods
in the OSMImporter and ShapefileImporter classes. Here we will describe how to setup the dependencies
for running the command-line, using the OSMImporter and the sample OSM file two-street.osm.
We will show two ways to run this on the command line, one with the java command itself, and the
other using the 'exec:java' target in maven. In both cases we use maven to setup the dependencies.

### Compile ###

~~~bash
    git clone git://github.com/neo4j-contrib/spatial.git
    cd spatial
    mvn clean compile
~~~

### Run using JAVA command ###

~~~bash
    mvn dependency:copy-dependencies
    java -cp target/classes:target/dependency/* org.neo4j.gis.spatial.osm.OSMImporter osm-db two-street.osm 
~~~

_Note: On windows remember to separate the classpath with ';' instead of ':'._

The first command above only needs to be run once, to get a copy of all required JAR files into the directory target/dependency.
Once this is done, all further java commands with the -cp specifying that directory will load all dependencies.
It is likely that the specific command being run does not require all dependencies copied, since it will only be using
parts of the Neo4j-Spatial library, but working out exactly which dependencies are required can take a little time, so
the above approach is most certainly the easiest way to do this.

### Run using 'mvn exec:java' ###

~~~bash
    mvn exec:java -Dexec.mainClass=org.neo4j.gis.spatial.osm.OSMImporter -Dexec.args="osm-db two-street.osm"
~~~

Note that the OSMImporter cannot re-import the same data multiple times,
so you need to delete the database between runs if you are planning to do that.
