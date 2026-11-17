# Neo4j Spatial

[![build](https://github.com/neo4j-contrib/spatial/actions/workflows/pr-build.yaml/badge.svg?branch=master)](https://github.com/neo4j-contrib/spatial/actions/workflows/pr-build.yaml)

Neo4j Spatial is a library facilitating the import, storage and querying of spatial data in
the [Neo4j open source graph database](http://neo4j.org/).

This project's manual is deployed to the [Neo4j Labs page](https://neo4j.com/labs/neo4j-spatial/5/).
## Versioning

Since version 5.19.0, the versioning of Neo4j Spatial is aligned with the versioning of Neo4j itself.
This means that Neo4j Spatial 5.19.0 is build against Neo4j 5.19, and so on.

## Installation

1. Copy the desired neo4j-spatial-server-plugin-x.x.x-with-dependencies.jar from the [release page](https://github.com/neo4j-contrib/spatial/releases) to your neo4j plugin directory.

   NOTE: OpenStreetMap (OSM) features are shipped in a separate, optional plugin JAR (`neo4j-spatial-osm-server-plugin-x.x.x-with-dependencies.jar`). To enable OSM-specific imports and endpoints, add that JAR along with the main server-plugin to `$NEO4J_HOME/plugins` and restart Neo4j.

2. set up your database with the following configuration in your `neo4j.conf` file:

    ```
    dbms.security.procedures.unrestricted=spatial.*
    ```

   NOTE: If you're concerned about the security risks of unrestricted access, we recommend reviewing the code to assess
   the level of risk for your use case. For example, the method 
   [`IndexAccessMode.withIndexCreate`](/server-plugin/src/main/java/org/neo4j/gis/spatial/index/IndexManager.java#L42) grants index
   creation capabilities within the security model. This allows users without index creation privileges to generate the
   required spatial support indexes. This behavior was not intentionally designed to bypass security but was necessary
   due to Neo4j’s security model, where procedures with WRITE mode are not permitted to create indexes.
   As a result, this adjustment was required even in the Community Edition of Neo4j.

3. Restart your Neo4j server.
4. Run `CALL spatial.procedures` in the Neo4j browser to see the available spatial procedures.

### Docker

Use the official [`neo4j`](https://hub.docker.com/_/neo4j) image with the same version as the plugin, and mount the
directory holding the plugin JAR(s) into `/plugins`:

~~~bash
docker run -p 7474:7474 -p 7687:7687 \
  -v "$PWD/plugins:/plugins" \
  -e NEO4J_dbms_security_procedures_unrestricted='spatial.*' \
  neo4j:2026.10.0
~~~

## Concurrency

Each layer supports only one writer at a time. The RTree index has no locking of its own, so concurrent writes to the
same layer (`spatial.addNode(s)`, `spatial.addWKT(s)`, `spatial.removeNode(s)`, `spatial.updateWKT`, imports) can fail
with a `DeadlockDetectedException` and need to be retried. Serialize writes per layer; reads, and writes to different
layers, can run concurrently.

## Concept Overview

The key concepts of this library include:

* Allow the user to model geograph data in whatever way they wish, through providing an adapter (
  extend `GeometryEncoder`).
  Built-in encoders include:
	* WKT and WKB stored as properties of nodes
	* Simple points as properties of nodes (two doubles, or a double[] or a native Neo4j `Point`)
	* OpenStreetMap with complex geometries stored as sub-graphs to reflect the original topology of the OSM model
* Multiple CoordinationReferenceSystem support using GeoTools
* Support the concept of multiple geographic layers, each with its own CRS and Index
* Include an index capable of searching for complex geometries (in-graph RTree index)
* Support import and export in a number of known formats (e.g. Shapefile and OSM) (Note: OSM support is optional and provided by the separate `neo4j-spatial-osm-server-plugin-x.x.x-with-dependencies` JAR)
* Expose layers to GeoTools-based applications like GeoServer

Some key features include:

* Utilities for importing from ESRI Shapefile as well as Open Street Map files
* Support for all the common geometry types
* An RTree index for fast searches on geometries
* Support for topology operations during the search (contains, within, intersects, covers, disjoint, etc.)
* The possibility to enable spatial operations on any graph of data, regardless of the way the spatial data is stored,
  as long as an adapter is provided to map from the graph to the geometries.
* Cypher procedures and functions for all of the above; see the
  [reference](https://neo4j.com/labs/neo4j-spatial/5/api/) or run `CALL spatial.procedures`

## Index and Querying ##

The current index is an RTree index, but it has been developed in an extensible way allowing for other indices to be
added if necessary.
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

~~~bash
./mvnw clean install
~~~

This will download all dependencies, compile the library, run the tests and install the artifacts in your local
repository. The plugin JARs are written to each module's `target` directory, and can be copied to your local server
using the instructions from the [installation section](#installation).

Generated documentation under `docs/` (API pages, GeoPipes examples and images) is only written with the
`generate-docs` profile; CI fails if the committed `.adoc` files are stale:

~~~bash
./mvnw -Pgenerate-docs verify -pl doc-examples -am
~~~

## Layers and GeometryEncoders ##

See [Layers and GeometryEncoders](https://neo4j.com/labs/neo4j-spatial/5/dev/layers/) in the manual.

## Examples ##

### Importing a shapefile ###

Spatial data is divided in Layers and indexed by a RTree.

~~~cypher
CALL spatial.addWKTLayer('layer_roads', 'geometry');
CALL spatial.importShapefileToLayer('layer_roads', 'roads.shp');
~~~

### Importing an Open Street Map file ###

Requires the OSM plugin JAR.

~~~cypher
CALL spatial.importOSM('sweden.osm');
~~~

### Executing a spatial query ###

~~~cypher
CALL spatial.bbox('layer_roads', {lon: 15.0, lat: 60.0}, {lon: 15.3, lat: 61.0})
~~~

Or using a polygon:

~~~cypher
WITH 'POLYGON((15.3 60.2, 15.3 60.4, 15.7 60.4, 15.7 60.2, 15.3 60.2))' AS polygon
CALL spatial.intersects('layer_roads', polygon) YIELD node
RETURN node.name AS name
~~~

For further examples, refer to
the [SpatialProceduresTest](server-plugin/src/test/java/org/neo4j/gis/spatial/procedures/SpatialProceduresTest.java)
class.

## Neo4j Spatial GeoServer Plugin ##

The `neo4j-spatial-geotools-plugin-x.x.x-with-dependencies.jar` from the
[release page](https://github.com/neo4j-contrib/spatial/releases) provides a GeoTools DataStore that connects to Neo4j
over Bolt. Each release builds against one GeoTools version; use it in the matching GeoServer release. See
[Neo4j Spatial in GeoServer](https://neo4j.com/labs/neo4j-spatial/5/appendix/geoserver/) for setup.
