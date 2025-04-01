## History

This library began as a collaborative vision between Neo-Technology
and [Craig Taverner](https://github.com/craigtaverner) in early 2010.
The bulk of the initial work was done by [Davide Savazzi](https://github.com/svzdvd) as part of his 2010 Google Summer
of Code (GSoC) project
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
* 0.29: Port to Neo4j 5.13

However, despite all these improvements, the core of the library only exposes the rich capabilities of JTS and GeoTools
if used in an embedded environment.
The large effort required to port the library to Neo4j 4.0 resulted in many backwards incompatibilities and
highlighted the need for a new approach to spatial libraries for Neo4j.
