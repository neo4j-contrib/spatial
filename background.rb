#!/usr/bin/env ruby

# This script uses the amanzi-sld DSL to create a sample SLD file
# for styling a number of layers in a typical OSM data model.
# This example should work with a layer containing all geometries.

# useful if being run inside a source code checkout
$: << 'lib'
$: << '../lib'
$: << '../amanzi-sld/lib'

#require 'rubygems'
require 'amanzi/sld'

Amanzi::SLD::Config.config[:geometry_property] = 'the_geom'
#Amanzi::SLD::Config.config[:verbose] = true

sld = Amanzi::SLD::Document.new "Example Neo4j Spatial OSM Style"

sld.comment "A catch-all style for all ways, drawn first so it only shows if anyther style does not apply"
sld.add_line_symbolizer(:stroke => '#444444')

sld.comment "A catch-all style for all Polygons"
sld.add_polygon_symbolizer(
  :fill => '#444444',
  :fill_opacity => '0.4',
  :stroke => '#444444',
  :geometry => 'Polygon'
)

puts sld.to_xml(:tab => '    ')

