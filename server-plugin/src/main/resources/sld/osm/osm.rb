#!/usr/bin/env ruby

# This script uses the amanzi-sld DSL to create a sample SLD file
# for styling a number of layers in a typical OSM data model.
# This example should work with a layer containing all geometries.

require 'amanzi/sld'

Amanzi::SLD::Config.config[:geometry_property] = 'the_geom'
#Amanzi::SLD::Config.config[:verbose] = true

sld = Amanzi::SLD::Document.new "Example Neo4j Spatial OSM Style"

sld.comment "A catch-all style for all ways, drawn first so it only shows if anyther style does not apply"
sld.add_line_symbolizer(:stroke => '#dddddd')

sld.comment "A catch-all style for all Polygons"
sld.add_polygon_symbolizer(
  :fill => '#aaaaaa',
  :fill_opacity => '0.4',
  :stroke => '#ffe0e0',
  :geometry => 'Polygon'
)

sld.comment "Color areas of land-use"
sld.add_polygon_symbolizer(
  :fill => '#d0d0d0',
  :fill_opacity => '0.6',
  :stroke => '#e0e0e0',
  :geometry => 'Polygon'
) do |f|
  f.property.exists? 'landuse'
end

# Old code below, copied from test.rb

sld.comment "A catch-all style for all ways, drawn first so it only shows if anyther style does not apply"
sld.
  add_line_symbolizer(:stroke_width => 7, :stroke => '#303030').
  add_line_symbolizer(:stroke_width => 5, :stroke => '#e0e0ff') do |f|
  f.op(:and) do |f|
    f.property.exists? :highway
    f.op(:or) do |f|
      f.property[:highway] = 'secondary'
      f.property[:highway] = 'tertiary'
    end
  end
end

#puts sld.to_xml(:tab => '    ')

sld

