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

colors = ["#FFFF00","#7FFF00","#00FF00","#00FF7F","#00FFFF",
          "#007FFF","#0000FF","#7F00FF","#FF00FF","#FF007F",
          "#FE0EF0","#FF07F0","#0F00F0","#0F70FF","#0FFF0F",
          "#070FFF","#00F00F","#07F0FF","#F0FFF0","#7F00FF",
          "#060EEE","#00E00E","#06E0EE","#E0EEE0","#4E00EE"]

colors.each_with_index do |color,index|
  user_rank = index + 1
  options = {:stroke => color, :fill => color, :fill_opacity => 0.5}
  sld.comment "Style geometries where the user_rank is #{user_rank}"
  block = Proc.new do |f|
    f.property['user_rank'] = user_rank
  end
  sld.add_point_symbolizer(options.merge(:geometry => 'Point'),&block)
  sld.add_line_symbolizer(options.merge(:stroke_width => 1, :geometry => 'LineString'),&block)
  sld.add_polygon_symbolizer(options.merge(:geometry => 'Polygon'),&block)
end

puts sld.to_xml(:tab => '    ')

