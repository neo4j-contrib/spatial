#!/usr/bin/env python

import mapnik
import os

m = mapnik.Map(2000, 2000, "+proj=latlong +datum=WGS84")
m.background = mapnik.Color('grey')

# buidling
s_building = mapnik.Style()
r_building = mapnik.Rule()
r_building.symbols.append(mapnik.PolygonSymbolizer(mapnik.Color('brown')))
# r_building.symbols.append(mapnik.LineSymbolizer(mapnik.Color('rgb(50%,50%,50%)'),0.1))
s_building.rules.append(r_building)
m.append_style('building', s_building)
l_building = mapnik.Layer('building', "+proj=latlong +datum=WGS84")
l_building.datasource = mapnik.Shapefile(file='target/export/building')
l_building.styles.append('building')

# natural
s_natural = mapnik.Style()
r_natural = mapnik.Rule()
r_natural.symbols.append(mapnik.PolygonSymbolizer(mapnik.Color('green')))
r_natural.symbols.append(mapnik.LineSymbolizer(mapnik.Color('rgb(50%,50%,50%)'), 0.1))
s_natural.rules.append(r_natural)
m.append_style('natural', s_natural)
l_natural = mapnik.Layer('natural', "+proj=latlong +datum=WGS84")
l_natural.datasource = mapnik.Shapefile(file='target/export/natural-wood')
l_natural.styles.append('natural')

# water
s_water = mapnik.Style()
r_water = mapnik.Rule()
r_water.symbols.append(mapnik.PolygonSymbolizer(mapnik.Color('steelblue')))
# r_water.symbols.append(mapnik.LineSymbolizer(mapnik.Color('rgb(50%,50%,50%)'),0.1))
s_water.rules.append(r_water)
m.append_style('water', s_water)
l_water = mapnik.Layer('water', "+proj=latlong +datum=WGS84")
l_water.datasource = mapnik.Shapefile(file='target/export/natural-water')
l_water.styles.append('water')

# highways
s_highway = mapnik.Style()
r_highway = mapnik.Rule()
road_stroke = mapnik.Stroke()
road_stroke.width = 2.0
# dashed lines
# road_stroke.add_dash(8, 4)
# road_stroke.add_dash(2, 2)
# road_stroke.add_dash(2, 2)
road_stroke.color = mapnik.Color('yellow')
road_stroke.line_cap = mapnik.line_cap.ROUND_CAP
# r_highway.symbols.append(mapnik.PolygonSymbolizer(mapnik.Color('yellow')))
r_highway.symbols.append(mapnik.LineSymbolizer(road_stroke))
s_highway.rules.append(r_highway)
m.append_style('highway', s_highway)
l_highway = mapnik.Layer('highway', "+proj=latlong +datum=WGS84")
l_highway.datasource = mapnik.Shapefile(file='target/export/highway')
l_highway.styles.append('highway')
m.append_style('highway', s_highway)

m.layers.append(l_building)
m.layers.append(l_highway)
m.layers.append(l_water)
m.layers.append(l_natural)
m.zoom_to_box(l_highway.envelope())
img_loc = 'target/export.png'
mapnik.render_to_file(m, img_loc, 'png')
mapnik.save_map(m, "target/map.xml")
os.system('open ' + img_loc)
