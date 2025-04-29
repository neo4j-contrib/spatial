/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Spatial.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial;

import org.neo4j.graphdb.Label;

/**
 * @author Davide Savazzi
 */
public interface Constants {

	// Node properties

	String PROP_BBOX = "bbox";
	String PROP_LAYER = "layer";
	String PROP_LAYERNODEEXTRAPROPS = "layerprops";
	String PROP_CRS = "layercrs";
	String PROP_GEOMENCODER = "geomencoder";
	String PROP_INDEX_CLASS = "index_class";
	String PROP_GEOMENCODER_CONFIG = "geomencoder_config";
	String PROP_INDEX_CONFIG = "index_config";
	String PROP_LAYER_CLASS = "layer_class";

	String PROP_TYPE = "gtype";
	String PROP_QUERY = "query";
	String PROP_WKB = "wkb";
	String PROP_WKT = "wkt";
	String PROP_GEOM = "geometry";

	String[] RESERVED_PROPS = new String[]{
			PROP_BBOX,
			PROP_LAYER,
			PROP_LAYERNODEEXTRAPROPS,
			PROP_CRS,
			PROP_TYPE,
			PROP_WKB,
			PROP_WKT,
			PROP_GEOM
	};

	Label LABEL_LAYER = Label.label("SpatialLayer");

	// OpenGIS geometry type numbers

	int GTYPE_GEOMETRY = 0;
	int GTYPE_POINT = 1;
	int GTYPE_LINESTRING = 2;
	int GTYPE_POLYGON = 3;
	int GTYPE_MULTIPOINT = 4;
	int GTYPE_MULTILINESTRING = 5;
	int GTYPE_MULTIPOLYGON = 6;

	int SRID_COORDINATES_2D = 4326;
	int SRID_COORDINATES_3D = 4979;

	String INDEX_TYPE_RTREE = "rtree";
	String INDEX_TYPE_GEOHASH = "geohash";
	String INDEX_TYPE_ZORDER = "zorder";
	String INDEX_TYPE_HILBERT = "hilbert";

	String WGS84_CRS_NAME = "wgs84";


	String DOC_LAYER_NAME = "The name of the layer";
	String DOC_LAYER_TYPE = "The type of the new layer. Existing layer types can be queried by `spatial.layer`";
	String DOC_ENCODER_CONFIG = "The configuration of the encoder that is used by layer to en-/decode the geometry of indexed nodes";
	String DOC_INDEX_CONFIG = "The configuration of the newly created index";
	String DOC_ENCODER_NAME = "The name of the encoder to be used for the new layer";
	String DOC_INDEX_TYPE =
			"The type of the index algorithm to be used, valid values are: `" + INDEX_TYPE_RTREE + "`, `"
					+ INDEX_TYPE_GEOHASH + "`, `" + INDEX_TYPE_ZORDER + "` or `" + INDEX_TYPE_HILBERT + "`";
	String DOC_CRS = "The CRS to be used, valid values are: `" + WGS84_CRS_NAME + "`";
	String DOC_COORDINATE = """
			A valid value for the coordinate is a:
			
			* JTS Coordinate
			* Neo4j Coordinate
			* Neo4j Point
			* Map with keys 'latitude' and 'longitude'
			* Map with keys 'lat' and 'lon'"
			* Node or Relationship with properties 'latitude' and 'longitude'
			* Node or Relationship with properties 'lat' and 'lon'
			""";
	String DOC_URI = "The path of the file to import the data from";

	String DOC_JTS_GEOMETRY = "A WKT string or a coordinate. " + DOC_COORDINATE;
}
