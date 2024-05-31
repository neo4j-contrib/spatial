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

package org.neo4j.gis.spatial.functions;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.procedures.SpatialProcedures.GeometryResult;
import org.neo4j.gis.spatial.utilities.GeoJsonUtils;
import org.neo4j.gis.spatial.utilities.SpatialApiBase;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class SpatialFunctions extends SpatialApiBase {

	@UserFunction("spatial.decodeGeometry")
	@Description("Returns a geometry of a layer node as the Neo4j geometry type, to be passed to other procedures or returned to a client")
	public Object decodeGeometry(
			@Name("layerName") String name,
			@Name("node") Node node) {

		Layer layer = getLayerOrThrow(tx, spatial(), name);
		GeometryResult result = new GeometryResult(
				toNeo4jGeometry(layer, layer.getGeometryEncoder().decodeGeometry(node)));
		return result.geometry;
	}

	@UserFunction("spatial.asMap")
	@Description("Returns a Map object representing the Geometry, to be passed to other procedures or returned to a client")
	public Object asMap(@Name("object") Object geometry) {
		return toGeometryMap(geometry);
	}

	@UserFunction("spatial.asGeometry")
	@Description("Returns a geometry object as the Neo4j geometry type, to be passed to other functions or procedures or returned to a client")
	public Object asGeometry(@Name("geometry") Object geometry) {
		return toNeo4jGeometry(null, geometry);
	}


	@UserFunction("spatial.wktToGeoJson")
	@Description("Converts a WKT to GeoJson structure")
	public Object wktToGeoJson(@Name("wkt") String wkt) throws ParseException {
		if (wkt == null) {
			return null;
		}
		WKTReader wktReader = new WKTReader();
		Geometry geometry = wktReader.read(wkt);
		return GeoJsonUtils.toGeoJsonStructure(geometry);
	}
}
