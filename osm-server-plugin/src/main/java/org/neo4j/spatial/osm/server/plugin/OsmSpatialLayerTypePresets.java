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

package org.neo4j.spatial.osm.server.plugin;

import java.util.List;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.spatial.api.layer.LayerTypePresets;

public class OsmSpatialLayerTypePresets implements LayerTypePresets {

	@Override
	public List<RegisteredLayerType> getLayerTypePresets() {
		return List.of(
				new RegisteredLayerType("OSM", OSMGeometryEncoder.class, OSMLayer.class,
						DefaultGeographicCRS.WGS84, LayerRTreeIndex.class, "geometry")
		);
	}
}
