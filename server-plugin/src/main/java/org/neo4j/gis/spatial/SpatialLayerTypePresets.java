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

import java.util.List;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.neo4j.gis.spatial.encoders.AbstractSinglePropertyEncoder;
import org.neo4j.gis.spatial.encoders.NativePointEncoder;
import org.neo4j.gis.spatial.encoders.NativePointsEncoder;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.encoders.WKBGeometryEncoder;
import org.neo4j.gis.spatial.encoders.WKTGeometryEncoder;
import org.neo4j.gis.spatial.index.LayerGeohashPointIndex;
import org.neo4j.gis.spatial.index.LayerHilbertPointIndex;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.gis.spatial.index.LayerZOrderPointIndex;
import org.neo4j.spatial.api.layer.LayerTypePresets;

public class SpatialLayerTypePresets implements LayerTypePresets {

	@Override
	public List<RegisteredLayerType> getLayerTypePresets() {
		return List.of(
				RegisteredLayerType.of("SimplePoint", SimplePointEncoder.class,
						SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerRTreeIndex.class,
						new SimplePointEncoder.Config("longitude", "latitude")),
				RegisteredLayerType.of("Geohash", SimplePointEncoder.class,
						SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerGeohashPointIndex.class,
						new SimplePointEncoder.Config("longitude", "latitude")),
				RegisteredLayerType.of("ZOrder", SimplePointEncoder.class,
						SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerZOrderPointIndex.class,
						new SimplePointEncoder.Config("longitude", "latitude")),
				RegisteredLayerType.of("Hilbert", SimplePointEncoder.class,
						SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerHilbertPointIndex.class,
						new SimplePointEncoder.Config("longitude", "latitude")),
				RegisteredLayerType.of("NativePoint", NativePointEncoder.class,
						SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerRTreeIndex.class,
						new NativePointEncoder.Config("location")),
				RegisteredLayerType.of("NativePoints", NativePointsEncoder.class,
						EditableLayerImpl.class, DefaultGeographicCRS.WGS84, LayerRTreeIndex.class,
						new NativePointsEncoder.Config("geometry")),
				RegisteredLayerType.of("NativeGeohash", NativePointEncoder.class,
						SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerGeohashPointIndex.class,
						new NativePointEncoder.Config("location")),
				RegisteredLayerType.of("NativeZOrder", NativePointEncoder.class,
						SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerZOrderPointIndex.class,
						new NativePointEncoder.Config("location")),
				RegisteredLayerType.of("NativeHilbert", NativePointEncoder.class,
						SimplePointLayer.class, DefaultGeographicCRS.WGS84, LayerHilbertPointIndex.class,
						new NativePointEncoder.Config("location")),
				RegisteredLayerType.of("WKT", WKTGeometryEncoder.class, EditableLayerImpl.class,
						DefaultGeographicCRS.WGS84, LayerRTreeIndex.class,
						new AbstractSinglePropertyEncoder.Config("geometry")),
				RegisteredLayerType.of("WKB", WKBGeometryEncoder.class, EditableLayerImpl.class,
						DefaultGeographicCRS.WGS84, LayerRTreeIndex.class,
						new AbstractSinglePropertyEncoder.Config("geometry"))
		);
	}
}
