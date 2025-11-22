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

package org.neo4j.spatial.api.layer;

import java.util.List;
import org.geotools.referencing.crs.AbstractCRS;
import org.neo4j.spatial.api.encoder.GeometryEncoder;
import org.neo4j.spatial.api.index.SpatialIndexWriter;

/**
 * Interface that provides a mechanism for managing a collection of predefined
 * layer configurations, referred to as layer-type presets. These presets define
 * mappings between layer types and their corresponding properties, components
 * and behaviors.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader} and must
 * be registered in META-INF/services/org.neo4j.spatial.api.layer.LayerTypePresets.
 * All registered presets from all implementations are combined into a single registry.
 * If multiple implementations define presets with the same type name (case-insensitive),
 * the last one loaded will override earlier registrations.</p>
 */
public interface LayerTypePresets {

	List<RegisteredLayerType> getLayerTypePresets();

	/**
	 * Support mapping a String (ex: 'SimplePoint') to the respective GeometryEncoder and Layer classes
	 * to allow for more streamlined method for creating Layers.
	 */
	record RegisteredLayerType(
			String typeName,
			Class<? extends GeometryEncoder> geometryEncoder,
			Class<? extends Layer> layerClass,
			AbstractCRS crs,
			Class<? extends SpatialIndexWriter> layerIndexClass,
			String defaultEncoderConfig
	) {

	}

}
