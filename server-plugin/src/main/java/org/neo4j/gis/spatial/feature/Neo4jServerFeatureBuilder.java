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

package org.neo4j.gis.spatial.feature;

import java.util.Collections;
import java.util.Map;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.spatial.api.SpatialRecord;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.geotools.common.feature.Neo4jFeatureBuilder;

public class Neo4jServerFeatureBuilder extends Neo4jFeatureBuilder {

	private static final String FEATURE_PROP_GEOM = "the_geom";

	private final SimpleFeatureBuilder builder;
	private final Map<String, Class<?>> extraProperties;

	public Neo4jServerFeatureBuilder(SimpleFeatureType sft, Map<String, Class<?>> extraProperties) {
		this.builder = new SimpleFeatureBuilder(sft);
		this.extraProperties = extraProperties == null ? Collections.emptyMap() : extraProperties;
	}

	public SimpleFeature buildFeature(String id, Geometry geometry, Map<String, Object> properties) {
		builder.reset();
		builder.set(FEATURE_PROP_GEOM, geometry);
		for (String name : extraProperties.keySet()) {
			builder.set(name, properties.get(name));
		}
		return builder.buildFeature(id);
	}

	/**
	 * If it is necessary to look up the layer type with a transaction, use this factory method to make the feature
	 * builder
	 */
	public static Neo4jServerFeatureBuilder fromLayer(Transaction tx, Layer layer) {
		return new Neo4jServerFeatureBuilder(getTypeFromLayer(tx, layer), layer.getExtraProperties(tx));
	}

	public SimpleFeature buildFeature(Transaction tx, SpatialRecord rec) {
		return buildFeature(rec.getId(), rec.getGeometry(), rec.getProperties(tx));
	}

	public static SimpleFeatureType getTypeFromLayer(Transaction tx, Layer layer) {
		return getType(
				layer.getName(),
				layer.getGeometryType(tx),
				layer.getCoordinateReferenceSystem(tx),
				layer.getGeometryEncoder().hasComplexAttributes(),
				layer.getExtraProperties(tx));
	}

	public static SimpleFeatureType getType(
			String name,
			Integer geometryTypeId,
			CoordinateReferenceSystem crs,
			boolean hasComplexAttributes,
			Map<String, Class<?>> extraProperties
	) {
		Class<? extends Geometry> geometryClass = SpatialDatabaseService.convertGeometryTypeToJtsClass(geometryTypeId);
		return getType(name, geometryClass, crs, hasComplexAttributes, extraProperties);
	}


}
