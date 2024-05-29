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
package org.geotools.data.neo4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.api.feature.type.GeometryDescriptor;
import org.geotools.api.feature.type.GeometryType;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.BasicFeatureTypes;
import org.geotools.util.Classes;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.SpatialRecord;
import org.neo4j.graphdb.Transaction;

public class Neo4jFeatureBuilder {

	private static final String FEATURE_PROP_GEOM = "the_geom";
	private final SimpleFeatureBuilder builder;
	private final List<String> extraPropertyNames;

	public Neo4jFeatureBuilder(SimpleFeatureType sft, List<String> extraPropertyNames) {
		this.builder = new SimpleFeatureBuilder(sft);
		this.extraPropertyNames = extraPropertyNames;
	}

	/**
	 * If it is necessary to look up the layer type with a transaction, use this factory method to make the feature
	 * builder
	 */
	public static Neo4jFeatureBuilder fromLayer(Transaction tx, Layer layer) {
		return new Neo4jFeatureBuilder(getTypeFromLayer(tx, layer), Arrays.asList(layer.getExtraPropertyNames(tx)));
	}

	public SimpleFeature buildFeature(String id, Geometry geometry, Map<String, Object> properties) {
		builder.reset();
		builder.set(FEATURE_PROP_GEOM, geometry);
		if (extraPropertyNames != null) {
			for (String name : extraPropertyNames) {
				builder.set(name, properties.get(name));
			}
		}

		return builder.buildFeature(id);
	}

	public SimpleFeature buildFeature(Transaction tx, SpatialRecord rec) {
		return buildFeature(rec.getId(), rec.getGeometry(), rec.getProperties(tx));
	}

	public static SimpleFeatureType getTypeFromLayer(Transaction tx, Layer layer) {
		return getType(layer.getName(), layer.getGeometryType(tx), layer.getCoordinateReferenceSystem(tx),
				layer.getExtraPropertyNames(tx));
	}

	public static SimpleFeatureType getType(String name, Integer geometryTypeId, CoordinateReferenceSystem crs,
			String[] extraPropertyNames) {
		List<AttributeDescriptor> types = readAttributes(geometryTypeId, crs, extraPropertyNames);

		// find Geometry type
		SimpleFeatureType parent = null;
		GeometryDescriptor geomDescriptor = (GeometryDescriptor) types.get(0);
		Class<?> geomBinding = geomDescriptor.getType().getBinding();
		if ((geomBinding == Point.class) || (geomBinding == MultiPoint.class)) {
			parent = BasicFeatureTypes.POINT;
		} else if ((geomBinding == Polygon.class) || (geomBinding == MultiPolygon.class)) {
			parent = BasicFeatureTypes.POLYGON;
		} else if ((geomBinding == LineString.class) || (geomBinding == MultiLineString.class) || (geomBinding
				== LinearRing.class)) {
			parent = BasicFeatureTypes.LINE;
		}

		SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.setDefaultGeometry(geomDescriptor.getLocalName());
		builder.addAll(types);
		builder.setName(name);
		builder.setNamespaceURI(BasicFeatureTypes.DEFAULT_NAMESPACE);
		builder.setAbstract(false);
		builder.setCRS(crs);
		if (parent != null) {
			builder.setSuperType(parent);
		}

		return builder.buildFeatureType();
	}

	private static List<AttributeDescriptor> readAttributes(Integer geometryTypeId, CoordinateReferenceSystem crs,
			String[] extraPropertyNames) {
		Class<? extends Geometry> geometryClass = SpatialDatabaseService.convertGeometryTypeToJtsClass(geometryTypeId);

		AttributeTypeBuilder build = new AttributeTypeBuilder();
		build.setName(Classes.getShortName(geometryClass));
		build.setNillable(true);
		build.setCRS(crs);
		build.setBinding(geometryClass);

		GeometryType geometryType = build.buildGeometryType();

		List<AttributeDescriptor> attributes = new ArrayList<>();
		attributes.add(build.buildDescriptor(BasicFeatureTypes.GEOMETRY_ATTRIBUTE_NAME, geometryType));

		if (extraPropertyNames != null) {
			Set<String> usedNames = new HashSet<>();
			// record names in case of duplicates
			usedNames.add(BasicFeatureTypes.GEOMETRY_ATTRIBUTE_NAME);

			for (String propertyName : extraPropertyNames) {
				if (!usedNames.contains(propertyName)) {
					usedNames.add(propertyName);

					build.setNillable(true);
					build.setBinding(String.class);

					attributes.add(build.buildDescriptor(propertyName));
				}
			}
		}

		return attributes;
	}
}

