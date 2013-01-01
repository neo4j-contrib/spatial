/**
 * Copyright (c) 2010-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.geotools.data.neo4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.BasicFeatureTypes;
import org.geotools.resources.Classes;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.SpatialRecord;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.GeometryType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class Neo4jFeatureBuilder {
	
    private static final String FEATURE_PROP_GEOM = "the_geom";
    private final SimpleFeatureBuilder builder;
    private final List<String> extraPropertyNames;
    
	/**
	 * 
	 */
    public Neo4jFeatureBuilder(SimpleFeatureType sft, List<String> extraPropertyNames) {
        this.builder = new SimpleFeatureBuilder(sft);
        this.extraPropertyNames = extraPropertyNames;
    }
    
	/**
	 * 
	 */
    public Neo4jFeatureBuilder(Layer layer) { 
        this(getTypeFromLayer(layer), Arrays.asList(layer.getExtraPropertyNames()));
    } 
    
    public SimpleFeature buildFeature(String id, Geometry geometry, Map<String,Object> properties) {
        builder.reset();
        builder.set(FEATURE_PROP_GEOM, geometry);
        if (extraPropertyNames != null) {
            for (String name : extraPropertyNames) {
                builder.set(name, properties.get(name));
            }
        }

        return builder.buildFeature(id);    	
    }
    
    public SimpleFeature buildFeature(SpatialRecord rec) {
    	return buildFeature(rec.getId(), rec.getGeometry(), rec.getProperties());
    }

    public static SimpleFeatureType getTypeFromLayer(Layer layer) {    
    	return getType(layer.getName(), layer.getGeometryType(), layer.getCoordinateReferenceSystem(), layer.getExtraPropertyNames());
    }
    
    public static SimpleFeatureType getType(String name, Integer geometryTypeId, CoordinateReferenceSystem crs, String[] extraPropertyNames) {
        List<AttributeDescriptor> types = readAttributes(geometryTypeId, crs, extraPropertyNames);

        // find Geometry type
        SimpleFeatureType parent = null;
        GeometryDescriptor geomDescriptor = (GeometryDescriptor) types.get(0);
        Class< ? > geomBinding = geomDescriptor.getType().getBinding();
        if ((geomBinding == Point.class) || (geomBinding == MultiPoint.class)) {
            parent = BasicFeatureTypes.POINT;
        } else if ((geomBinding == Polygon.class) || (geomBinding == MultiPolygon.class)) {
            parent = BasicFeatureTypes.POLYGON;
        } else if ((geomBinding == LineString.class) || (geomBinding == MultiLineString.class) || (geomBinding == LinearRing.class)) {
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

    private static List<AttributeDescriptor> readAttributes(Integer geometryTypeId, CoordinateReferenceSystem crs, String[] extraPropertyNames) {
        Class<? extends Geometry> geometryClass = SpatialDatabaseService.convertGeometryTypeToJtsClass(geometryTypeId);

        AttributeTypeBuilder build = new AttributeTypeBuilder();
        build.setName(Classes.getShortName(geometryClass));
        build.setNillable(true);
        build.setCRS(crs);
        build.setBinding(geometryClass);

        GeometryType geometryType = build.buildGeometryType();

        List<AttributeDescriptor> attributes = new ArrayList<AttributeDescriptor>();
        attributes.add(build.buildDescriptor(BasicFeatureTypes.GEOMETRY_ATTRIBUTE_NAME, geometryType));

        if (extraPropertyNames != null) {
            Set<String> usedNames = new HashSet<String>(); 
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

