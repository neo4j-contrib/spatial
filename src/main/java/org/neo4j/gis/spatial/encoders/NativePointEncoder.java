/*
 * Copyright (c) 2010-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j Spatial.
 * <p>
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.encoders;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jCRS;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jPoint;
import org.neo4j.graphdb.PropertyContainer;

import java.util.List;

/**
 * Simple encoder that stores point geometries as one Neo4j Point property.
 */
public class NativePointEncoder extends AbstractGeometryEncoder implements Configurable {
    private static final String DEFAULT_GEOM = "location";
    private static GeometryFactory geometryFactory;
    private String locationProperty = DEFAULT_GEOM;
    private Neo4jCRS crs = Neo4jCRS.findCRS("WGS-84");

    protected GeometryFactory getGeometryFactory() {
        if (geometryFactory == null) geometryFactory = new GeometryFactory();
        return geometryFactory;
    }

    @Override
    protected void encodeGeometryShape(Geometry geometry,
                                       PropertyContainer container) {
        int gtype = SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass());
        if (gtype == GTYPE_POINT) {
            container.setProperty("gtype", gtype);
            Neo4jPoint neo4jPoint = new Neo4jPoint((Point) geometry, crs);
            container.setProperty(locationProperty, neo4jPoint);
        } else {
            throw new IllegalArgumentException("Cannot store non-Point types as Native Neo4j properties: " + SpatialDatabaseService.convertGeometryTypeToName(gtype));
        }

    }

    @Override
    public Geometry decodeGeometry(PropertyContainer container) {
        org.neo4j.graphdb.spatial.Point point = ((org.neo4j.graphdb.spatial.Point) container.getProperty(locationProperty));
        if (point.getCRS().getCode() != crs.getCode()) {
            throw new IllegalStateException("Trying to decode geometry with wrong CRS: layer configured to crs=" + crs + ", but geometry has crs=" + point.getCRS().getCode());
        }
        List<Double> coordinate = point.getCoordinate().getCoordinate();
        if (crs.dimensions() == 3) {
            return getGeometryFactory().createPoint(new Coordinate(coordinate.get(0), coordinate.get(1), coordinate.get(2)));
        } else {
            return getGeometryFactory().createPoint(new Coordinate(coordinate.get(0), coordinate.get(1)));
        }
    }

    @Override
    public String getConfiguration() {
        return locationProperty + ":" + bboxProperty + ": " + crs.getCode();
    }

    @Override
    public void setConfiguration(String configuration) {
        if (configuration != null && configuration.trim().length() > 0) {
            String[] fields = configuration.split(":");
            if (fields.length > 0) locationProperty = fields[0];
            if (fields.length > 1) bboxProperty = fields[1];
            if (fields.length > 2) crs = Neo4jCRS.findCRS(fields[2]);
        }
    }

    @Override
    public String getSignature() {
        return "NativePointEncoder(geometry='" + locationProperty + "', bbox='" + bboxProperty + "', crs=" + crs.getCode() + ")";
    }
}