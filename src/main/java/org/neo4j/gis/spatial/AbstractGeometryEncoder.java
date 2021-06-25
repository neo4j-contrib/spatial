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

import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public abstract class AbstractGeometryEncoder implements GeometryEncoder, Constants {

    protected String bboxProperty = PROP_BBOX;
    protected Layer layer;

    @Override
    public void init(Transaction tx, Layer layer) {
        this.layer = layer;
    }

    public void encodeEnvelope(Envelope mbb, Entity container) {
        container.setProperty(bboxProperty, new double[]{mbb.getMinX(), mbb.getMinY(), mbb.getMaxX(), mbb.getMaxY()});
    }

    @Override
    public void ensureIndexable(Geometry geometry, Entity container) {
        container.setProperty(PROP_TYPE, encodeGeometryType(geometry.getGeometryType()));
        encodeEnvelope(Utilities.fromJtsToNeo4j(geometry.getEnvelopeInternal()), container);
    }

    @Override
    public void encodeGeometry(Transaction tx, Geometry geometry, Entity container) {
        ensureIndexable(geometry, container);
        encodeGeometryShape(tx, geometry, container);
    }

    @Override
    public Envelope decodeEnvelope(Entity container) {
        double[] bbox = new double[]{0, 0, 0, 0};
        Object bboxProp = container.getProperty(bboxProperty);
        if (bboxProp instanceof Double[]) {
            bbox = ArrayUtils.toPrimitive((Double[]) bboxProp);
        } else if (bboxProp instanceof double[]) {
            bbox = (double[]) bboxProp;
        }

        // Envelope parameters: xmin, xmax, ymin, ymax
        return new Envelope(bbox[0], bbox[2], bbox[1], bbox[3]);
    }


    // Protected methods

    protected abstract void encodeGeometryShape(Transaction tx, Geometry geometry, Entity container);

    protected Integer encodeGeometryType(String jtsGeometryType) {
        // TODO: Consider alternatives for specifying type, like relationship to
        // type category
        // objects (or similar indexing structure)
        if ("Point".equals(jtsGeometryType)) {
            return GTYPE_POINT;
        } else if ("MultiPoint".equals(jtsGeometryType)) {
            return GTYPE_MULTIPOINT;
        } else if ("LineString".equals(jtsGeometryType)) {
            return GTYPE_LINESTRING;
        } else if ("MultiLineString".equals(jtsGeometryType)) {
            return GTYPE_MULTILINESTRING;
        } else if ("Polygon".equals(jtsGeometryType)) {
            return GTYPE_POLYGON;
        } else if ("MultiPolygon".equals(jtsGeometryType)) {
            return GTYPE_MULTIPOLYGON;
        } else {
            throw new IllegalArgumentException("unknown type:" + jtsGeometryType);
        }
    }

    /**
     * This method wraps the hasProperty(String) method on the geometry node.
     * This means the default way of storing attributes is simply as properties
     * of the geometry node. This behaviour can be changed by other domain
     * models with different encodings.
     */
    public boolean hasAttribute(Node geomNode, String name) {
        return geomNode.hasProperty(name);
    }

    /**
     * This method wraps the getProperty(String,null) method on the geometry
     * node. This means the default way of storing attributes is simply as
     * properties of the geometry node. This behaviour can be changed by other
     * domain models with different encodings. If the property does not exist,
     * the method returns null.
     */
    public Object getAttribute(Node geomNode, String name) {
        return geomNode.getProperty(name, null);
    }

    /**
     * For external expression of the configuration of this geometry encoder
     *
     * @return descriptive signature of encoder, type and configuration
     */
    public String getSignature() {
        return "GeometryEncoder(bbox='" + bboxProperty + "')";
    }
}