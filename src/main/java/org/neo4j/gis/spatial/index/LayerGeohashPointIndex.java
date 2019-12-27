/*
 * Copyright (c) 2010-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.gis.spatial.index;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.apache.lucene.spatial.util.GeoHashUtils;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.filter.AbstractSearchEnvelopeIntersection;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Node;

public class LayerGeohashPointIndex extends ExplicitIndexBackedPointIndex<String> {

    @Override
    protected String indexTypeName() {
        return "geohash";
    }

    @Override
    protected String getIndexValueFor(Node geomNode) {
        //TODO: Make this code projection aware - currently it assumes lat/lon
        Geometry geom = layer.getGeometryEncoder().decodeGeometry(geomNode);
        Point point = geom.getCentroid();   // Other code is ensuring only point layers use this, but just in case we encode the centroid
        return GeoHashUtils.stringEncode(point.getX(), point.getY());
    }

    private String greatestCommonPrefix(String a, String b) {
        int minLength = Math.min(a.length(), b.length());
        for (int i = 0; i < minLength; i++) {
            if (a.charAt(i) != b.charAt(i)) {
                return a.substring(0, i);
            }
        }
        return a.substring(0, minLength);
    }

    protected String queryStringFor(SearchFilter filter) {
        if (filter instanceof AbstractSearchEnvelopeIntersection) {
            Envelope referenceEnvelope = ((AbstractSearchEnvelopeIntersection) filter).getReferenceEnvelope();
            String maxHash = GeoHashUtils.stringEncode(referenceEnvelope.getMaxX(), referenceEnvelope.getMaxY());
            String minHash = GeoHashUtils.stringEncode(referenceEnvelope.getMinX(), referenceEnvelope.getMinY());
            return greatestCommonPrefix(minHash, maxHash) + "*";
        } else {
            throw new UnsupportedOperationException("Geohash Index only supports searches based on AbstractSearchEnvelopeIntersection, not " + filter.getClass().getCanonicalName());
        }
    }
}
