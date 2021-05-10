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
package org.neo4j.gis.spatial.index;

import org.apache.lucene.spatial.util.MortonEncoder;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.filter.AbstractSearchEnvelopeIntersection;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.StringSearchMode;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Iterator;

public class LayerGeohashPointIndex extends ExplicitIndexBackedPointIndex<String> {

    @Override
    protected String indexTypeName() {
        return "geohash";
    }

    @Override
    protected String getIndexValueFor(Transaction tx, Node geomNode) {
        //TODO: Make this code projection aware - currently it assumes lat/lon
        Geometry geom = layer.getGeometryEncoder().decodeGeometry(geomNode);
        Point point = geom.getCentroid();   // Other code is ensuring only point layers use this, but just in case we encode the centroid
        long encoded = MortonEncoder.encode(point.getY(), point.getX());
        return MortonEncoder.geoTermToString(encoded);
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

    protected Neo4jIndexSearcher searcherFor(Transaction tx, SearchFilter filter) {
        if (filter instanceof AbstractSearchEnvelopeIntersection) {
            Envelope referenceEnvelope = ((AbstractSearchEnvelopeIntersection) filter).getReferenceEnvelope();
            String maxHash = MortonEncoder.geoTermToString(MortonEncoder.encode(referenceEnvelope.getMaxY(), referenceEnvelope.getMaxX()));
            String minHash = MortonEncoder.geoTermToString(MortonEncoder.encode(referenceEnvelope.getMinY(), referenceEnvelope.getMinX()));
            return new PrefixSearcher(greatestCommonPrefix(minHash, maxHash));
        } else {
            throw new UnsupportedOperationException("Geohash Index only supports searches based on AbstractSearchEnvelopeIntersection, not " + filter.getClass().getCanonicalName());
        }
    }

    public static class PrefixSearcher implements Neo4jIndexSearcher {
        final String prefix;

        PrefixSearcher(String prefix) {
            this.prefix = prefix;
        }

        public Iterator<Node> search(KernelTransaction ktx, Label label, String propertyKey) {
            return ktx.internalTransaction().findNodes(label, propertyKey, prefix, StringSearchMode.PREFIX);
        }
    }
}
