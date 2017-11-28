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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.index.hilbert.HilbertSpaceFillingCurve;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.filter.AbstractSearchEnvelopeIntersection;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Node;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.util.List;

public class LayerHilbertPointIndex extends ExplicitIndexBackedPointIndex<Long> {

    private HilbertSpaceFillingCurve curve = null;

    @Override
    protected String indexTypeName() {
        return "hilbert";
    }

    @Override
    public void init(Layer layer) {
        super.init(layer);
        CoordinateReferenceSystem crs = layer.getCoordinateReferenceSystem();
        if (crs == null) {
            throw new IllegalArgumentException("HilbertPointIndex cannot support layers without CRS");
        }
        if (crs.getCoordinateSystem().getDimension() != 2) {
            throw new IllegalArgumentException("HilbertPointIndex cannot support CRS that is not 2D: " + crs.getName());
        }
        Envelope envelope = new Envelope(
                crs.getCoordinateSystem().getAxis(0).getMinimumValue(),
                crs.getCoordinateSystem().getAxis(0).getMaximumValue(),
                crs.getCoordinateSystem().getAxis(1).getMinimumValue(),
                crs.getCoordinateSystem().getAxis(1).getMaximumValue()
        );
        this.curve = new HilbertSpaceFillingCurve(envelope,8);
    }

    @Override
    protected Long getIndexValueFor(Node geomNode) {
        //TODO: Make this code projection aware - currently it assumes lat/lon
        Geometry geom = layer.getGeometryEncoder().decodeGeometry(geomNode);
        Point point = geom.getCentroid();   // Other code is ensuring only point layers use this, but just in case we encode the centroid
        return curve.longValueFor(point.getX(), point.getY());
    }

    private void appendRange(StringBuilder sb, HilbertSpaceFillingCurve.LongRange range) {
        if (range.min == range.max) {
            sb.append(indexTypeName()).append(":").append(range.min);
        } else {
            sb.append(indexTypeName()).append(":[").append(range.min).append(" TO ").append(range.max).append("]");
        }
    }

    protected String queryStringFor(SearchFilter filter) {
        if (filter instanceof AbstractSearchEnvelopeIntersection) {
            Envelope referenceEnvelope = ((AbstractSearchEnvelopeIntersection) filter).getReferenceEnvelope();
            List<HilbertSpaceFillingCurve.LongRange> tiles = curve.getTilesIntersectingEnvelope(referenceEnvelope);
            StringBuilder sb = new StringBuilder();
            for (HilbertSpaceFillingCurve.LongRange range : tiles) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                appendRange(sb, range);
            }
            return sb.toString();
        } else {
            throw new UnsupportedOperationException("Hilbert Index only supports searches based on AbstractSearchEnvelopeIntersection, not " + filter.getClass().getCanonicalName());
        }
    }
}
