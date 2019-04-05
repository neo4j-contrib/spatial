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
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.gis.spatial.rtree.filter.AbstractSearchEnvelopeIntersection;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Node;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;

import java.util.List;

public abstract class LayerSpaceFillingCurvePointIndex extends ExplicitIndexBackedPointIndex<Long> {

    private SpaceFillingCurve curve = null;

    @Override
    protected String indexTypeName() {
        return "hilbert";
    }

    private SpaceFillingCurve getCurve() {
        if (this.curve == null) {
            CoordinateReferenceSystem crs = layer.getCoordinateReferenceSystem();
            if (crs == null) {
                throw new IllegalArgumentException("HilbertPointIndex cannot support layers without CRS");
            }
            if (crs.getCoordinateSystem().getDimension() != 2) {
                throw new IllegalArgumentException("HilbertPointIndex cannot support CRS that is not 2D: " + crs.getName());
            }
            Envelope envelope = new Envelope(
                    getMin(crs.getCoordinateSystem().getAxis(0)),
                    getMax(crs.getCoordinateSystem().getAxis(0)),
                    getMin(crs.getCoordinateSystem().getAxis(1)),
                    getMax(crs.getCoordinateSystem().getAxis(1))
            );
            this.curve = makeCurve(envelope, 12);//new HilbertSpaceFillingCurve2D(envelope, 12);
        }
        return this.curve;
    }

    protected abstract SpaceFillingCurve makeCurve(Envelope envelope, int maxLevels);

    private double getMin(CoordinateSystemAxis axis) {
        double min = axis.getMinimumValue();
        if (Double.isInfinite(min)) return 0.0;
        else return min;
    }

    private double getMax(CoordinateSystemAxis axis) {
        double max = axis.getMaximumValue();
        if (Double.isInfinite(max)) return 1.0;
        else return max;
    }

    @Override
    protected Long getIndexValueFor(Node geomNode) {
        //TODO: Make this code projection aware - currently it assumes lat/lon
        Geometry geom = layer.getGeometryEncoder().decodeGeometry(geomNode);
        Point point = geom.getCentroid();   // Other code is ensuring only point layers use this, but just in case we encode the centroid
        return getCurve().derivedValueFor(new double[]{point.getX(), point.getY()});
    }

    private void appendRange(StringBuilder sb, SpaceFillingCurve.LongRange range) {
        if (range.min == range.max) {
            sb.append(indexTypeName()).append(":").append(range.min);
        } else {
            sb.append(indexTypeName()).append(":[").append(range.min).append(" TO ").append(range.max).append("]");
        }
    }

    protected String queryStringFor(SearchFilter filter) {
        if (filter instanceof AbstractSearchEnvelopeIntersection) {
            org.neo4j.gis.spatial.rtree.Envelope referenceEnvelope = ((AbstractSearchEnvelopeIntersection) filter).getReferenceEnvelope();
            List<SpaceFillingCurve.LongRange> tiles = getCurve().getTilesIntersectingEnvelope(referenceEnvelope.getMin(), referenceEnvelope.getMax(), new StandardConfiguration());
            StringBuilder sb = new StringBuilder();
            for (SpaceFillingCurve.LongRange range : tiles) {
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
