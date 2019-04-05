/**
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
package org.neo4j.gis.spatial;

import java.util.Comparator;
import java.util.Iterator;

import org.geotools.filter.AndImpl;
import org.geotools.filter.GeometryFilterImpl;
import org.geotools.filter.LiteralExpressionImpl;
import org.geotools.filter.spatial.BBOXImpl;
import org.geotools.filter.spatial.ContainsImpl;
import org.geotools.filter.spatial.CrossesImpl;
import org.geotools.filter.spatial.EqualsImpl;
import org.geotools.filter.spatial.IntersectsImpl;
import org.geotools.filter.spatial.OverlapsImpl;
import org.geotools.filter.spatial.TouchesImpl;
import org.geotools.filter.spatial.WithinImpl;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.EnvelopeDecoder;
import org.neo4j.graphdb.Node;
import org.opengis.filter.Filter;

import org.locationtech.jts.geom.Geometry;

public class Utilities {

	public static Envelope fromJtsToNeo4j(org.locationtech.jts.geom.Envelope envelope) {
		return new Envelope(
				envelope.getMinX(),
				envelope.getMaxX(),
				envelope.getMinY(),
				envelope.getMaxY()
		);
	}

	public static org.locationtech.jts.geom.Envelope fromNeo4jToJts(org.neo4j.gis.spatial.index.Envelope envelope) {
		return new org.locationtech.jts.geom.Envelope(
				envelope.getMinX(),
				envelope.getMaxX(),
				envelope.getMinY(),
				envelope.getMaxY()
		);
	}

	/**
	 * To create an optimized index search from a Filter we try to extract an Envelope from it.
	 */
	public static org.neo4j.gis.spatial.rtree.Envelope extractEnvelopeFromFilter(Filter filter) {
		return extractEnvelopeFromFilter(filter, true);
	}

	@SuppressWarnings("rawtypes")
	private static org.neo4j.gis.spatial.rtree.Envelope extractEnvelopeFromFilter(Filter filter, boolean inspectAndFilters) {
		if (filter instanceof BBOXImpl) {
			return extractEnvelopeFromBBox((BBOXImpl) filter);
		} else if (filter instanceof IntersectsImpl ||
				   filter instanceof ContainsImpl ||
				   filter instanceof CrossesImpl ||
				   filter instanceof EqualsImpl ||
				   filter instanceof OverlapsImpl ||
				   filter instanceof TouchesImpl ||
				   filter instanceof WithinImpl) {			
			return extractEnvelopeFromGeometryFilter((GeometryFilterImpl) filter);
		} else if (filter instanceof AndImpl && inspectAndFilters) {
			AndImpl andFilter = (AndImpl) filter;
			Iterator children = andFilter.getFilterIterator();
			while (children.hasNext()) {
				Filter child = (Filter) children.next();
				Envelope result = extractEnvelopeFromFilter(child, false);
				if (result != null) {
					return result;
				}
			}
		}
		
		return null;
	}
		
	@SuppressWarnings("deprecation")
	private static Envelope extractEnvelopeFromGeometryFilter(GeometryFilterImpl intersectFilter) {
		if (intersectFilter.getExpression1() instanceof LiteralExpressionImpl) {
			return extractEnvelopeFromLiteralExpression((LiteralExpressionImpl) intersectFilter.getExpression1());
		} else if (intersectFilter.getExpression2() instanceof LiteralExpressionImpl) {
			return extractEnvelopeFromLiteralExpression((LiteralExpressionImpl) intersectFilter.getExpression2());
		}
	
		return null;
	}
	
	private static Envelope extractEnvelopeFromLiteralExpression(LiteralExpressionImpl exp) {
		if (exp.getValue() instanceof Geometry) {
			return fromJtsToNeo4j(((Geometry) exp.getValue()).getEnvelopeInternal());
		} else {
			return null;
		}
	}
	
    private static Envelope extractEnvelopeFromBBox(BBOXImpl bbox) {
    	return new Envelope(bbox.getMinX(), bbox.getMaxX(), bbox.getMinY(), bbox.getMaxY());
    }


	/**
	 * Comparator for comparing nodes by compaing the xMin on their evelopes.
	 */
	public static class ComparatorOnXMin implements Comparator<Node> {
		final private EnvelopeDecoder decoder;

		public ComparatorOnXMin(EnvelopeDecoder decoder){
			this.decoder = decoder;
		}

		@Override
		public int compare(Node o1, Node o2) {
			return Double.compare(decoder.decodeEnvelope(o1).getMinX(), decoder.decodeEnvelope(o2).getMinX());
		}
	}

	/**
	 * Comparator or comparing nodes by coparing the yMin on their envelopes.
	 */
	public static class ComparatorOnYMin implements Comparator<Node> {
		final private EnvelopeDecoder decoder;

		public ComparatorOnYMin(EnvelopeDecoder decoder){
			this.decoder = decoder;
		}

		@Override
		public int compare(Node o1, Node o2) {
			return Double.compare(decoder.decodeEnvelope(o1).getMinY(), decoder.decodeEnvelope(o2).getMinY());
		}
	}

}
