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

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoUtils.checkLatitude;
import static org.apache.lucene.geo.GeoUtils.checkLongitude;

import java.util.Iterator;
import org.apache.lucene.util.BitUtil;
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
		long encoded = encode(point.getY(), point.getX());
		return geoTermToString(encoded);
	}

	private static String greatestCommonPrefix(String a, String b) {
		int minLength = Math.min(a.length(), b.length());
		for (int i = 0; i < minLength; i++) {
			if (a.charAt(i) != b.charAt(i)) {
				return a.substring(0, i);
			}
		}
		return a.substring(0, minLength);
	}

	@Override
	protected Neo4jIndexSearcher searcherFor(Transaction tx, SearchFilter filter) {
		if (filter instanceof AbstractSearchEnvelopeIntersection) {
			Envelope referenceEnvelope = ((AbstractSearchEnvelopeIntersection) filter).getReferenceEnvelope();
			String maxHash = geoTermToString(encode(referenceEnvelope.getMaxY(), referenceEnvelope.getMaxX()));
			String minHash = geoTermToString(encode(referenceEnvelope.getMinY(), referenceEnvelope.getMinX()));
			return new PrefixSearcher(greatestCommonPrefix(minHash, maxHash));
		}
		throw new UnsupportedOperationException(
				"Geohash Index only supports searches based on AbstractSearchEnvelopeIntersection, not "
						+ filter.getClass().getCanonicalName());
	}

	public static class PrefixSearcher implements Neo4jIndexSearcher {

		final String prefix;

		PrefixSearcher(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public Iterator<Node> search(KernelTransaction ktx, Label label, String propertyKey) {
			return ktx.internalTransaction().findNodes(label, propertyKey, prefix, StringSearchMode.PREFIX).stream()
					.iterator();
		}
	}

	public static long encode(double latitude, double longitude) {
		checkLatitude(latitude);
		checkLongitude(longitude);
		// encode lat/lon flipping the sign bit so negative ints sort before positive ints
		final int latEnc = encodeLatitude(latitude) ^ 0x80000000;
		final int lonEnc = encodeLongitude(longitude) ^ 0x80000000;
		return BitUtil.interleave(lonEnc, latEnc);
	}

	/**
	 * Converts a long value into a full 64 bit string (useful for debugging)
	 */
	public static String geoTermToString(long term) {
		StringBuilder s = new StringBuilder(64);
		final int numberOfLeadingZeros = Long.numberOfLeadingZeros(term);
		s.append("0".repeat(numberOfLeadingZeros));
		if (term != 0) {
			s.append(Long.toBinaryString(term));
		}
		return s.toString();
	}
}
