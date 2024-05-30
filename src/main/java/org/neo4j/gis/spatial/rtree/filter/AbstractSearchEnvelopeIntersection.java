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
package org.neo4j.gis.spatial.rtree.filter;

import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.EnvelopeDecoder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public abstract class AbstractSearchEnvelopeIntersection implements SearchFilter {

	protected final EnvelopeDecoder decoder;
	protected final Envelope referenceEnvelope;

	public AbstractSearchEnvelopeIntersection(EnvelopeDecoder decoder, Envelope referenceEnvelope) {
		this.decoder = decoder;
		this.referenceEnvelope = referenceEnvelope;
	}

	public Envelope getReferenceEnvelope() {
		return referenceEnvelope;
	}

	@Override
	public boolean needsToVisit(Envelope indexNodeEnvelope) {
		return indexNodeEnvelope.intersects(referenceEnvelope);
	}

	@Override
	public boolean geometryMatches(Transaction tx, Node geomNode) {
		Envelope geomEnvelope = decoder.decodeEnvelope(geomNode);
		if (geomEnvelope.intersects(referenceEnvelope)) {
			return onEnvelopeIntersection(geomNode, geomEnvelope);
		}
		return false;
	}

	@Override
	public String toString() {
		return "SearchEnvelopeIntersection[" + referenceEnvelope + "]";
	}

	protected abstract boolean onEnvelopeIntersection(Node geomNode, Envelope geomEnvelope);
}
