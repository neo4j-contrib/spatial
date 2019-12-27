/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.rtree.filter;

import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.EnvelopeDecoder;
import org.neo4j.graphdb.Node;
import org.opengis.filter.FilterVisitor;

public abstract class AbstractSearchEnvelopeIntersection implements SearchFilter {
	
	protected EnvelopeDecoder decoder;
	protected Envelope referenceEnvelope;

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
	public boolean evaluate(Object o) {
		Node geomNode = (Node) o;
		Envelope geomEnvelope = decoder.decodeEnvelope(geomNode);
		if (geomEnvelope.intersects(referenceEnvelope)) {
			return onEnvelopeIntersection(geomNode, geomEnvelope);
		}

		return false;
	}

	@Override
	public Object accept(FilterVisitor filterVisitor, Object o) {
		return filterVisitor.visitNullFilter(o);
	}

	@Override
	public String toString() {
		return "SearchEnvelopeIntersection[" + referenceEnvelope + "]";
	}
	
	protected abstract boolean onEnvelopeIntersection(Node geomNode, Envelope geomEnvelope);
}