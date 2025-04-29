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

package org.neo4j.gis.spatial.pipes.impl;

import java.util.Iterator;
import org.neo4j.gis.spatial.utilities.RelationshipTraversal;

public class LastElementIterator<T> implements Iterator<T> {

	private final Iterator<T> source;
	private T lastElement;

	public LastElementIterator(final Iterator<T> source) {
		this.source = source;
	}

	@Override
	public boolean hasNext() {
		return source.hasNext();
	}

	@Override
	public T next() {
		return lastElement = source.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove not supported");
	}

	public T lastElement() {
		return lastElement;
	}

	/**
	 * Work around bug in Neo4j 4.3 with leaked RelationshipTraversalCursor
	 */
	public void reset() {
		// TODO: rather try get deeper into the underlying index and close resources instead of exhausting the iterator
		// The challenge is that there are many sources, all of which need to be made closable, and that is very hard
		// to achieve in a generic way without a full-stack code change.
		RelationshipTraversal.exhaustIterator(source);
	}
}
