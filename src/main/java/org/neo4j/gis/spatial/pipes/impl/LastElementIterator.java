package org.neo4j.gis.spatial.pipes.impl;

import java.util.Iterator;
import org.neo4j.gis.spatial.utilities.RelationshipTraversal;

public class LastElementIterator<T> implements Iterator<T> {

	private final Iterator<T> source;
	private T lastElement;

	public LastElementIterator(final Iterator<T> source) {
		this.source = source;
	}

	public boolean hasNext() {
		return source.hasNext();
	}

	public T next() {
		return lastElement = source.next();
	}

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
