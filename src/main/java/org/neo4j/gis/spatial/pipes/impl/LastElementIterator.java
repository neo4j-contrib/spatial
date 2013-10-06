package org.neo4j.gis.spatial.pipes.impl;

import java.util.Iterator;

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
}
