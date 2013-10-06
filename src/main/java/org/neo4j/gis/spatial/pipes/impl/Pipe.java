package org.neo4j.gis.spatial.pipes.impl;

import java.util.Iterator;
import java.util.List;

/**
 * The generic interface for any Pipe implementation.
 * A Pipe takes/consumes objects of type S and returns/emits objects of type E.
 * S refers to <i>starts</i> and the E refers to <i>ends</i>.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Darrick Wiebe (darrick@innatesoftware.com)
 */
public interface Pipe<S, E> extends Iterator<E>, Iterable<E> {

    /**
     * Set an iterator of S objects to the head (start) of the pipe.
     *
     * @param starts the iterator of incoming objects
     */
    public void setStarts(Iterator<S> starts);

    /**
     * Set an iterable of S objects to the head (start) of the pipe.
     *
     * @param starts the iterable of incoming objects
     */
    public void setStarts(Iterable<S> starts);

    /**
     * Returns the transformation path to arrive at the current object of the pipe.
     *
     * @return a List of all of the objects traversed for the current iterator position of the pipe.
     */
    public List getPath();

    /**
     * A pipe may maintain state. Reset is used to remove state.
     * The general use case for reset() is to reuse a pipe in another computation without having to create a new Pipe object.
     */
    public void reset();
}
