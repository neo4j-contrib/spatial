package org.neo4j.gis.spatial.pipes.impl;

import org.neo4j.helpers.collection.Iterators;

import java.util.*;

/**
 * A Pipeline is a linear composite of Pipes.
 * Pipeline takes a List of Pipes and joins them according to their order as specified by their location in the List.
 * It is important to ensure that the provided ordered Pipes can connect together.
 * That is, that the output type of the n-1 Pipe is the same as the input type of the n Pipe.
 * Once all provided Pipes are composed, a Pipeline can be treated like any other Pipe.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Pipeline<S, E> implements Pipe<S, E> {

    protected Pipe<S, ?> startPipe;
    protected Pipe<?, E> endPipe;
    protected List<Pipe> pipes;
    protected Iterator<S> starts;

    public Pipeline() {
        this.pipes = new ArrayList<Pipe>();
    }

    /**
     * Constructs a pipeline from the provided pipes. The ordered list determines how the pipes will be chained together.
     * When the pipes are chained together, the start of pipe n is the end of pipe n-1.
     *
     * @param pipes the ordered list of pipes to chain together into a pipeline
     */
    public Pipeline(final List<Pipe> pipes) {
        this.pipes = pipes;
        this.setPipes(pipes);
    }


    /**
     * Constructs a pipeline from the provided pipes. The ordered array determines how the pipes will be chained together.
     * When the pipes are chained together, the start of pipe n is the end of pipe n-1.
     *
     * @param pipes the ordered array of pipes to chain together into a pipeline
     */
    public Pipeline(final Pipe... pipes) {
        this(new ArrayList<Pipe>(Arrays.asList(pipes)));
    }

    /**
     * Useful for constructing the pipeline chain without making use of the constructor.
     *
     * @param pipes the ordered list of pipes to chain together into a pipeline
     */
    protected void setPipes(final List<Pipe> pipes) {
        this.startPipe = (Pipe<S, ?>) pipes.get(0);
        this.endPipe = (Pipe<?, E>) pipes.get(pipes.size() - 1);
        for (int i = 1; i < pipes.size(); i++) {
            pipes.get(i).setStarts((Iterator) pipes.get(i - 1));
        }
    }

    /**
     * Useful for constructing the pipeline chain without making use of the constructor.
     *
     * @param pipes the ordered array of pipes to chain together into a pipeline
     */
    protected void setPipes(final Pipe... pipes) {
        this.setPipes(Arrays.asList(pipes));
    }

    /**
     * Adds a new pipe to the end of the pipeline and then reconstructs the pipeline chain.
     *
     * @param pipe the new pipe to add to the pipeline
     */
    public void addPipe(final Pipe pipe) {
        this.pipes.add(pipe);
        this.setPipes(this.pipes);
    }

    public void setStarts(final Iterator<S> starts) {
        this.starts = starts;
        this.startPipe.setStarts(starts);
    }

    public void setStarts(final Iterable<S> starts) {
        this.setStarts(starts.iterator());
    }

    /**
     * An unsupported operation that throws an UnsupportedOperationException.
     */
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * Determines if there is another object that can be emitted from the pipeline.
     *
     * @return true if an object can be next()'d out of the pipeline
     */
    public boolean hasNext() {
        return this.endPipe.hasNext();
    }

    /**
     * Get the next object emitted from the pipeline.
     * If no such object exists, then a NoSuchElementException is thrown.
     *
     * @return the next emitted object
     */
    public E next() {
        return this.endPipe.next();
    }

    public List getPath() {
        return this.endPipe.getPath();
    }

    /**
     * Get the number of pipes in the pipeline.
     *
     * @return the pipeline length
     */
    public int size() {
        return this.pipes.size();
    }

    public void reset() {
        this.endPipe.reset();
    }

    /**
     * Simply returns this as as a pipeline (more specifically, pipe) implements Iterator.
     *
     * @return returns the iterator representation of this pipeline
     */
    public Iterator<E> iterator() {
        return this;
    }

    public String toString() {
        return this.pipes.toString();
    }

    public List<Pipe> getPipes() {
        return this.pipes;
    }

    public Iterator<S> getStarts() {
        return this.starts;
    }

    public Pipe remove(final int index) {
        return this.pipes.remove(index);
    }

    public Pipe get(final int index) {
        return this.pipes.get(index);
    }

    public boolean equals(final Object object) {
        return (object instanceof Pipeline) && areEqual(this, (Pipeline) object);
    }

    public static boolean areEqual(final Iterator it1, final Iterator it2) {
        if (it1.hasNext() != it2.hasNext())
            return false;

        while (it1.hasNext()) {
            if (!it2.hasNext())
                return false;
            if (it1.next() != it2.next())
                return false;
        }
        return true;
    }


    public long count() {
       return Iterators.count((Iterator<E>) this);
    }

    public void iterate() {
        try {
            while (true) {
                next();
            }
        } catch (final NoSuchElementException e) {
        }
    }

    public List<E> next(final int number) {
        final List<E> list = new ArrayList<E>(number);
        try {
            for (int i = 0; i < number; i++) {
                list.add(next());
            }
        } catch (final NoSuchElementException e) {
        }
        return list;
    }

    public List<E> toList() {
        return Iterators.addToCollection((Iterator<E>) this,new ArrayList<E>());
    }

    public Collection<E> fill(final Collection<E> collection) {
        return Iterators.addToCollection((Iterator<E>) this,collection);
    }
}