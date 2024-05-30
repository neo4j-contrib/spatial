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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;
import org.neo4j.internal.helpers.collection.Iterators;

/**
 * A Pipeline is a linear composite of Pipes.
 * Pipeline takes a List of Pipes and joins them according to their order as specified by their location in the List.
 * It is important to ensure that the provided ordered Pipes can connect together.
 * That is, that the output type of the n-1 Pipe is the same as the input type of the n Pipe.
 * Once all provided Pipes are composed, a Pipeline can be treated like any other Pipe.
 *
 * @author <a href="http://markorodriguez.com" >Marko A. Rodriguez</a>
 */
public class Pipeline<S, E> implements Pipe<S, E> {

	protected Pipe<S, ?> startPipe;
	protected Pipe<?, E> endPipe;
	protected final List<Pipe<?, ?>> pipes;
	protected Iterator<S> starts;

	public Pipeline() {
		this.pipes = new ArrayList<>();
	}

	/**
	 * Constructs a pipeline from the provided pipes. The ordered list determines how the pipes will be chained
	 * together.
	 * When the pipes are chained together, the start of pipe n is the end of pipe n-1.
	 *
	 * @param pipes the ordered list of pipes to chain together into a pipeline
	 */
	public Pipeline(final List<Pipe<?, ?>> pipes) {
		this.pipes = pipes;
		this.setPipes(pipes);
	}


	/**
	 * Constructs a pipeline from the provided pipes. The ordered array determines how the pipes will be chained
	 * together.
	 * When the pipes are chained together, the start of pipe n is the end of pipe n-1.
	 *
	 * @param pipes the ordered array of pipes to chain together into a pipeline
	 */
	public Pipeline(final Pipe<?, ?>... pipes) {
		this(new ArrayList<>(Arrays.asList(pipes)));
	}

	/**
	 * Useful for constructing the pipeline chain without making use of the constructor.
	 *
	 * @param pipes the ordered list of pipes to chain together into a pipeline
	 */
	protected void setPipes(final List<Pipe<?, ?>> pipes) {
		//noinspection unchecked
		this.startPipe = (Pipe<S, ?>) pipes.get(0);
		//noinspection unchecked
		this.endPipe = (Pipe<?, E>) pipes.get(pipes.size() - 1);
		for (int i = 1; i < pipes.size(); i++) {
			Pipe<?, ?> pipe = pipes.get(i - 1);
			//noinspection rawtypes,unchecked
			pipes.get(i).setStarts((Iterator) pipe);
		}
	}

	/**
	 * Useful for constructing the pipeline chain without making use of the constructor.
	 *
	 * @param pipes the ordered array of pipes to chain together into a pipeline
	 */
	protected void setPipes(final Pipe<?, ?>... pipes) {
		this.setPipes(Arrays.asList(pipes));
	}

	/**
	 * Adds a new pipe to the end of the pipeline and then reconstructs the pipeline chain.
	 *
	 * @param pipe the new pipe to add to the pipeline
	 */
	public void addPipe(final Pipe<?, ?> pipe) {
		this.pipes.add(pipe);
		this.setPipes(this.pipes);
	}

	@Override
	public void setStarts(final Iterator<S> starts) {
		this.starts = starts;
		this.startPipe.setStarts(starts);
	}

	@Override
	public void setStarts(final Iterable<S> starts) {
		this.setStarts(starts.iterator());
	}

	/**
	 * An unsupported operation that throws an UnsupportedOperationException.
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Determines if there is another object that can be emitted from the pipeline.
	 *
	 * @return true if an object can be next()'d out of the pipeline
	 */
	@Override
	public boolean hasNext() {
		return this.endPipe.hasNext();
	}

	/**
	 * Get the next object emitted from the pipeline.
	 * If no such object exists, then a NoSuchElementException is thrown.
	 *
	 * @return the next emitted object
	 */
	@Override
	public E next() {
		return this.endPipe.next();
	}

	@Override
	public List<E> getPath() {
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

	@Override
	public void reset() {
		this.startPipe.reset(); // Clear incoming state to avoid bug in Neo4j 4.3 with leaked RelationshipTraversalCursor
		this.endPipe.reset();
	}

	/**
	 * Simply returns this as a pipeline (more specifically, pipe) implements Iterator.
	 *
	 * @return returns the iterator representation of this pipeline
	 */
	@Override
	@Nonnull
	public Iterator<E> iterator() {
		return this;
	}

	@Override
	public String toString() {
		return this.pipes.toString();
	}

	public List<Pipe<?, ?>> getPipes() {
		return this.pipes;
	}

	public Iterator<S> getStarts() {
		return this.starts;
	}

	public Pipe<?, ?> remove(final int index) {
		return this.pipes.remove(index);
	}

	public Pipe<?, ?> get(final int index) {
		return this.pipes.get(index);
	}

	@Override
	public boolean equals(final Object object) {
		return (object instanceof Pipeline) && areEqual(this, (Pipeline<?, ?>) object);
	}

	public static boolean areEqual(final Iterator<?> it1, final Iterator<?> it2) {
		if (it1.hasNext() != it2.hasNext()) {
			return false;
		}

		while (it1.hasNext()) {
			if (!it2.hasNext()) {
				return false;
			}
			if (it1.next() != it2.next()) {
				return false;
			}
		}
		return true;
	}


	public long count() {
		return Iterators.count(this);
	}

	public void iterate() {
		while (true) {
			try {
				next();
			} catch (final NoSuchElementException e) {
				break;
			}
		}
	}

	public List<E> next(final int number) {
		final List<E> list = new ArrayList<>(number);
		for (int i = 0; i < number; i++) {
			try {
				list.add(next());
			} catch (final NoSuchElementException e) {
				break;
			}
		}
		return list;
	}

	public List<E> toList() {
		return Iterators.addToCollection(this, new ArrayList<>());
	}

	public Collection<E> fill(final Collection<E> collection) {
		return Iterators.addToCollection(this, collection);
	}
}
