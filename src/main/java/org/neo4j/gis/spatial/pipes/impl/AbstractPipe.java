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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

/**
 * An AbstractPipe provides most of the functionality that is repeated in every instance of a Pipe.
 * Any subclass of AbstractPipe should simply implement processNextStart(). The standard model is
 * <pre>
 * protected E processNextStart() throws NoSuchElementException {
 *   S s = this.starts.next();
 *   E e = // do something with the S to yield an E
 *   return e;
 * }
 * </pre>
 * If the current incoming S is not to be emitted and there are no other S objects to process and emit, then throw a
 * NoSuchElementException.
 *
 * @author <a href="http://markorodriguez.com" >Marko A. Rodriguez</a>
 */
public abstract class AbstractPipe<S, E> implements Pipe<S, E> {

	protected Iterator<S> starts;
	private E nextEnd;
	protected E currentEnd;
	private boolean available = false;

	public void setStarts(final Pipe<?, S> starts) {
		this.starts = starts;
	}

	@Override
	public void setStarts(final Iterator<S> starts) {
		if (starts instanceof Pipe) {
			this.starts = starts;
		} else {
			this.starts = new LastElementIterator<>(starts);
		}
	}

	@Override
	public void setStarts(final Iterable<S> starts) {
		this.setStarts(starts.iterator());
	}

	@Override
	public void reset() {
		if (this.starts instanceof Pipe) {
			((Pipe<?, ?>) this.starts).reset();
		}
		if (this.starts instanceof LastElementIterator) {
			((LastElementIterator<?>) this.starts).reset();
		}
		this.nextEnd = null;
		this.currentEnd = null;
		this.available = false;
	}

	@Override
	public List<E> getPath() {
		final List<E> pathElements = getPathToHere();
		final int size = pathElements.size();
		// do not repeat filters as they dup the object
		if (size == 0 || pathElements.get(size - 1) != this.currentEnd) {
			pathElements.add(this.currentEnd);
		}
		return pathElements;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public E next() {
		if (this.available) {
			this.available = false;
			return (this.currentEnd = this.nextEnd);
		}
		return (this.currentEnd = this.processNextStart());
	}

	@Override
	public boolean hasNext() {
		if (this.available) {
			return true;
		}
		try {
			this.nextEnd = this.processNextStart();
			return (this.available = true);
		} catch (final NoSuchElementException e) {
			return (this.available = false);
		}
	}

	/**
	 * The iterator method of Iterable is not faithful to the Java semantics of iterator().
	 * This method simply returns the pipe itself (which is an iterator) and thus, is useful only for foreach iteration.
	 *
	 * @return the pipe from the perspective of an iterator
	 */
	@Override
	@Nonnull
	public Iterator<E> iterator() {
		return this;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}

	protected abstract E processNextStart() throws NoSuchElementException;

	@SuppressWarnings("unchecked")
	protected List<E> getPathToHere() {
		if (this.starts instanceof Pipe) {
			return ((Pipe<?, E>) this.starts).getPath();
		}
		if (this.starts instanceof LastElementIterator<?> iter) {
			final List<E> list = new ArrayList<>();
			list.add((E) iter.lastElement());
			return list;
		}
		return new ArrayList<>();
	}


}

