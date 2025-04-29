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
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * The generic interface for any Pipe implementation.
 * A Pipe takes/consumes objects of type S and returns/emits objects of type E.
 * S refers to <i>starts</i> and the E refers to <i>ends</i>.
 *
 * @author <a href="http://markorodriguez.com" >Marko A. Rodriguez</a>
 * @author Darrick Wiebe (darrick@innatesoftware.com)
 */
public interface Pipe<S, E> extends Iterator<E>, Iterable<E> {

	/**
	 * Set an iterator of S objects to the head (start) of the pipe.
	 *
	 * @param starts the iterator of incoming objects
	 */
	void setStarts(Iterator<S> starts);

	/**
	 * Set an iterable of S objects to the head (start) of the pipe.
	 *
	 * @param starts the iterable of incoming objects
	 */
	void setStarts(Iterable<S> starts);

	/**
	 * Returns the transformation path to arrive at the current object of the pipe.
	 *
	 * @return a List of all the objects traversed for the current iterator position of the pipe.
	 */
	List<E> getPath();

	/**
	 * A pipe may maintain state. Reset is used to remove state.
	 * The general use case for reset() is to reuse a pipe in another computation without having to create a new Pipe
	 * object.
	 */
	void reset();

	default Stream<E> stream() {
		return StreamSupport.stream(spliterator(), false);
	}
}
