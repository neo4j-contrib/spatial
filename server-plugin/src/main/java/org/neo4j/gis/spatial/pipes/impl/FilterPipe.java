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

import java.util.Objects;

/**
 * A FilterPipe has no specified behavior save that it takes the same objects it emits.
 * This interface is used to specify that a Pipe will either emit its input or not.
 *
 * @author <a href="http://markorodriguez.com" >Marko A. Rodriguez</a>
 */
public interface FilterPipe<S> extends Pipe<S, S> {

	enum Filter {
		EQUAL, NOT_EQUAL, GREATER_THAN, LESS_THAN, GREATER_THAN_EQUAL, LESS_THAN_EQUAL;

		@SuppressWarnings("unchecked")
		public boolean compare(Object leftObject, Object rightObject) {
			return switch (this) {
				case EQUAL -> Objects.equals(leftObject, rightObject);
				case NOT_EQUAL -> !Objects.equals(leftObject, rightObject);
				case GREATER_THAN -> leftObject instanceof Comparable<?>
						&& rightObject != null
						&& ((Comparable<Object>) leftObject).compareTo(rightObject) > 0;
				case LESS_THAN -> leftObject instanceof Comparable<?>
						&& rightObject != null
						&& ((Comparable<Object>) leftObject).compareTo(rightObject) < 0;
				case GREATER_THAN_EQUAL -> leftObject instanceof Comparable<?>
						&& rightObject != null
						&& ((Comparable<Object>) leftObject).compareTo(rightObject) >= 0;
				case LESS_THAN_EQUAL -> leftObject instanceof Comparable<?>
						&& rightObject != null
						&& ((Comparable<Object>) leftObject).compareTo(rightObject) <= 0;
			};
		}
	}
}
