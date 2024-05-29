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
