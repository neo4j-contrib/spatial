package org.neo4j.gis.spatial.pipes.impl;

/**
 * A FilterPipe has no specified behavior save that it takes the same objects it emits.
 * This interface is used to specify that a Pipe will either emit its input or not.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface FilterPipe<S> extends Pipe<S, S> {
    enum Filter {
        EQUAL, NOT_EQUAL, GREATER_THAN, LESS_THAN, GREATER_THAN_EQUAL, LESS_THAN_EQUAL;

        public boolean compare(Object leftObject, Object rightObject) {
            switch (this) {
                case EQUAL:
                    if (null == leftObject)
                        return rightObject == null;
                    return leftObject.equals(rightObject);
                case NOT_EQUAL:
                    if (null == leftObject)
                        return rightObject != null;
                    return !leftObject.equals(rightObject);
                case GREATER_THAN:
                    if (null == leftObject || rightObject == null)
                        return false;
                    return ((Comparable) leftObject).compareTo(rightObject) == 1;
                case LESS_THAN:
                    if (null == leftObject || rightObject == null)
                        return false;
                    return ((Comparable) leftObject).compareTo(rightObject) == -1;
                case GREATER_THAN_EQUAL:
                    if (null == leftObject || rightObject == null)
                        return false;
                    return ((Comparable) leftObject).compareTo(rightObject) >= 0;
                case LESS_THAN_EQUAL:
                    if (null == leftObject || rightObject == null)
                        return false;
                    return ((Comparable) leftObject).compareTo(rightObject) <= 0;
                default:
                    throw new IllegalArgumentException("Invalid state as no valid filter was provided");
            }
        }
    }
}