package org.neo4j.gis.spatial.pipes;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.gis.spatial.SpatialDatabaseRecord;

public class GeoPipeHelper
{
    /**
     * Count the number of objects in a geometry.
     * This will exhaust the iterator.
     * Note that the try/catch model is not "acceptable Java," but is more efficient given the architecture of AbstractPipe.
     *
     * @param iterator the iterator to count
     * @return the number of objects in the iterator
     */
    public static long counter(final Iterator<SpatialDatabaseRecord> iterator) {
        long counter = 0;
        try {
            while (true) {
                SpatialDatabaseRecord next = iterator.next();
                counter+=next.getGeometry().getNumPoints();
            }
        } catch (final NoSuchElementException e) {
        }
        return counter;
    }
}
