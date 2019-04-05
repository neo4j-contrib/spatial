package org.geotools.data.neo4j;

import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Node;

class SearchNone implements SearchFilter {
    public static final SearchFilter EXCLUDE_SEARCH_FILTER = new SearchNone();

    @Override
    public boolean needsToVisit(Envelope envelope) {
        return false;
    }

    @Override
    public boolean geometryMatches(Node geomNode) {
        return false;
    }
}
