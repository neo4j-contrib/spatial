package org.geotools.data.neo4j;

import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentState;

public class Neo4jState extends ContentState {
    public Neo4jState(ContentEntry entry) {
        super(entry);
    }

    public Neo4jState(Neo4jState state) {
        this(state.getEntry());
    }

    public Neo4jEntry getEntry() {
        return (Neo4jEntry)super.getEntry();
    }

    public Neo4jState copy() {
        return new Neo4jState(this);
    }
}
