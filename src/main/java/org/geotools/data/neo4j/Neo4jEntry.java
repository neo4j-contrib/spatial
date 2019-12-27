package org.geotools.data.neo4j;

import org.geotools.data.store.ContentEntry;
import org.opengis.feature.simple.SimpleFeatureType;

public class Neo4jEntry extends ContentEntry {

    final SimpleFeatureType schema;

    Neo4jEntry(Neo4jSpatialDataStore store, SimpleFeatureType schema) {
        super(store, schema.getName());
        this.schema = schema;
    }

    @Override
    public Neo4jSpatialDataStore getDataStore() {
        return (Neo4jSpatialDataStore) super.getDataStore();
    }

}
