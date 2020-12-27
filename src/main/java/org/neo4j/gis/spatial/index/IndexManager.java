package org.neo4j.gis.spatial.index;

import org.neo4j.io.layout.DatabaseLayout;

import java.nio.file.Path;
import java.nio.file.Paths;

public class IndexManager {
    private final DatabaseLayout databaseLayout;

    public IndexManager(DatabaseLayout databaseLayout) {
        this.databaseLayout = databaseLayout;
    }

    public Path makePathFor(String indexName) {
        return Paths.get(databaseLayout.databaseDirectory().getAbsolutePath(), "spatial", indexName);
    }
}
