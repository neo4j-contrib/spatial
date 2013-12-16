/**
 * Copyright (c) 2010-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.indexprovider;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;

import java.util.Map;

public class SpatialIndexImplementation implements IndexImplementation {

    private GraphDatabaseService db;

    public SpatialIndexImplementation(GraphDatabaseService db) {

        this.db = db;
    }

    @Override
    public String getDataSourceName() {
        return NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME;
    }

    @Override
    public Index<Node> nodeIndex(String indexName, Map<String, String> config) {
        return new LayerNodeIndex(indexName, db, config);
    }

    @Override
    public RelationshipIndex relationshipIndex(String indexName, Map<String, String> config) {
        throw new UnsupportedOperationException("Spatial relationship indexing is not supported at the moment. Please use the node index.");
    }

    @Override
    public Map<String, String> fillInDefaults(Map<String, String> config) {
        return config;
    }

    @Override
    public boolean configMatches(Map<String, String> storedConfig, Map<String, String> config ) {
        return storedConfig.equals(config);
    }
}
