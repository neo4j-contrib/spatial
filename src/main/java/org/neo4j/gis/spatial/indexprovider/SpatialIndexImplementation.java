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

import java.io.File;
import java.io.IOException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.index.IndexImplementation;

import java.util.Map;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.IndexCommandFactory;
import org.neo4j.graphdb.index.LegacyIndexProviderTransaction;
import org.neo4j.kernel.api.LegacyIndex;
import org.neo4j.kernel.impl.transaction.command.NeoCommandHandler;
import org.neo4j.kernel.impl.util.ResourceIterators;

public class SpatialIndexImplementation implements IndexImplementation {

    private GraphDatabaseService db;

    public SpatialIndexImplementation(GraphDatabaseService db) {

        this.db = db;
    }

    @Override
    public Map<String, String> fillInDefaults(Map<String, String> config) {
        return config;
    }

    @Override
    public boolean configMatches(Map<String, String> storedConfig, Map<String, String> config ) {
        return storedConfig.equals(config);
    }

    @Override
    public LegacyIndexProviderTransaction newTransaction(IndexCommandFactory icf) {

	    return new LegacyIndexProviderTransaction() {

		@Override
		public LegacyIndex nodeIndex(String indexName, Map<String, String> configuration) {
			return new LegacyIndexNodeWrapper(new LayerNodeIndex(indexName, db, configuration));
		}

		@Override
		public LegacyIndex relationshipIndex(String indexName, Map<String, String> configuration) {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public void close() {

		}
	};
    }

    @Override
    public NeoCommandHandler newApplier(boolean bln) {
        return NeoCommandHandler.EMPTY;
    }

    @Override
    public void force() {
	    // this is a graph based index
    }

    @Override
    public ResourceIterator<File> listStoreFiles() throws IOException {
	    // this is a graph based index
	    return ResourceIterators.EMPTY_ITERATOR;
    }
}
