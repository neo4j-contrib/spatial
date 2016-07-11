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

import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.index.IndexImplementation;

import java.util.Map;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.IndexCommandFactory;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.LegacyIndexProviderTransaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.LegacyIndex;
import org.neo4j.kernel.impl.transaction.command.CommandHandler;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class SpatialIndexImplementation implements IndexImplementation {

    private GraphDatabaseService db;

    public SpatialIndexImplementation(GraphDatabaseService db) {

        this.db = db;
    }

    @Override
	public Map<String, String> fillInDefaults(Map<String, String> config) {
		if (config.containsKey(LayerNodeIndex.WKT_PROPERTY_KEY)) {
			return makeSinglePropertyConfig(config, LayerNodeIndex.WKT_PROPERTY_KEY, Constants.PROP_WKT);
		} else if (config.containsKey(LayerNodeIndex.WKB_PROPERTY_KEY)) {
			return makeSinglePropertyConfig(config, LayerNodeIndex.WKT_PROPERTY_KEY, Constants.PROP_WKT);
		} else if (config.containsKey(SpatialIndexProvider.GEOMETRY_TYPE)
				&& LayerNodeIndex.POINT_GEOMETRY_TYPE.equals(config.get(SpatialIndexProvider.GEOMETRY_TYPE))) {
			return stringMap(
					IndexManager.PROVIDER, config.get(IndexManager.PROVIDER),
					SpatialIndexProvider.GEOMETRY_TYPE, LayerNodeIndex.POINT_GEOMETRY_TYPE,
					LayerNodeIndex.LON_PROPERTY_KEY, config.containsKey(LayerNodeIndex.LON_PROPERTY_KEY) ?  config.get(LayerNodeIndex.LON_PROPERTY_KEY) : SimplePointEncoder.DEFAULT_X,
					LayerNodeIndex.LAT_PROPERTY_KEY, config.containsKey(LayerNodeIndex.LAT_PROPERTY_KEY) ?  config.get(LayerNodeIndex.LAT_PROPERTY_KEY) : SimplePointEncoder.DEFAULT_Y
			);
		} else {
			throw new IllegalArgumentException("Invalid spatial index config: " + config);
		}
	}

	private Map<String, String> makeSinglePropertyConfig(Map<String, String> config, String key, String defaultValue) {
		return stringMap(IndexManager.PROVIDER, config.get(IndexManager.PROVIDER), key, config.containsKey(key) ? config.get(key) : defaultValue);
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
    public CommandHandler newApplier( boolean bln ) {
        return CommandHandler.EMPTY;
    }

    @Override
    public void force() {
	    // this is a graph based index
    }

    @Override
    public ResourceIterator<File> listStoreFiles() throws IOException {
	    // this is a graph based index
	    return IteratorUtil.emptyIterator();
    }

	@Override
	public void init() throws Throwable {
	}

	@Override
	public void start() throws Throwable {
	}

	@Override
	public void stop() throws Throwable {
	}

	@Override
	public void shutdown() throws Throwable {
	}

}
