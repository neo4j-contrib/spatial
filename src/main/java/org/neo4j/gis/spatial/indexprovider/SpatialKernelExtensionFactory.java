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
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.spi.legacyindex.IndexProviders;

public class SpatialKernelExtensionFactory extends KernelExtensionFactory<SpatialKernelExtensionFactory.Dependencies> {
    public SpatialKernelExtensionFactory() {
        super(SpatialIndexProvider.SERVICE_NAME);
    }

    public interface Dependencies {
        IndexProviders getIndexProviders();
        GraphDatabaseService getGraphDatabaseService();
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies ) {
        return new SpatialKernelExtension(dependencies.getIndexProviders(), dependencies.getGraphDatabaseService());
    }


    public static class SpatialKernelExtension implements Lifecycle {
        private final IndexProviders indexProviders;
        private final GraphDatabaseService graphDatabaseService;

        public SpatialKernelExtension(IndexProviders indexProviders, GraphDatabaseService graphDatabaseService) {
            this.indexProviders = indexProviders;
            this.graphDatabaseService = graphDatabaseService;
        }

        @Override
        public void init() throws Throwable {
        }

        @Override
        public void start() throws Throwable {
            indexProviders.registerIndexProvider(SpatialIndexProvider.SERVICE_NAME,new SpatialIndexImplementation(graphDatabaseService));
        }

        @Override
        public void stop() throws Throwable {
            indexProviders.unregisterIndexProvider(SpatialIndexProvider.SERVICE_NAME);
        }

        @Override
        public void shutdown() throws Throwable {
        }
    }
}
