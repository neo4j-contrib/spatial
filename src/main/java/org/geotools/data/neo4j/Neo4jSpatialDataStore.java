/**
 * Copyright (c) 2010-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is part of Neo4j Spatial.
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.geotools.data.neo4j;

import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.styling.Style;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.util.*;

/**
 * Geotools DataStore implementation.
 *
 * @author Davide Savazzi
 */
public class Neo4jSpatialDataStore extends ContentDataStore implements Constants {

    private SpatialDatabaseService spatialDatabase;

    public Neo4jSpatialDataStore(GraphDatabaseService database) {
        this.spatialDatabase = new SpatialDatabaseService(database);
    }

    @Override
    protected List<Name> createTypeNames() {
        List<Name> notEmptyTypes = new ArrayList<>();
        try (Transaction tx = spatialDatabase.getDatabase().beginTx()) {
            String[] allTypeNames = spatialDatabase.getLayerNames();
            for (int i = 0; i < allTypeNames.length; i++) {
                // discard empty layers
                LOGGER.info("loading layer " + allTypeNames[i]);
                Layer layer = spatialDatabase.getLayer(allTypeNames[i]);
                if (!layer.getIndex().isEmpty()) {
                    notEmptyTypes.add(name(allTypeNames[i]));
                }
            }
            tx.success();
        }
        return notEmptyTypes;
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry contentEntry) {
        return new Neo4jSpatialFeatureSource(spatialDatabase, contentEntry, Query.ALL);
    }

    @Override
    public void dispose() {
        spatialDatabase.getDatabase().shutdown();
        super.dispose();
    }

    public Transaction beginTx() {
        return spatialDatabase.getDatabase().beginTx();
    }
}
