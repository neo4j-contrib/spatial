/*
 * Copyright (c) 2010-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Spatial.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.geotools.data.neo4j;

import java.io.IOException;
import java.util.logging.Logger;

import org.geotools.data.FeatureListenerManager;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;

/**
 * FeatureWriter implementation. Instances of this class are created by
 * Neo4jSpatialDataStore.
 *
 * @author Davide Savazzi, Andreas Wilhelm
 */
public class Neo4jSpatialFeatureWriter implements
        FeatureWriter<SimpleFeatureType, SimpleFeature> {

    // current for FeatureWriter
    private SimpleFeature live;
    // copy of live returned to user
    private SimpleFeature current;

    private final GraphDatabaseService database;
    private final FeatureListenerManager listener;
    private final org.geotools.data.Transaction geoTransaction;
    private final SimpleFeatureType featureType;
    private final FeatureReader<SimpleFeatureType, SimpleFeature> reader;
    private final EditableLayer layer;
    private boolean closed;

    private static final Logger LOGGER = org.geotools.util.logging.Logging.getLogger("org.neo4j.gis.spatial");

    protected Neo4jSpatialFeatureWriter(GraphDatabaseService database, FeatureListenerManager listener,
                                        org.geotools.data.Transaction geoTransaction, EditableLayer layer,
                                        FeatureReader<SimpleFeatureType, SimpleFeature> reader) {
        this.database = database;
        this.geoTransaction = geoTransaction;
        this.listener = listener;
        this.reader = reader;
        this.layer = layer;
        this.featureType = reader.getFeatureType();
    }

    /**
     *
     */
    public SimpleFeatureType getFeatureType() {
        return featureType;
    }

    /**
     *
     */
    public boolean hasNext() throws IOException {
        if (closed) {
            throw new IOException("Feature writer is closed");
        }

        return reader != null && reader.hasNext();
    }

    /**
     *
     */
    public SimpleFeature next() throws IOException {
        if (closed) {
            throw new IOException("FeatureWriter has been closed");
        }

        SimpleFeatureType featureType = getFeatureType();

        if (hasNext()) {
            live = reader.next();
            current = SimpleFeatureBuilder.copy(live);
            LOGGER.finer("Calling next on writer");
        } else {
            // new content
            live = null;
            current = SimpleFeatureBuilder.template(featureType, null);
        }

        return current;
    }

    /**
     *
     */
    public void remove() throws IOException {
        if (closed) {
            throw new IOException("FeatureWriter has been closed");
        }

        if (current == null) {
            throw new IOException("No feature available to remove");
        }

        if (live != null) {
            LOGGER.fine("Removing " + live);

            try (Transaction tx = database.beginTx()) {
                layer.delete(tx, Long.parseLong(live.getID()));
                tx.commit();
            }

            listener.fireFeaturesRemoved(featureType.getTypeName(), geoTransaction, new ReferencedEnvelope(live.getBounds()), true);
        }

        live = null;
        current = null;
    }

    /**
     *
     */
    public void write() throws IOException {
        if (closed) {
            throw new IOException("FeatureWriter has been closed");
        }

        if (current == null) {
            throw new IOException("No feature available to write");
        }

        LOGGER.fine("Write called, live is " + live + " and cur is " + current);

        if (live != null) {
            if (!live.equals(current)) {
                LOGGER.fine("Updating " + current);
                try (Transaction tx = database.beginTx()) {
                    layer.update(tx, Long.parseLong(current.getID()), (Geometry) current.getDefaultGeometry());
                    tx.commit();
                }

                listener.fireFeaturesChanged(featureType.getTypeName(),
                        geoTransaction,
                        new ReferencedEnvelope(current.getBounds()), true);

            }
        } else {
            LOGGER.fine("Inserting " + current);
            try (Transaction tx = database.beginTx()) {
                layer.add(tx, (Geometry) current.getDefaultGeometry());
                tx.commit();
            }

            listener.fireFeaturesAdded(featureType.getTypeName(), geoTransaction, new ReferencedEnvelope(current.getBounds()), true);
        }

        live = null;
        current = null;
    }

    /**
     *
     */
    public void close() throws IOException {
        if (reader != null)
            reader.close();
        closed = true;
    }

}