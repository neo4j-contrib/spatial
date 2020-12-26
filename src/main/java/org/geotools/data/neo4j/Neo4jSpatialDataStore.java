/*
 * Copyright (c) 2010-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import com.vividsolutions.jts.geom.Envelope;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.filter.FidFilterImpl;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.styling.SLDParser;
import org.geotools.styling.Style;
import org.geotools.styling.StyleFactory;
import org.geotools.styling.StyleFactoryImpl;
import org.neo4j.gis.spatial.*;
import org.neo4j.gis.spatial.filter.SearchCQL;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.rtree.filter.SearchAll;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * Geotools DataStore implementation.
 */
public class Neo4jSpatialDataStore extends AbstractDataStore implements Constants {
    private String[] typeNames;
    private final Map<String, SimpleFeatureType> simpleFeatureTypeIndex = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, CoordinateReferenceSystem> crsIndex = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Style> styleIndex = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, ReferencedEnvelope> boundsIndex = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, SimpleFeatureSource> featureSourceIndex = Collections.synchronizedMap(new HashMap<>());
    private final GraphDatabaseService database;
    private final SpatialDatabaseService spatialDatabase;

    public Neo4jSpatialDataStore(GraphDatabaseService database) {
        super(true);

        this.database = database;
        this.spatialDatabase = new SpatialDatabaseService();
    }

    /**
     * Return list of not-empty Layer names.
     * The list is cached in memory.
     *
     * @return layer names
     */
    @Override
    public String[] getTypeNames() {
        if (typeNames == null) {
            try (Transaction tx = database.beginTx()) {
                List<String> notEmptyTypes = new ArrayList<>();
                String[] allTypeNames = spatialDatabase.getLayerNames(tx);
                for (String allTypeName : allTypeNames) {
                    // discard empty layers
                    System.out.print("loading layer " + allTypeName);
                    Layer layer = spatialDatabase.getLayer(tx, allTypeName);
                    if (!layer.getIndex().isEmpty(tx)) {
                        notEmptyTypes.add(allTypeName);
                    }
                }
                typeNames = notEmptyTypes.toArray(new String[]{});
                tx.commit();
            }
        }
        return typeNames;
    }

    /**
     * Return FeatureType of the given Layer.
     * FeatureTypes are cached in memory.
     */
    @Override
    public SimpleFeatureType getSchema(String typeName) throws IOException {
        SimpleFeatureType result = simpleFeatureTypeIndex.get(typeName);
        if (result == null) {
            try (Transaction tx = database.beginTx()) {
                Layer layer = spatialDatabase.getLayer(tx, typeName);
                if (layer == null) {
                    throw new IOException("Layer not found: " + typeName);
                }

                result = Neo4jFeatureBuilder.getTypeFromLayer(tx, layer);
                simpleFeatureTypeIndex.put(typeName, result);
                tx.commit();
            }
        }

        return result;
    }

    /**
     * Return a FeatureSource implementation.
     * A FeatureSource can be used to retrieve Layer metadata, bounds and geometries.
     */
    @Override
    public SimpleFeatureSource getFeatureSource(String typeName) throws IOException {
        SimpleFeatureSource result = featureSourceIndex.get(typeName);
        if (result == null) {
            final SimpleFeatureType featureType = getSchema(typeName);

            if (getLockingManager() != null) {
                System.out.println("getFeatureSource(" + typeName + ") - locking manager is present");

                result = new AbstractFeatureLocking(getSupportedHints()) {
                    public DataStore getDataStore() {
                        return Neo4jSpatialDataStore.this;
                    }

                    public void addFeatureListener(FeatureListener listener) {
                        listenerManager.addFeatureListener(this, listener);
                    }

                    public void removeFeatureListener(FeatureListener listener) {
                        listenerManager.removeFeatureListener(this, listener);
                    }

                    public SimpleFeatureType getSchema() {
                        return featureType;
                    }

                    public ReferencedEnvelope getBounds() {
                        return Neo4jSpatialDataStore.this.getBounds(featureType.getTypeName());
                    }

                    public ResourceInfo getInfo() {
                        return Neo4jSpatialDataStore.this.getInfo(featureType.getTypeName());
                    }
                };
            } else {
                System.out.println("getFeatureSource(" + typeName + ") - locking manager is NOT present");

                result = new AbstractFeatureStore(getSupportedHints()) {
                    public DataStore getDataStore() {
                        return Neo4jSpatialDataStore.this;
                    }

                    public void addFeatureListener(FeatureListener listener) {
                        listenerManager.addFeatureListener(this, listener);
                    }

                    public void removeFeatureListener(FeatureListener listener) {
                        listenerManager.removeFeatureListener(this, listener);
                    }

                    public SimpleFeatureType getSchema() {
                        return featureType;
                    }

                    public ReferencedEnvelope getBounds() {
                        return Neo4jSpatialDataStore.this.getBounds(featureType.getTypeName());
                    }

                    public ResourceInfo getInfo() {
                        return Neo4jSpatialDataStore.this.getInfo(featureType.getTypeName());
                    }
                };
            }

            featureSourceIndex.put(typeName, result);
        }

        return result;
    }

    /* public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriterAppend(String typeName, org.geotools.data.Transaction transaction) throws IOException {
		FeatureWriter<SimpleFeatureType, SimpleFeature> writer = getFeatureWriter(typeName, Filter.EXCLUDE, transaction);
		while (writer.hasNext()) {
			writer.next();
		}
		return writer;
    } */

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(String typeName, Filter filter, org.geotools.data.Transaction transaction) throws IOException {
        if (filter == null) {
            throw new NullPointerException("getFeatureReader requires Filter: did you mean Filter.INCLUDE?");
        }

        if (transaction == null) {
            throw new NullPointerException("getFeatureWriter requires Transaction: did you mean to use Transaction.AUTO_COMMIT?");
        }

        System.out.println("getFeatureWriter(" + typeName + "," + filter.getClass() + " " + filter + "," + transaction + ")");

        FeatureWriter<SimpleFeatureType, SimpleFeature> writer;

        if (transaction == org.geotools.data.Transaction.AUTO_COMMIT) {
            try {
                writer = createFeatureWriter(typeName, filter, transaction);
            } catch (UnsupportedOperationException e) {
                // this is for backward compatibility
                try {
                    writer = getFeatureWriter(typeName, transaction);
                } catch (UnsupportedOperationException eek) {
                    throw e; // throw original - our fallback did not work
                }
            }
        } else {
            TransactionStateDiff state = state(transaction);
            if (state != null) {
                writer = state.writer(typeName, filter);
            } else {
                throw new UnsupportedOperationException("not implemented...");
            }
        }

        if (getLockingManager() != null) {
            writer = ((InProcessLockingManager) getLockingManager()).checkedWriter(writer, transaction);
        }

        if (filter != Filter.INCLUDE) {
            writer = new FilteringFeatureWriter(writer, filter);
        }

        return writer;
    }

    public ReferencedEnvelope getBounds(String typeName) {
        ReferencedEnvelope result = boundsIndex.get(typeName);
        if (result == null) {
            try (Transaction tx = database.beginTx()) {
                Layer layer = spatialDatabase.getLayer(tx, typeName);
                if (layer != null) {
                    Envelope bbox = Utilities.fromNeo4jToJts(layer.getIndex().getBoundingBox(tx));
                    result = convertEnvelopeToRefEnvelope(typeName, bbox);
                    boundsIndex.put(typeName, result);
                }
                tx.commit();
            }
        }
        return result;
    }

    public SpatialDatabaseService getSpatialDatabaseService() {
        return spatialDatabase;
    }

    public Transaction beginTx() {
        return database.beginTx();
    }

    public void clearCache() {
        typeNames = null;
        simpleFeatureTypeIndex.clear();
        crsIndex.clear();
        styleIndex.clear();
        boundsIndex.clear();
        featureSourceIndex.clear();
    }

    public void dispose() {
        super.dispose();
    }


    // Private methods

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(String typeName, Query query) throws IOException {
        System.out.println("getFeatureReader(" + typeName + "," + query.getFilter().getClass() + " " + query.getFilter() + ")");

        FeatureReader<SimpleFeatureType, SimpleFeature> reader = null;
        if (query.getTypeName() != null) {
            // use Filter to create optimized FeatureReader
            Filter filter = query.getFilter();
            reader = getFeatureReader(typeName, filter);
        }

        // default
        if (reader == null) {
            reader = super.getFeatureReader(typeName, query);
        }

        return reader;
    }

    /**
     * Create an optimized FeatureReader for most of the uDig operations.
     */
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(String typeName, Filter filter) throws IOException {
        Layer layer;
        Iterator<SpatialDatabaseRecord> records;
        String[] extraPropertyNames;
        try (Transaction tx = database.beginTx()) {
            layer = spatialDatabase.getLayer(tx, typeName);
            if (filter.equals(Filter.EXCLUDE)) {
                // filter that excludes everything: create an empty FeatureReader
                records = null;
                extraPropertyNames = new String[0];
            } else if (filter instanceof FidFilterImpl) {
                // filter by Feature unique id
                throw new UnsupportedOperationException("Unsupported use of FidFilterImpl in Neo4jSpatialDataStore");
            } else {
                records = layer.getIndex().search(tx, new SearchCQL(tx, layer, filter));
                extraPropertyNames = layer.getExtraPropertyNames(tx);
            }

            tx.commit();
        }
        return new Neo4jSpatialFeatureReader(database, layer, getSchema(typeName), records, extraPropertyNames);
    }

    protected ResourceInfo getInfo(String typeName) {
        return new DefaultResourceInfo(typeName, getCRS(typeName), getBounds(typeName));
    }

    /**
     * Create a FeatureReader that returns all Feature in the given Layer
     */
    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(String typeName) throws IOException {
        System.out.println("getFeatureReader(" + typeName + ") SLOW QUERY :(");
        return getFeatureReader(typeName, new SearchAll());
    }

    private FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(String typeName, SearchFilter search) throws IOException {
        Layer layer;
        SearchRecords results;
        String[] extraPropertyNames;
        try (Transaction tx = database.beginTx()) {
            layer = spatialDatabase.getLayer(tx, typeName);
            results = layer.getIndex().search(tx, search);
            extraPropertyNames = layer.getExtraPropertyNames(tx);
            tx.commit();
        }
        return new Neo4jSpatialFeatureReader(database, layer, getSchema(typeName), results, extraPropertyNames);
    }

    private ReferencedEnvelope convertEnvelopeToRefEnvelope(String typeName, Envelope bbox) {
        return new ReferencedEnvelope(bbox, getCRS(typeName));
    }

    private CoordinateReferenceSystem getCRS(String typeName) {
        CoordinateReferenceSystem result = crsIndex.get(typeName);
        if (result == null) {
            try (Transaction tx = database.beginTx()) {
                Layer layer = spatialDatabase.getLayer(tx, typeName);
                result = layer.getCoordinateReferenceSystem(tx);
                crsIndex.put(typeName, result);
                tx.commit();
            }
        }

        return result;
    }

    private Object getLayerStyle(String typeName) {
        try (Transaction tx = database.beginTx()) {
            Layer layer = spatialDatabase.getLayer(tx, typeName);
            tx.commit();
            if (layer == null) return null;
            else return layer.getStyle();
        }
    }

    public Style getStyle(String typeName) {
        Style result = styleIndex.get(typeName);
        if (result == null) {
            Object obj = getLayerStyle(typeName);
            if (obj instanceof Style) {
                result = (Style) obj;
            } else if (obj instanceof File || obj instanceof String) {
                StyleFactory styleFactory = new StyleFactoryImpl();
                SLDParser parser = new SLDParser(styleFactory);
                try {
                    if (obj instanceof File) {
                        parser.setInput(new FileReader((File) obj));
                    } else {
                        parser.setInput(new StringReader(obj.toString()));
                    }
                    Style[] styles = parser.readXML();
                    result = styles[0];
                } catch (Exception e) {
                    System.err.println("Error loading style '" + obj + "': " + e.getMessage());
                    e.printStackTrace(System.err);
                }
            }
            styleIndex.put(typeName, result);
        }
        return result;
    }

    private Integer getGeometryType(String typeName) {
        try (Transaction tx = database.beginTx()) {
            Layer layer = spatialDatabase.getLayer(tx, typeName);
            tx.commit();
            return layer.getGeometryType(tx);
        }
    }

    private EditableLayer getEditableLayer(String typeName) throws IOException {
        try (Transaction tx = database.beginTx()) {
            Layer layer = spatialDatabase.getLayer(tx, typeName);
            if (layer == null) {
                throw new IOException("Layer not found: " + typeName);
            }

            if (!(layer instanceof EditableLayer)) {
                throw new IOException("Cannot create a FeatureWriter on a read-only layer: " + layer);
            }
            tx.commit();

            return (EditableLayer) layer;
        }
    }

    /**
     * Try to create an optimized FeatureWriter for the given Filter.
     */
    protected FeatureWriter<SimpleFeatureType, SimpleFeature> createFeatureWriter(String typeName, Filter filter, org.geotools.data.Transaction transaction) throws IOException {
        FeatureReader<SimpleFeatureType, SimpleFeature> reader = getFeatureReader(typeName, filter);
        if (reader == null) {
            reader = getFeatureReader(typeName, new SearchAll());
        }

        return new Neo4jSpatialFeatureWriter(database, listenerManager, transaction, getEditableLayer(typeName), reader);
    }

    @Override
    protected FeatureWriter<SimpleFeatureType, SimpleFeature> createFeatureWriter(String typeName, org.geotools.data.Transaction transaction) throws IOException {
        return new Neo4jSpatialFeatureWriter(database, listenerManager, transaction, getEditableLayer(typeName), getFeatureReader(typeName, new SearchAll()));
    }
}