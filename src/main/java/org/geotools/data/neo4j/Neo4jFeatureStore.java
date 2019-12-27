package org.geotools.data.neo4j;

import org.geotools.data.*;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.filter.FidFilterImpl;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Envelope;
import org.neo4j.gis.spatial.*;
import org.neo4j.gis.spatial.filter.SearchCQL;
import org.neo4j.gis.spatial.rtree.filter.SearchAll;
import org.neo4j.graphdb.GraphDatabaseService;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jFeatureStore extends ContentFeatureStore {

    /** Manages listener lists for SimpleFeatureSource implementation */
    public FeatureListenerManager listenerManager = new FeatureListenerManager();
    protected LockingManager lockingManager = new InProcessLockingManager();
    private Map<String,ReferencedEnvelope> boundsIndex = Collections.synchronizedMap(new HashMap<>());
    private Map<String, CoordinateReferenceSystem> crsIndex = Collections.synchronizedMap(new HashMap<>());
    private Map<String,Integer> countIndex = Collections.synchronizedMap(new HashMap<>());

    private GraphDatabaseService database;
    private SpatialDatabaseService spatialDatabase;
    Neo4jFeatureSource delegate;

    public Neo4jFeatureStore(GraphDatabaseService database, ContentEntry entry, Query query) {
        super(entry, query);
        this.delegate = new Neo4jFeatureSource(database, entry, query) {
            public void setTransaction(Transaction transaction) {
                super.setTransaction(transaction);
                Neo4jFeatureStore.this.setTransaction(transaction);
            }
        };
        this.database = database;
        this.spatialDatabase = new SpatialDatabaseService(database);
    }

    @Override
    protected FeatureWriter<SimpleFeatureType, SimpleFeature> getWriterInternal(Query query, int i) throws IOException {
        Filter filter = query.getFilter();
        if (filter == null) {
            throw new NullPointerException("getFeatureReader requires Filter: did you mean Filter.INCLUDE?");
        }

        if (transaction == null) {
            throw new NullPointerException("getFeatureWriter requires Transaction: did you mean to use Transaction.AUTO_COMMIT?");
        }

        String typeName = query.getTypeName();

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
            throw new UnsupportedOperationException("not implemented...");
        }

        if (lockingManager != null) {
            writer = ((InProcessLockingManager) lockingManager).checkedWriter(writer, transaction);
        }

        if (filter != Filter.INCLUDE) {
            writer = new FilteringFeatureWriter(writer, filter);
        }

        return writer;
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        String typeName = query.getTypeName();
        ReferencedEnvelope result = boundsIndex.get(typeName);
        if (result == null) {
            Layer layer = spatialDatabase.getLayer(typeName);
            if (layer != null) {
                try (org.neo4j.graphdb.Transaction tx = database.beginTx()) {
                    Envelope bbox = Utilities.fromNeo4jToJts(layer.getIndex().getBoundingBox());
                    result = convertEnvelopeToRefEnvelope(typeName, bbox);
                    boundsIndex.put(typeName, result);
                    tx.success();
                }
            }
        }
        return result;
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        String typeName = query.getTypeName();
        Integer result = countIndex.get(typeName);
        if (result == null) {
            Layer layer = spatialDatabase.getLayer(typeName);
            if (layer != null) {
                try (org.neo4j.graphdb.Transaction tx = database.beginTx()) {
                    result = layer.getIndex().count();
                    countIndex.put(typeName, result);
                    tx.success();
                }
            }
        }
        return result == null ? 0 : result;
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
        Layer layer = spatialDatabase.getLayer(query.getTypeName());

        try (org.neo4j.graphdb.Transaction tx = database.beginTx()) {
            Iterator<SpatialDatabaseRecord> records;
            if (query.getFilter().equals(Filter.EXCLUDE)) {
                // filter that excludes everything: create an empty FeatureReader
                records = null;
            } else if (query.getFilter() instanceof FidFilterImpl) {
                // filter by Feature unique id
                throw new UnsupportedOperationException("Unsupported use of FidFilterImpl in Neo4jSpatialDataStore");
            } else {
                records = layer.getIndex().search(new SearchCQL(layer, query.getFilter()));
            }

            tx.success();
            return new Neo4jSpatialFeatureReader(layer, getSchema(), records);
        }
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        return this.getState().getEntry().schema;
    }

    @Override
    public Neo4jState getState() {
        return (Neo4jState)super.getState();
    }

    public final FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(String typeName, Transaction tx) throws IOException {
        return this.getFeatureWriter(typeName, Filter.INCLUDE, tx);
    }

    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(String typeName, Filter filter, Transaction tx) throws IOException {
        return this.getWriter(filter, 3);
    }

    /**
     * Try to create an optimized FeatureWriter for the given Filter.
     */
    protected FeatureWriter<SimpleFeatureType, SimpleFeature> createFeatureWriter(String typeName, Filter filter, org.geotools.data.Transaction transaction) throws IOException {
        FeatureReader<SimpleFeatureType, SimpleFeature> reader = getFeatureReader(typeName, filter);
        if (reader == null) {
            reader = getFeatureReader(typeName, new SearchAll());
        }

        return new Neo4jSpatialFeatureWriter(listenerManager, transaction, getEditableLayer(typeName), reader);
    }

    protected FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(String typeName, Filter filter) throws IOException {
        Layer layer = spatialDatabase.getLayer(typeName);

        try (org.neo4j.graphdb.Transaction tx = database.beginTx()) {
            Iterator<SpatialDatabaseRecord> records;
            if (filter.equals(Filter.EXCLUDE)) {
                // filter that excludes everything: create an empty FeatureReader
                records = null;
            } else if (filter instanceof FidFilterImpl) {
                // filter by Feature unique id
                throw new UnsupportedOperationException("Unsupported use of FidFilterImpl in Neo4jSpatialDataStore");
            } else {
                records = layer.getIndex().search(new SearchCQL(layer, filter));
            }

            tx.success();
            return new Neo4jSpatialFeatureReader(layer, getSchema(), records);
        }
    }

    private EditableLayer getEditableLayer(String typeName) throws IOException {
        Layer layer = spatialDatabase.getLayer(typeName);
        if (layer == null) {
            throw new IOException("Layer not found: " + typeName);
        }

        if (!(layer instanceof EditableLayer)) {
            throw new IOException("Cannot create a FeatureWriter on a read-only layer: " + layer);
        }

        return (EditableLayer) layer;
    }

    private ReferencedEnvelope convertEnvelopeToRefEnvelope(String typeName, Envelope bbox) {
        return new ReferencedEnvelope(bbox, getCRS(typeName));
    }

    private CoordinateReferenceSystem getCRS(String typeName) {
        CoordinateReferenceSystem result = crsIndex.get(typeName);
        if (result == null) {
            try (org.neo4j.graphdb.Transaction tx = database.beginTx()) {
                Layer layer = spatialDatabase.getLayer(typeName);
                result = layer.getCoordinateReferenceSystem();
                crsIndex.put(typeName, result);
                tx.success();
            }
        }

        return result;
    }

}
