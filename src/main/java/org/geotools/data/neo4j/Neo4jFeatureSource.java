package org.geotools.data.neo4j;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.filter.FidFilterImpl;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Envelope;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.Utilities;
import org.neo4j.gis.spatial.filter.SearchCQL;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jFeatureSource extends ContentFeatureSource {
    private GraphDatabaseService database;
    private SpatialDatabaseService spatialDatabase;
    private Map<String,ReferencedEnvelope> boundsIndex = Collections.synchronizedMap(new HashMap<>());
    private Map<String,CoordinateReferenceSystem> crsIndex = Collections.synchronizedMap(new HashMap<>());
    private Map<String,Integer> countIndex = Collections.synchronizedMap(new HashMap<>());

    public Neo4jFeatureSource(GraphDatabaseService database, ContentEntry entry, Query query) {
        super(entry, query);
        this.database = database;
        this.spatialDatabase = new SpatialDatabaseService(database);
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        String typeName = entry.getTypeName();
        ReferencedEnvelope result = boundsIndex.get(typeName);
        if (result == null) {
            Layer layer = spatialDatabase.getLayer(typeName);
            if (layer != null) {
                try (Transaction tx = database.beginTx()) {
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
        String typeName = entry.getTypeName();
        Integer result = countIndex.get(typeName);
        if (result == null) {
            Layer layer = spatialDatabase.getLayer(entry.getTypeName());
            if (layer != null) {
                try (Transaction tx = database.beginTx()) {
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
        Layer layer = spatialDatabase.getLayer(entry.getTypeName());

        try (Transaction tx = database.beginTx()) {
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
    public Neo4jEntry getEntry() {
        return (Neo4jEntry)super.getEntry();
    }

    @Override
    public Neo4jState getState() {
        return (Neo4jState)super.getState();
    }

    private ReferencedEnvelope convertEnvelopeToRefEnvelope(String typeName, Envelope bbox) {
        return new ReferencedEnvelope(bbox, getCRS(typeName));
    }

    private CoordinateReferenceSystem getCRS(String typeName) {
        CoordinateReferenceSystem result = crsIndex.get(typeName);
        if (result == null) {
            try (Transaction tx = database.beginTx()) {
                Layer layer = spatialDatabase.getLayer(typeName);
                result = layer.getCoordinateReferenceSystem();
                crsIndex.put(typeName, result);
                tx.success();
            }
        }

        return result;
    }

}
