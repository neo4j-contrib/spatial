package org.geotools.data.neo4j;

import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.filter.FidFilterImpl;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Envelope;
import org.neo4j.gis.spatial.*;
import org.neo4j.gis.spatial.filter.SearchCQL;
import org.neo4j.gis.spatial.rtree.filter.SearchAll;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Transaction;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.IOException;
import java.util.Iterator;

public class Neo4jSpatialFeatureSource extends ContentFeatureStore {
    private final SpatialDatabaseService spatialDatabaseService;
    private ReferencedEnvelope allEnvelope;
    private CoordinateReferenceSystem crs;
    private static final SearchFilter SEARCH_ALL = new SearchAll();
    private static final ReferencedEnvelope NULL_ENVELOPE = new ReferencedEnvelope();

    public Neo4jSpatialFeatureSource(SpatialDatabaseService spatialDatabaseService, ContentEntry entry, Query query) {
        super(entry, query);
        this.spatialDatabaseService = spatialDatabaseService;
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) {
        final SearchFilter searchFilter = getSearchFilter(query);
        if (SEARCH_ALL.equals(searchFilter)) {
            return getAllEnvelope();
        }
        if (SearchNone.EXCLUDE_SEARCH_FILTER.equals(searchFilter)) {
            return NULL_ENVELOPE;
        }
        Layer layer = getLayer();
        if (layer == null) {
            return NULL_ENVELOPE;
        }
        Envelope envelope = new Envelope();
        try (Transaction tx = spatialDatabaseService.getDatabase().beginTx()) {
            Iterator<SpatialDatabaseRecord> records = layer.getIndex().search(searchFilter);
            while (records.hasNext()) {
                final SpatialDatabaseRecord record = records.next();
                if (record.getGeometry() != null) {
                    envelope.expandToInclude(record.getGeometry().getEnvelopeInternal());
                }
            }
            tx.success();
        }
        return convertEnvelopeToRefEnvelope(envelope);
    }

    private CoordinateReferenceSystem getCRS() {
        if (crs == null) {
            crs = getLayer().getCoordinateReferenceSystem();
        }
        return crs;
    }

    private ReferencedEnvelope convertEnvelopeToRefEnvelope(Envelope bbox) {
        return new ReferencedEnvelope(bbox, getCRS());
    }

    private ReferencedEnvelope getAllEnvelope() {
        if (allEnvelope == null) {
            Layer layer = getLayer();
            if (layer != null) {
                try (Transaction tx = spatialDatabaseService.getDatabase().beginTx()) {
                    Envelope bbox = Utilities.fromNeo4jToJts(layer.getIndex().getBoundingBox());
                    allEnvelope = convertEnvelopeToRefEnvelope(bbox);
                    tx.success();
                }
            }
        }
        return allEnvelope;
    }

    @Override
    protected int getCountInternal(Query query) {
        SearchFilter filter = getSearchFilter(query);
        if (SearchNone.EXCLUDE_SEARCH_FILTER.equals(filter)) {
            return 0;
        }
        int count = -1;
        Layer layer = getLayer();

        try (Transaction tx = spatialDatabaseService.getDatabase().beginTx()) {
            count = layer.getIndex().count();
            tx.success();
        }
        return count;
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) {
        Layer layer = getLayer();
        SearchFilter filter = getSearchFilter(query);
        if (SearchNone.EXCLUDE_SEARCH_FILTER.equals(filter)) {
            return new Neo4jSpatialFeatureReader(layer, getSchema(), null);
        }

        try (Transaction tx = spatialDatabaseService.getDatabase().beginTx()) {
            Iterator<SpatialDatabaseRecord> records = layer.getIndex().search(filter);
            tx.success();
            return new Neo4jSpatialFeatureReader(layer, getSchema(), records);
        }
    }

    private SearchFilter getSearchFilter(Query query) {
        Filter filter = getFilter(query);
        if (Filter.EXCLUDE.equals(filter)) {
            return SearchNone.EXCLUDE_SEARCH_FILTER;
        }
        if (Filter.INCLUDE.equals(filter)) {
            return SEARCH_ALL;
        }
        if (filter instanceof FidFilterImpl) {
            // filter by Feature unique id
            throw new UnsupportedOperationException("Unsupported use of FidFilterImpl in Neo4jSpatialDataStore");
        }
        return new SearchCQL(getLayer(), filter);
    }

    private Filter getFilter(Query query) {
        Filter filter = null;
        if (query != null) {
            filter = query.getFilter();
        }
        if (filter == null) {
            filter = Filter.INCLUDE;
        }
        return filter;
    }

    @Override
    protected SimpleFeatureType buildFeatureType() {
        SimpleFeatureType result;

        try (Transaction tx = spatialDatabaseService.getDatabase().beginTx()) {
            Layer layer = getLayer();
            result = Neo4jFeatureBuilder.getTypeFromLayer(layer);
            tx.success();
        }
        return result;
    }

    private Layer getLayer() {
        return spatialDatabaseService.getLayer(entry.getName().getURI());
    }

    @Override
    public Neo4jSpatialDataStore getDataStore() {
        return (Neo4jSpatialDataStore) super.getDataStore();
    }

    @Override
    protected FeatureWriter<SimpleFeatureType, SimpleFeature> getWriterInternal(Query query, int i) throws IOException {
        Filter filter = getFilter(query);
        if (transaction == null) {
            throw new NullPointerException("getFeatureWriter requires Transaction: did you mean to use Transaction.AUTO_COMMIT?");
        }
        FeatureReader<SimpleFeatureType, SimpleFeature> reader = getReader(filter);
        return new Neo4jSpatialFeatureWriter(getEditableLayer(), reader);
    }

    private EditableLayer getEditableLayer() throws IOException {
        Layer layer = getLayer();
        if (layer == null) {
            throw new IOException("Layer not found: " + entry.getName().getURI());
        }

        if (!(layer instanceof EditableLayer)) {
            throw new IOException("Cannot create a FeatureWriter on a read-only layer: " + layer);
        }

        return (EditableLayer) layer;
    }
}
