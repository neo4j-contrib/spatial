/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.gis.spatial.indexfilter;

import java.util.List;

import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.neo4j.collections.rtree.Envelope;
import org.neo4j.collections.rtree.SpatialIndexRecordCounter;
import org.neo4j.collections.rtree.search.Search;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.LayerSearch;
import org.neo4j.gis.spatial.LayerTreeIndexReader;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.geotools.data.Neo4jFeatureBuilder;
import org.neo4j.graphdb.Node;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;


/**
 * This class enables support for CQL based dynamic layers. This means the
 * filtering on the result set is based on matches to a CQL query. Some key
 * differences between CQL queries and JSON queries are:
 * <ul>
 * <li>CQL will operate on the geometry itself, performing spatial
 * operations, but also requiring the geometry to be created from the graph.
 * This makes it slower than the JSON approach, but richer from a GIS
 * perspective</li>
 * <li>JSON will operate on the graph itself, and so it is more specific to
 * the data model, and not at all specific to the GIS meaning of the data.
 * This makes it faster, but more complex to develop to. You really need to
 * know your graph structure well to write a complex JSON query. For simple
 * single-node property matches, this is the easiest solution.</li>
 * </ul>
 * 
 * @author dwins
 */
public class CQLIndexReader extends LayerIndexReaderWrapper {

    private final Filter filter;
    private final Neo4jFeatureBuilder builder;
    private final Layer layer;

    public CQLIndexReader(LayerTreeIndexReader index, Layer layer, String query) throws CQLException {
        super(index);
        this.filter = ECQL.toFilter(query);
        this.builder = new Neo4jFeatureBuilder(layer);
        this.layer = layer;
    }

    private class Counter extends SpatialIndexRecordCounter {
        public boolean needsToVisit(Envelope indexNodeEnvelope) {
            return queryIndexNode(indexNodeEnvelope);
        }

        public void onIndexReference(Node geomNode) {
            if (queryLeafNode(geomNode)) {
                super.onIndexReference(geomNode);
            }
        }
    }

    private class FilteredSearch implements Search {
    	
        private Search delegate;
        
        public FilteredSearch(Search delegate) {
            this.delegate = delegate;
        }

		@Override            
        public List<Node> getResults() {
            return delegate.getResults();
        }

		@Override            
        public boolean needsToVisit(Envelope indexNodeEnvelope) {
            return delegate.needsToVisit(indexNodeEnvelope);
        }

		@Override            
        public void onIndexReference(Node geomNode) {
            if (queryLeafNode(geomNode)) {
                delegate.onIndexReference(geomNode);
            }
        }
    }        
    
    private class FilteredLayerSearch implements LayerSearch {
    	
        private LayerSearch delegate;
        
        public FilteredLayerSearch(LayerSearch delegate) {
            this.delegate = delegate;
        }

		@Override            
        public List<Node> getResults() {
            return delegate.getResults();
        }

		@Override            
        public boolean needsToVisit(Envelope indexNodeEnvelope) {
            return delegate.needsToVisit(indexNodeEnvelope);
        }

		@Override            
        public void onIndexReference(Node geomNode) {
            if (queryLeafNode(geomNode)) {
                delegate.onIndexReference(geomNode);
            }
        }

		@Override
		public void setLayer(Layer layer) {
			delegate.setLayer(layer);
		}

		@Override
		public List<SpatialDatabaseRecord> getExtendedResults() {
			return delegate.getExtendedResults();
		}
    }

    private boolean queryIndexNode(Envelope indexNodeEnvelope) {
        return true;
    }

	private boolean queryLeafNode(Node indexNode) {
		SpatialDatabaseRecord dbRecord = new SpatialDatabaseRecord(layer, indexNode);
		SimpleFeature feature = builder.buildFeature(dbRecord);
		return filter.evaluate(feature);
	}

	public int count() {
		Counter counter = new Counter();
		index.visit(counter, index.getIndexRoot());
		return counter.getResult();
	}

	/**
	 * @deprecated
	 */
	public void executeSearch(final Search search) {
		if (LayerSearch.class.isAssignableFrom(search.getClass())) {
			index.executeSearch(new FilteredLayerSearch((LayerSearch) search));
		} else {
			index.executeSearch(new FilteredSearch(search));
		}
	}
}