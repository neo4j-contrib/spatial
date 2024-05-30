/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gis.spatial.indexfilter;

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.filter.Filter;
import org.geotools.data.neo4j.Neo4jFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.Utilities;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.index.LayerTreeIndexReader;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.SpatialIndexRecordCounter;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.rtree.filter.SearchResults;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;


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
	private final Envelope filterEnvelope;
	private final Layer layer;

	public CQLIndexReader(LayerTreeIndexReader index, Layer layer, String query) throws CQLException {
		super(index);
		this.filter = ECQL.toFilter(query);
		this.layer = layer;
		this.filterEnvelope = Utilities.extractEnvelopeFromFilter(filter);
	}

	private class Counter extends SpatialIndexRecordCounter {

		private final Transaction tx;

		private Counter(Transaction tx) {
			this.tx = tx;
		}

		@Override
		public boolean needsToVisit(Envelope indexNodeEnvelope) {
			return queryIndexNode(indexNodeEnvelope);
		}

		@Override
		public void onIndexReference(Node geomNode) {
			if (queryLeafNode(tx, geomNode)) {
				super.onIndexReference(geomNode);
			}
		}
	}

	private boolean queryIndexNode(Envelope indexNodeEnvelope) {
		return filterEnvelope == null || filterEnvelope.intersects(indexNodeEnvelope);
	}

	private boolean queryLeafNode(Transaction tx, Node indexNode) {
		SpatialDatabaseRecord dbRecord = new SpatialDatabaseRecord(layer, indexNode);
		Neo4jFeatureBuilder builder = Neo4jFeatureBuilder.fromLayer(tx, layer);
		SimpleFeature feature = builder.buildFeature(tx, dbRecord);
		return filter.evaluate(feature);
	}

	@Override
	public int count(Transaction tx) {
		Counter counter = new Counter(tx);
		index.visit(tx, counter, index.getIndexRoot(tx));
		return counter.getResult();
	}

	private SearchFilter wrapSearchFilter(final SearchFilter filter) {
		return new SearchFilter() {

			@Override
			public boolean needsToVisit(Envelope envelope) {
				return queryIndexNode(envelope) &&
						filter.needsToVisit(envelope);
			}

			@Override
			public boolean geometryMatches(Transaction tx, Node geomNode) {
				return queryLeafNode(tx, geomNode) && filter.geometryMatches(tx, geomNode);
			}
		};
	}

	@Override
	public SearchResults searchIndex(Transaction tx, SearchFilter filter) {
		return index.searchIndex(tx, wrapSearchFilter(filter));
	}

	@Override
	public SearchRecords search(Transaction tx, SearchFilter filter) {
		return index.search(tx, wrapSearchFilter(filter));
	}
}
