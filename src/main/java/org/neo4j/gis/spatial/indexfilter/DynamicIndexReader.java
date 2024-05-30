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

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.index.LayerTreeIndexReader;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.SpatialIndexRecordCounter;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.rtree.filter.SearchResults;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;


/**
 * The standard DynamicIndexReader allows for graph traversal and property
 * match queries written in JSON. The JSON code is expected to be a match
 * for a sub-graph and it's properties. It only supports tree structures,
 * since JSON is a tree format. The root of the JSON is the geometry node,
 * which means that queries for properties on nodes further away require
 * traversals in the JSON. The following example demonstrates a query for
 * an OSM geometry layer, with a test of the geometry type on the geometry
 * node itself, followed by a two-step traversal to the ways tag node, and
 * then a query on the tags.
 *
 * <pre>
 * { "properties": {"type": "geometry"},
 *   "step": {"type": "GEOM", "direction": "INCOMING"
 *     "step": {"type": "TAGS", "direction": "OUTGOING"
 *       "properties": {"highway": "residential"}
 *     }
 *   }
 * }
 * </pre>
 * <p>
 * This will work with OSM datasets, traversing from the geometry node to
 * the way node and then to the tags node to test if the way is a
 * residential street.
 */
public class DynamicIndexReader extends LayerIndexReaderWrapper {

	private final JSONObject query;

	private class DynamicRecordCounter extends SpatialIndexRecordCounter {

		@Override
		public boolean needsToVisit(Envelope indexNodeEnvelope) {
			return queryIndexNode(indexNodeEnvelope);
		}

		@Override
		public void onIndexReference(Node geomNode) {
			if (queryLeafNode(geomNode)) {
				super.onIndexReference(geomNode);
			}
		}
	}

	public DynamicIndexReader(LayerTreeIndexReader index, String query) {
		super(index);
		this.query = (JSONObject) JSONValue.parse(query);
	}

	private static boolean queryIndexNode(Envelope indexNodeEnvelope) {
		// TODO: Support making the query on each index node for performance
		return true;
	}

	/**
	 * This method is there the real querying is done. It first tests for
	 * properties on the geometry node, and then steps though the tree
	 * structure of the JSON, and a matching structure in the graph,
	 * querying recursively each nodes properties on the way, as along as
	 * the JSON contains to have properties to test, and traversal steps to
	 * take.
	 *
	 * @param geomNode the node to test
	 * @return true if the node matches the query string, or the query
	 * string is empty
	 */
	private boolean queryLeafNode(Node geomNode) {
		// TODO: Extend support for more complex queries
		JSONObject properties = (JSONObject) query.get("properties");
		JSONObject step = (JSONObject) query.get("step");
		return queryNodeProperties(geomNode, properties) && stepAndQuery(geomNode, step);
	}

	private static boolean stepAndQuery(Node source, JSONObject step) {
		if (step != null) {
			JSONObject properties = (JSONObject) step.get("properties");
			RelationshipType relType = RelationshipType.withName(step.get("type").toString());
			Relationship rel = source.getSingleRelationship(relType,
					Direction.valueOf(step.get("direction").toString()));
			if (rel != null) {
				Node node = rel.getOtherNode(source);
				step = (JSONObject) step.get("step");
				return queryNodeProperties(node, properties) && stepAndQuery(node, step);
			}
			return false;
		}
		return true;
	}

	private static boolean queryNodeProperties(Node node, JSONObject properties) {
		if (properties != null) {
			if (properties.containsKey("geometry")) {
				System.out.println("Unexpected 'geometry' in query string");
				properties.remove("geometry");
			}

			for (Object key : properties.keySet()) {
				Object value = node.getProperty(key.toString(), null);
				Object match = properties.get(key);
				// TODO: Find a better way to solve minor type mismatches (Long!=Integer) than the string conversion below
				if (value == null || (match != null && !value.equals(match) && !value.toString()
						.equals(match.toString()))) {
					return false;
				}
			}
		}

		return true;
	}

	@Override
	public int count(Transaction tx) {
		DynamicRecordCounter counter = new DynamicRecordCounter();
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
				return queryLeafNode(geomNode) && filter.geometryMatches(tx, geomNode);
			}
		};
	}

	@Override
	public SearchResults searchIndex(Transaction tx, final SearchFilter filter) {
		return index.searchIndex(tx, wrapSearchFilter(filter));
	}

	@Override
	public SearchRecords search(Transaction tx, SearchFilter filter) {
		return index.search(tx, wrapSearchFilter(filter));
	}
}
