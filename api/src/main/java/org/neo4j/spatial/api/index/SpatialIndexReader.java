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
package org.neo4j.spatial.api.index;

import java.util.Map;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.spatial.api.Envelope;
import org.neo4j.spatial.api.EnvelopeDecoder;
import org.neo4j.spatial.api.SearchFilter;
import org.neo4j.spatial.api.SearchResults;
import org.neo4j.spatial.api.monitoring.TreeMonitor;

public interface SpatialIndexReader {

	EnvelopeDecoder getEnvelopeDecoder();

	boolean isEmpty(Transaction tx);

	int count(Transaction tx);

	Envelope getBoundingBox(Transaction tx);

	boolean isNodeIndexed(Transaction tx, String nodeId);

	Iterable<Node> getAllIndexedNodes(Transaction tx);

	SearchResults searchIndex(Transaction tx, SearchFilter filter);

	void addMonitor(TreeMonitor monitor);

	void configure(Map<String, Object> config);

	default void finalizeTransaction(Transaction tx) {
	}
}
