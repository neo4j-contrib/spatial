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

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;

public interface IndexManager {

	/**
	 * Blocking call that spawns a thread to create an index and then waits for that thread to finish.
	 * This is highly likely to cause deadlocks on index checks, so be careful where it is used.
	 * Best used if you can commit any other outer transaction first, then run this, and after that
	 * start a new transaction. For example, see the OSMImport approaching to batching transactions.
	 * It is possible to use this in procedures with outer transactions if you can ensure the outer
	 * transactions are read-only.
	 */
	IndexDefinition indexFor(Transaction tx, String indexName, Label label, String propertyKey);

	/**
	 * Non-blocking call that spawns a thread to create an index and then waits for that thread to finish.
	 * Use this especially on indexes that are not immediately needed. Also use it if you have an outer
	 * transaction that cannot be committed before making this call.
	 */
	void makeIndexFor(Transaction tx, String indexName, Label label, String propertyKey);

	void deleteIndex(IndexDefinition index);
}
