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

import java.util.List;
import java.util.function.Predicate;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.spatial.api.Identifiable;
import org.neo4j.spatial.api.monitoring.ProgressListener;

/**
 * Writable spatial index of a layer.
 * <p>
 * Implementations need a public no-argument constructor: layers re-create their index from the persisted
 * identifier and configuration (see {@link org.neo4j.spatial.api.Configurable}).
 */
public interface SpatialIndexWriter extends SpatialIndexReader, Identifiable {

	default void add(Transaction tx, Node geomNode) {
		add(tx, List.of(geomNode));
	}

	void add(Transaction tx, List<Node> geomNodes);

	void remove(Transaction tx, String geomNodeId, boolean deleteGeomNode, boolean throwExceptionIfNotFound);

	/**
	 * Removes all nodes from the index and deletes the index structure.
	 *
	 * @param deleteGeomNode decides per indexed node whether to delete it; all other nodes are only detached
	 */
	void removeAll(Transaction tx, Predicate<Node> deleteGeomNode, ProgressListener monitor);

	void clear(Transaction tx, ProgressListener monitor);

	/**
	 * Provides a textual description of the index writer.
	 *
	 * @return a description of the index writer, typically including its purpose or functionality
	 */
	String getDescription();

}
