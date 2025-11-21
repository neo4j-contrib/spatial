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
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.spatial.api.Identifiable;
import org.neo4j.spatial.api.monitoring.ProgressListener;


public interface SpatialIndexWriter extends SpatialIndexReader, Identifiable {

	default void add(Transaction tx, Node geomNode) {
		add(tx, List.of(geomNode));
	}

	void add(Transaction tx, List<Node> geomNodes);

	void remove(Transaction tx, String geomNodeId, boolean deleteGeomNode, boolean throwExceptionIfNotFound);

	void removeAll(Transaction tx, boolean deleteGeomNodes, ProgressListener monitor);

	void clear(Transaction tx, ProgressListener monitor);

}
