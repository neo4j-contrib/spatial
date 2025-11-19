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

import org.neo4j.graphdb.Node;
import org.neo4j.spatial.api.Envelope;


/**
 * Visitor interface for traversing spatial index structures.
 * <p>
 * This interface implements the Visitor pattern for spatial index traversal,
 * allowing custom logic to be applied during index navigation. The visitor
 * can control which parts of the index to explore and define actions to
 * perform when geometry nodes are encountered.
 * </p>
 * <p>
 * Typical usage involves implementing this interface to define search criteria
 * and result processing logic for spatial queries.
 * </p>
 */
public interface SpatialIndexVisitor {

	/**
	 * Determines whether to visit a particular index node during traversal.
	 * <p>
	 * This method is called for each index node to decide if its subtree
	 * should be explored. Implementations can use the envelope information
	 * to perform spatial filtering and optimize traversal by pruning
	 * irrelevant branches.
	 * </p>
	 *
	 * @param indexNodeEnvelope the bounding envelope of the index node
	 * @return {@code true} if the index node should be visited and its
	 *         subtree explored, {@code false} to skip this branch
	 */
	boolean needsToVisit(Envelope indexNodeEnvelope);

	/**
	 * Callback method invoked when a geometry node is encountered during traversal.
	 * <p>
	 * This method is called for each leaf node (geometry node) that is visited
	 * during the index traversal. Implementations should define the action to
	 * perform with the encountered geometry node, such as collecting results,
	 * performing calculations, or applying filters.
	 * </p>
	 *
	 * @param geomNode the geometry node that was found in the index
	 */
	void onIndexReference(Node geomNode);

}
