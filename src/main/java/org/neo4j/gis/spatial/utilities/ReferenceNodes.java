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

package org.neo4j.gis.spatial.utilities;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * In Neo4j 0.x and even 1.x it was common to not use an index for the starting point of a search.
 * Instead, Neo4j had a single reference node id=0 which always existed
 * and all models could be connected at some point to that node.
 * Finding things was always by traversal.
 * This model was used in Neo4j Spatial, and transitioned to using a special spatial reference node.
 * However, this kind of thinking became more and more of a problem as we moved to procedures in Neo4j 3.0
 * and in particular the nested transaction model of Neo4j 4.0. Since this node was created on-demand
 * even in read-only queries like 'findLayer', it messed with nested transactions where both might
 * try to create the same node, even if the developer was careful to split read and write aspects of the
 * code.
 * <p>
 * It is time to stop using a root node. This class will remain only for the purpose of helping
 * users transition older spatial models away from root nodes.
 */
public class ReferenceNodes {

	public static final Label LABEL_REFERENCE = Label.label("ReferenceNode");
	public static final String PROP_NAME = "name";

	@Deprecated
	public static Node getReferenceNode(Transaction tx, String name) {
		throw new IllegalStateException("It is no longer valid to use a root or reference node in the spatial model");
	}

	public static Node findDeprecatedReferenceNode(Transaction tx, String name) {
		return tx.findNode(LABEL_REFERENCE, PROP_NAME, name);
	}

	/**
	 * Should be used for tests only. No attempt is made to ensure no duplicates are created.
	 */
	public static Node createDeprecatedReferenceNode(Transaction tx, String name) {
		Node node = tx.createNode(LABEL_REFERENCE);
		node.setProperty(PROP_NAME, name);
		return node;
	}
}
