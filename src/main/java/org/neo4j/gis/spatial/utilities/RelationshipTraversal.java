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

import java.util.Iterator;
import org.neo4j.graphdb.Node;

/**
 * Neo4j 4.3 introduced some bugs around closing RelationshipTraversalCursor.
 * This class provides alternative implementations of suspicious methods to work around that issue.
 */
public class RelationshipTraversal {

	/**
	 * Normally just calling iterator.next() once should work, but the bug in Neo4j 4.3 results in leaked cursors
	 * if this iterator comes from the traversal framework, so this code exhausts the traverser and returns the first
	 * node found. For cases where many results could be found, this is expensive. Try to use only when one or few
	 * results
	 * are likely.
	 */
	public static Node getFirstNode(Iterable<Node> nodes) {
		Node found = null;
		for (Node node : nodes) {
			if (found == null) {
				found = node;
			}
		}
		return found;
	}

	/**
	 * Some code has facilities for closing resource at a high level, but the underlying resources are only
	 * Iterators, with no access to the original sources and no way to close the resources properly.
	 * So to avoid the Neo4j 4.3 bug with leaked RelationshipTraversalCursor, we need to exhaust the iterator.
	 */
	public static void exhaustIterator(Iterator<?> source) {
		while (source.hasNext()) {
			source.next();
		}
	}
}
