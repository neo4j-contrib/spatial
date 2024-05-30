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

import java.lang.reflect.InvocationTargetException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;

public class TraverserFactory {

	public static Traverser createTraverserInBackwardsCompatibleWay(TraversalDescription traversalDescription,
			Node layerNode) {
		try {
			try {
				return (Traverser) TraversalDescription.class.getDeclaredMethod("traverse",
						Node.class).invoke(traversalDescription, layerNode);
			} catch (NoSuchMethodException e) {
				return (Traverser) TraversalDescription.class.getDeclaredMethod("traverse",
						Node[].class).invoke(traversalDescription, new Object[]{new Node[]{layerNode}});
			}
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new IllegalStateException("You seem to be using an unsupported version of Neo4j.", e);
		}
	}
}
