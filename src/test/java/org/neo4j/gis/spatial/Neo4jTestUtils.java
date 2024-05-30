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
package org.neo4j.gis.spatial;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class Neo4jTestUtils {

	public static <T> int countIterable(Iterable<T> iterable) {
		int counter = 0;
		Iterator<T> itr = iterable.iterator();
		while (itr.hasNext()) {
			itr.next();
			counter++;
		}
		return counter;
	}

	public static void debugIndexTree(GraphDatabaseService db, String layerName) {
		try (Transaction tx = db.beginTx()) {
			SpatialDatabaseService spatial = new SpatialDatabaseService(
					new IndexManager((GraphDatabaseAPI) db, SecurityContext.AUTH_DISABLED));
			Layer layer = spatial.getLayer(tx, layerName);
			RTreeIndex index = (RTreeIndex) layer.getIndex();
			printTree(index.getIndexRoot(tx), 0);
			tx.commit();
		}

	}

	private static String arrayString(double[] test) {
		StringBuffer sb = new StringBuffer();
		for (double d : test) {
			addToArrayString(sb, d);
		}
		sb.append("]");
		return sb.toString();
	}

	private static void addToArrayString(StringBuffer sb, Object obj) {
		if (sb.length() == 0) {
			sb.append("[");
		} else {
			sb.append(",");
		}
		sb.append(obj);
	}

	public static void printTree(Node root, int depth) {
		StringBuffer tab = new StringBuffer();
		for (int i = 0; i < depth; i++) {
			tab.append("  ");
		}

		if (root.hasProperty(Constants.PROP_BBOX)) {
			System.out.println(tab + "INDEX: " + root + " BBOX[" + arrayString(
					(double[]) root.getProperty(Constants.PROP_BBOX)) + "]");
		} else {
			System.out.println(tab + "INDEX: " + root);
		}

		StringBuffer data = new StringBuffer();
		for (Relationship rel : root.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_REFERENCE)) {
			if (data.length() > 0) {
				data.append(", ");
			} else {
				data.append("DATA: ");
			}
//			data.append(rel.getEndNode().toString());
			data.append(rel.getEndNode().toString() + " BBOX[" + arrayString((double[]) rel.getEndNode().getProperty
					(Constants
							.PROP_BBOX)) + "]");
		}

		if (data.length() > 0) {
			System.out.println("  " + tab + data);
		}

		for (Relationship rel : root.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
			printTree(rel.getEndNode(), depth + 1);
		}
	}

	public static <T> void assertCollection(Collection<T> collection, T... expectedItems) {
		String collectionString = join(", ", collection.toArray());
		assertEquals(expectedItems.length, collection.size(), collectionString);
		for (T item : expectedItems) {
			assertTrue(collection.contains(item));
		}
	}

	public static <T> Collection<T> asCollection(Iterable<T> iterable) {
		List<T> list = new ArrayList<T>();
		for (T item : iterable) {
			list.add(item);
		}
		return list;
	}

	private static <T> String join(String delimiter, T... items) {
		StringBuffer buffer = new StringBuffer();
		for (T item : items) {
			if (buffer.length() > 0) {
				buffer.append(delimiter);
			}
			buffer.append(item.toString());
		}
		return buffer.toString();
	}

	public static void printDatabaseStats(GraphDatabaseService db, File path) {
		System.out.println("Database stats:");
		System.out.println("\tTotal disk usage: " + (databaseDiskUsage(path)) / (1024.0 * 1024.0) + "MB");
		System.out.println("\tTotal # nodes:    " + getNumberOfNodes(db));
		System.out.println("\tTotal # rels:     " + getNumberOfRelationships(db));
	}

	private static long calculateDiskUsage(File file) {
		if (file.isDirectory()) {
			long count = 0;
			for (File sub : file.listFiles()) {
				count += calculateDiskUsage(sub);
			}
			return count;
		}
		return file.length();
	}

	private static long databaseDiskUsage(File path) {
		return calculateDiskUsage(path);
	}

	private static long getNumberOfNodes(GraphDatabaseService db) {
		try (Transaction tx = db.beginTx()) {
			return (Long) tx.execute("MATCH (n) RETURN count(n)").columnAs("count(n)").next();
		}
	}

	private static long getNumberOfRelationships(GraphDatabaseService db) {
		try (Transaction tx = db.beginTx()) {
			return (Long) tx.execute("MATCH ()-[r]->() RETURN count(r)").columnAs("count(r)").next();
		}
	}
}
