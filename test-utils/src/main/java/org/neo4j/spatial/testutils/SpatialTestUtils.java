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

package org.neo4j.spatial.testutils;

import java.io.File;
import java.util.Objects;
import org.locationtech.jts.geom.Envelope;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.spatial.api.layer.Layer;

// TODO do we need this one?
public class SpatialTestUtils {

	public static void debugEnvelope(Envelope bbox, String layer, String name) {
		System.out.println("Layer '" + layer + "' has envelope '" + name + "': " + bbox);
		System.out.println("\tX: [" + bbox.getMinX() + ":" + bbox.getMaxX() + "]");
		System.out.println("\tY: [" + bbox.getMinY() + ":" + bbox.getMaxY() + "]");
	}

	public static int checkIndexCount(Transaction tx, Layer layer) {
		if (layer.getIndex().count(tx) < 1) {
			System.out.println("Warning: index count zero: " + layer.getName());
		}
		System.out.println(
				"Layer '" + layer.getName() + "' has " + layer.getIndex().count(tx) + " entries in the index");
		return layer.getIndex().count(tx);
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
			for (File sub : Objects.requireNonNull(file.listFiles())) {
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
