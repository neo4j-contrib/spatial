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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

/**
 * This class contain functions which can be used to test the integrity of the Rtree.
 * <p>
 * Created by Philip Stephens on 12/11/2016.
 */
public class RTreeTestUtils {

	public static double one_d_overlap(double a1, double a2, double b1, double b2) {
		return Double.max(
				Double.min(a2, b2) - Double.max(a1, b1),
				0.0
		);
	}

	public static double compute_overlap(Envelope a, Envelope b) {
		return one_d_overlap(a.getMinX(), a.getMaxX(), b.getMinX(), b.getMaxX()) *
				one_d_overlap(a.getMinY(), a.getMaxY(), b.getMinY(), b.getMaxY());
	}

	public static double calculate_overlap(Node child) {

		Envelope parent = RTreeIndex.getIndexNodeEnvelope(child);
		List<Envelope> children = new ArrayList<Envelope>();

		for (Relationship r : child.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
			children.add(RTreeIndex.getIndexNodeEnvelope(r.getEndNode()));
		}
		children.sort(Comparator.comparing(Envelope::getMinX, Double::compare));
		double total_overlap = 0.0;
		List<Envelope> activeNodes = new LinkedList<>();

		for (Envelope x : children) {
			activeNodes = activeNodes
					.stream()
					.filter(envelope -> envelope.getMaxX() < x.getMinX())
					.collect(Collectors.toList());
			total_overlap += activeNodes.stream().mapToDouble(envelope -> compute_overlap(x, envelope)).sum();
			activeNodes.add(x);
		}

		return total_overlap / parent.getArea();

	}

	public static Map<Long, Long> get_height_map(Transaction tx, Node root) {
		String id = root.getElementId();

		String cypher = "MATCH p = (root) -[:RTREE_CHILD*0..] ->(child) -[:RTREE_REFERENCE]->(leaf)\n" +
				"    WHERE elementId(root) = " + id + "\n" +
				"    RETURN length(p) as depth, count (*) as freq";
		Result result = tx.execute(cypher);

		int i = 0;
		Map<Long, Long> map = new HashMap<>();
		while (result.hasNext()) {
			Map<String, Object> r = result.next();
			map.put((Long) r.get("depth"), (Long) r.get("freq"));
			i++;
		}
		return map;
	}

	public static boolean check_balance(Transaction tx, Node root) {
		String id = root.getElementId();

		String cypher = "MATCH p = (root) -[:RTREE_CHILD*0..] ->(child) -[:RTREE_REFERENCE]->(leaf)\n" +
				"    WHERE elementId(root) = " + id + "\n" +
				"    RETURN length(p) as depth, count (*) as freq";
		Result result = tx.execute(cypher);

		int i = 0;
		while (result.hasNext()) {
			Map<String, Object> r = result.next();
			System.out.println(r.get("depth").toString() + " : " + r.get("freq"));
			i++;
		}
		return i == 1;

	}


}
