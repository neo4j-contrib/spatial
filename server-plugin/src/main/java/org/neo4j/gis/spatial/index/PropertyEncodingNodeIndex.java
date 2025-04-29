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

package org.neo4j.gis.spatial.index;

import java.util.Iterator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

/**
 * The PropertyEncodingNodeIndex is a mapper onto a node property backed by a normal Cypher index,
 * and representing a 1D encoding of 2D space.
 * Geohash indexes use Strings for the encoding, while ZOrder and Hilbert space
 * filling curves use Long values.
 *
 * @param <E> either a String or a Long depending on whether the index is geohash or space-filling curve.
 */
public class PropertyEncodingNodeIndex<E> {

	private IndexDefinition index;
	private final String indexName;
	private final Label label;
	private final String propertyKey;
	private final IndexManager indexManager;

	public PropertyEncodingNodeIndex(IndexManager indexManager, String indexName, Label label, String propertyKey) {
		this.indexName = indexName;
		this.label = label;
		this.propertyKey = propertyKey;
		this.indexManager = indexManager;
	}

	public void initialize(Transaction tx) {
		index = indexManager.indexFor(tx, indexName, label, propertyKey);
	}

	public void add(Node geomNode, E indexValueFor) {
		geomNode.addLabel(label);
		geomNode.setProperty(propertyKey, indexValueFor);
	}

	public void remove(Node geomNode) {
		geomNode.removeLabel(label);
		geomNode.removeProperty(propertyKey);
	}

	public Iterable<Node> queryAll(Transaction tx) {
		return Iterators.loop(tx.findNodes(label));
	}

	public Iterator<Node> query(Transaction tx, ExplicitIndexBackedPointIndex.Neo4jIndexSearcher searcher) {
		KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
		return searcher.search(ktx, label, propertyKey);
	}

	public void delete(Transaction tx) {
		for (Node node : queryAll(tx)) {
			node.removeLabel(label);
			node.removeProperty(propertyKey);
		}
		indexManager.deleteIndex(index);
	}
}
