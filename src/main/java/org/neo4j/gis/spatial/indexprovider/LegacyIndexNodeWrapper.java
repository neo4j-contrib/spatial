/**
 * Copyright (c) 2010-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.indexprovider;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.api.LegacyIndex;
import org.neo4j.kernel.api.LegacyIndexHits;

public class LegacyIndexNodeWrapper implements LegacyIndex {
	
	private final Index<Node> index;

	public LegacyIndexNodeWrapper(Index<Node> index) {
		this.index = index;
	}
	
	@Override
	public LegacyIndexHits get(String key, Object value) {
		return new LegacyIndexHitsWrapper(index.get(key, value));
	}

	@Override
	public LegacyIndexHits query(String key, Object queryOrQueryObject) {
		return new LegacyIndexHitsWrapper(index.query(key, queryOrQueryObject));
	}

	@Override
	public LegacyIndexHits query(Object queryOrQueryObject) {
		return new LegacyIndexHitsWrapper(index.query(queryOrQueryObject));
	}

	@Override
	public void addNode(long id, String key, Object value) {
		index.add(node(id), key, value);
	}

	@Override
	public void remove(long id, String key, Object value) {
		index.remove(node(id), key, value);
	}

	@Override
	public void remove(long id, String key) {
		index.remove(node(id), key);
	}

	@Override
	public void remove(long id) {
		index.remove(node(id));
	}

	@Override
	public void drop() {
		index.delete();
	}

	@Override
	public LegacyIndexHits get(String key, Object value, long startNode, long endNode) {
		throw new UnsupportedOperationException();
	}

	@Override
	public LegacyIndexHits query(String key, Object queryOrQueryObject, long startNode, long endNode) {
		throw new UnsupportedOperationException();
	}

	@Override
	public LegacyIndexHits query(Object queryOrQueryObject, long startNode, long endNode) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addRelationship(long entity, String key, Object value, long startNode, long endNode) {
		throw new UnsupportedOperationException();
	}
	
	private Node node(long id) {
		return index.getGraphDatabase().getNodeById(id);
	}
}
