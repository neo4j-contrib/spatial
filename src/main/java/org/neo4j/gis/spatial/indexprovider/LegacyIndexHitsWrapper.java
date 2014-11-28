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
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.api.LegacyIndexHits;

public class LegacyIndexHitsWrapper implements LegacyIndexHits {

	private final IndexHits<Node> hits;
	
	public LegacyIndexHitsWrapper(IndexHits<Node> hits) {
		
		this.hits = hits;
	}
	
	@Override
	public int size() {
		return hits.size();
	}

	@Override
	public float currentScore() {
		return hits.currentScore();
	}

	@Override
	public boolean hasNext() {
		return hits.hasNext();
	}

	@Override
	public long next() {
		return hits.next().getId();
	}

	@Override
	public void close() {
		hits.close();
	}
	
}
