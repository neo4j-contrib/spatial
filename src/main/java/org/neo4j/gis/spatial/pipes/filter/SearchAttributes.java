/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.gis.spatial.pipes.filter;

import org.neo4j.collections.rtree.Envelope;
import org.neo4j.gis.spatial.AbstractLayerSearch;
import org.neo4j.graphdb.Node;

import com.tinkerpop.pipes.filter.FilterPipe;
import com.tinkerpop.pipes.util.PipeHelper;

public class SearchAttributes extends AbstractLayerSearch {
	
	private String key;
	private String value;
	private FilterPipe.Filter filter;

	public SearchAttributes(String key, String value, FilterPipe.Filter filter) {
		this.key = key;
		this.value = value;
		this.filter = filter;
	}

	public boolean needsToVisit(Envelope indexNodeEnvelope) {
		return true;
	}

	public void onIndexReference(Node geomNode) {

		
		if (geomNode.hasProperty(key)
				&& PipeHelper.compareObjects(filter, geomNode.getProperty(key), value)) {
			add(geomNode);
		}
		
	}

}
