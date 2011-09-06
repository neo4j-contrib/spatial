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
package org.neo4j.gis.spatial.pipes;

import java.util.HashMap;

import org.neo4j.gis.spatial.AbstractSearch;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Envelope;

public class SearchAttributes extends AbstractSearch {
	
	private HashMap<String, String> attributes;
	
	public SearchAttributes(HashMap<String, String> attributes) {
		this.attributes = attributes;
	}

	public boolean needsToVisit(Envelope indexNodeEnvelope) {
		return true;
	}

	public void onIndexReference(Node geomNode) {
		
		for (String key : attributes.keySet()) {
			if (geomNode.hasProperty(key)
					&& geomNode.getProperty(key).equals(attributes.get(key))) {
				add(geomNode);
			}
		}
		
	}

}
