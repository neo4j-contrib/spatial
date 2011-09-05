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
package org.neo4j.gis.spatial.query;

import org.neo4j.gis.spatial.EnvelopeUtils;
import org.neo4j.gis.spatial.LayerSearch;
import org.neo4j.graphdb.Node;

import org.neo4j.collections.rtree.Envelope;
import com.vividsolutions.jts.geom.Geometry;


/**
 * Find geometries that have no point in common with the given geometry
 * 
 * @author Davide Savazzi
 */
public class SearchDisjoint extends LayerSearch {

	public SearchDisjoint(Geometry other) {
		this.other = other;
		this.otherEnvelope = EnvelopeUtils.fromJtsToNeo4j(other.getEnvelopeInternal());
	}	
	
	public boolean needsToVisit(Envelope indexNodeEnvelope) {
		return true;
	}

	public void onIndexReference(Node geomNode) {
		Envelope geomEnvelope = getEnvelope(geomNode);
		if (!geomEnvelope.intersects(otherEnvelope)) {
			add(geomNode);
		} else {
			Geometry geometry = decode(geomNode);
			if (geometry.disjoint(other)) add(geomNode, geometry);
		}
	}

	private Geometry other;
	private Envelope otherEnvelope;
}