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

import org.neo4j.graphdb.Node;

import org.neo4j.collections.rtree.Envelope;
import com.vividsolutions.jts.geom.Geometry;


/**
 * Find geometries that contain the given geometry
 * 
 * @author Davide Savazzi
 */
public class SearchContain extends AbstractSearchIntersection {

	public SearchContain(Geometry other) {
		super(other);
	}

	protected void onEnvelopeIntersection(Node geomNode, Envelope geomEnvelope) {
		// check if every point of the other geometry is a point of this geometry,
		// and the interiors of the two geometries have at least one point in common		
	    if (geomEnvelope.contains(otherEnvelope)) {
	    	Geometry geometry = decode(geomNode);
	    	if (geometry.contains(other)) add(geomNode, geometry);
	    }
	}

}
