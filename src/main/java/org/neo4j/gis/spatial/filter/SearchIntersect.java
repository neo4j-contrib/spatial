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
package org.neo4j.gis.spatial.filter;

import org.neo4j.collections.rtree.Envelope;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Geometry;


/**
 * Find geometries that have at least one point in common with the given geometry
 * 
 * @author Davide Savazzi
 * @author Craig Taverner
 */
public class SearchIntersect extends AbstractSearchIntersection {

	public SearchIntersect(Layer layer, Geometry other) {
		super(layer, other);
	}

	protected boolean onEnvelopeIntersection(Node geomNode, Envelope geomEnvelope) {
		Geometry geometry = decode(geomNode);
		return geometry.intersects(referenceGeometry);
	}

}