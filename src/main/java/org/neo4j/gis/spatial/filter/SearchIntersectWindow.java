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
package org.neo4j.gis.spatial.filter;

import org.neo4j.gis.spatial.Utilities;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.graphdb.Node;

import org.neo4j.collections.rtree.filter.AbstractSearchEnvelopeIntersection;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Find geometries that intersect with the specified search window.
 * 
 * @author Craig Taverner
 */
public class SearchIntersectWindow extends AbstractSearchEnvelopeIntersection {

	private Layer layer;
	private Geometry windowGeom;

	public SearchIntersectWindow(Layer layer, Envelope other) {
		super(layer.getGeometryEncoder(), Utilities.fromJtsToNeo4j(other));
		this.layer = layer;
		this.windowGeom = layer.getGeometryFactory().toGeometry(other);
	}

	@Override
	protected boolean onEnvelopeIntersection(Node geomNode, org.neo4j.collections.rtree.Envelope geomEnvelope) {
		Geometry geometry = layer.getGeometryEncoder().decodeGeometry(geomNode);
		// The next line just calls the method that is causing exceptions on OSM data for testing
		// TODO: Remove when OSM is working properly
		geometry.getEnvelopeInternal();
		return geometry.intersects(windowGeom);
	}

}