/*
 * Copyright (c) 2010-2020 "Neo4j,"
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
package org.neo4j.gis.spatial.filter;

import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.Utilities;
import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.gis.spatial.rtree.filter.AbstractSearchEnvelopeIntersection;
import org.neo4j.graphdb.Node;

/**
 * Find geometries that intersect with the specified search window.
 *
 * @author Craig Taverner
 */
public class SearchIntersectWindow extends AbstractSearchEnvelopeIntersection {

	private final Layer layer;
	private final Geometry windowGeom;

	public SearchIntersectWindow(Layer layer, Envelope envelope) {
		this(layer, Utilities.fromNeo4jToJts(envelope));
	}

	public SearchIntersectWindow(Layer layer, org.locationtech.jts.geom.Envelope other) {
		super(layer.getGeometryEncoder(), Utilities.fromJtsToNeo4j(other));
		this.layer = layer;
		this.windowGeom = layer.getGeometryFactory().toGeometry(other);
	}

	@Override
	protected boolean onEnvelopeIntersection(Node geomNode, org.neo4j.gis.spatial.rtree.Envelope geomEnvelope) {
		Geometry geometry = layer.getGeometryEncoder().decodeGeometry(geomNode);
		// The next line just calls the method that is causing exceptions on OSM data for testing
		// TODO: Remove when OSM is working properly
		geometry.getEnvelopeInternal();
		return geometry.intersects(windowGeom);
	}

}
