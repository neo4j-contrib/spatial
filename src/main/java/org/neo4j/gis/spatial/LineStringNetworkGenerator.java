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
package org.neo4j.gis.spatial;

import java.util.Iterator;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.neo4j.gis.spatial.filter.SearchIntersect;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * Creates a Network of LineStrings.
 * If a LineString start point or end point is equal to some other LineString start point or end point,
 * the two LineStrings are connected together with a Relationship.
 */
public class LineStringNetworkGenerator {

	private final EditableLayer pointsLayer;
	private final EditableLayer edgesLayer;
	private final Double buffer;

	public LineStringNetworkGenerator(EditableLayer pointsLayer, EditableLayer edgesLayer) {
		this(pointsLayer, edgesLayer, null);
	}

	public LineStringNetworkGenerator(EditableLayer pointsLayer, EditableLayer edgesLayer, Double buffer) {
		this.pointsLayer = pointsLayer;
		this.edgesLayer = edgesLayer;
		this.buffer = buffer;
	}

	public void add(Transaction tx, SpatialDatabaseRecord record) {
		Geometry geometry = record.getGeometry();
		if (geometry instanceof MultiLineString) {
			add(tx, (MultiLineString) geometry, record);
		} else // TODO better handling?
			if (geometry instanceof LineString) {
				add(tx, (LineString) geometry, record);
			} else {
				throw new IllegalArgumentException("geometry type not supported: " + geometry.getGeometryType());
			}
	}

	public void add(Transaction tx, MultiLineString lines) {
		add(tx, lines, null);
	}

	public void add(Transaction tx, LineString line) {
		add(tx, line, null);
	}

	protected void add(Transaction tx, MultiLineString line, SpatialDatabaseRecord record) {
		for (int i = 0; i < line.getNumGeometries(); i++) {
			add(tx, (LineString) line.getGeometryN(i), record);
		}
	}

	protected void add(Transaction tx, LineString line, SpatialDatabaseRecord edge) {
		if (edge == null) {
			edge = edgesLayer.add(tx, line);
		}

		// TODO reserved property?
		edge.setProperty("_network_length", edge.getGeometry().getLength());

		addEdgePoint(tx, edge.getGeomNode(), line.getStartPoint());
		addEdgePoint(tx, edge.getGeomNode(), line.getEndPoint());
	}

	protected void addEdgePoint(Transaction tx, Node edge, Geometry edgePoint) {
		if (buffer != null) {
			edgePoint = edgePoint.buffer(buffer);
		}

		Iterator<SpatialDatabaseRecord> results = pointsLayer.getIndex()
				.search(tx, new SearchIntersect(pointsLayer, edgePoint));
		if (!results.hasNext()) {
			SpatialDatabaseRecord point = pointsLayer.add(tx, edgePoint);
			edge.createRelationshipTo(point.getGeomNode(), SpatialRelationshipTypes.NETWORK);
		} else {
			while (results.hasNext()) {
				edge.createRelationshipTo(results.next().getGeomNode(), SpatialRelationshipTypes.NETWORK);
			}
		}
	}
}
