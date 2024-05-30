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

import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.pipes.processing.OrthodromicDistance;
import org.neo4j.graphdb.Transaction;

public class SimplePointLayer extends EditableLayerImpl {

	public static final int LIMIT_RESULTS = 100;

	public SpatialDatabaseRecord add(Transaction tx, Coordinate coordinate) {
		return add(tx, coordinate, null, null);
	}

	public SpatialDatabaseRecord add(Transaction tx, Coordinate coordinate, String[] fieldsName, Object[] fields) {
		return add(tx, getGeometryFactory().createPoint(coordinate), fieldsName, fields);
	}

	public SpatialDatabaseRecord add(Transaction tx, double x, double y) {
		return add(tx, new Coordinate(x, y), null, null);
	}

	public SpatialDatabaseRecord add(Transaction tx, double x, double y, String[] fieldsName, Object[] fields) {
		return add(tx, new Coordinate(x, y), fieldsName, fields);
	}

	public static Integer getGeometryType() {
		return GTYPE_POINT;
	}

	public List<GeoPipeFlow> findClosestPointsTo(Transaction tx, Coordinate coordinate, double d) {
		return GeoPipeline
				.startNearestNeighborLatLonSearch(tx, this, coordinate, d)
				.sort(OrthodromicDistance.DISTANCE).toList();
	}

	public List<GeoPipeFlow> findClosestPointsTo(Transaction tx, Coordinate coordinate, int numberOfItemsToFind) {
		return GeoPipeline
				.startNearestNeighborLatLonSearch(tx, this, coordinate, 2 * numberOfItemsToFind)
				.sort(OrthodromicDistance.DISTANCE).next(numberOfItemsToFind);
	}

	public List<GeoPipeFlow> findClosestPointsTo(Transaction tx, Coordinate coordinate) {
		return GeoPipeline
				.startNearestNeighborLatLonSearch(tx, this, coordinate, 2 * LIMIT_RESULTS)
				.sort(OrthodromicDistance.DISTANCE).next(LIMIT_RESULTS);
	}
}
