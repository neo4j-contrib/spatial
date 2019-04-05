/**
 * Copyright (c) 2010-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;

import org.locationtech.jts.geom.Coordinate;
import org.neo4j.gis.spatial.pipes.processing.OrthodromicDistance;

public class SimplePointLayer extends EditableLayerImpl {

	public static final int LIMIT_RESULTS = 100;

	public SpatialDatabaseRecord add(Coordinate coordinate) {
		return add(coordinate, null, null);
	}

	public SpatialDatabaseRecord add(Coordinate coordinate, String[] fieldsName, Object[] fields) {
		return add(getGeometryFactory().createPoint(coordinate), fieldsName, fields);
	}

	public SpatialDatabaseRecord add(double x, double y) {
		return add(new Coordinate(x, y), null, null);
	}

	public SpatialDatabaseRecord add(double x, double y, String[] fieldsName, Object[] fields) {
		return add(new Coordinate(x, y), fieldsName, fields);
	}

	public Integer getGeometryType() {
		return GTYPE_POINT;
	}

	public List<GeoPipeFlow> findClosestPointsTo(Coordinate coordinate, double d) {
		return GeoPipeline
			.startNearestNeighborLatLonSearch(this, coordinate, d)
			.sort(OrthodromicDistance.DISTANCE).toList();
	}

	public List<GeoPipeFlow> findClosestPointsTo(Coordinate coordinate, int numberOfItemsToFind) {
		return GeoPipeline
			.startNearestNeighborLatLonSearch(this, coordinate, 2 * numberOfItemsToFind)
			.sort(OrthodromicDistance.DISTANCE).next(numberOfItemsToFind);
	}

	public List<GeoPipeFlow> findClosestPointsTo(Coordinate coordinate) {
		return GeoPipeline
			.startNearestNeighborLatLonSearch(this, coordinate, 2 * LIMIT_RESULTS)
			.sort(OrthodromicDistance.DISTANCE).next(LIMIT_RESULTS);
	}
}
