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
package org.neo4j.gis.spatial;

import org.neo4j.gis.spatial.filter.SearchIntersectWindow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.pipes.processing.OrthodromicDistance;

import com.tinkerpop.pipes.filter.FilterPipe;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

public class SimplePointLayer extends EditableLayerImpl {
	
	public static final int LIMIT_RESULTS = 100;

	public GeoPipeline findClosestPointsTo(Coordinate point) {
		Envelope extent = SpatialTopologyUtils.createEnvelopeForGeometryDensityEstimate(this, point, LIMIT_RESULTS);
		return GeoPipeline.start(this, new SearchIntersectWindow(this, extent))
			.calculateOrthodromicDistance(point)
			.sort("OrthodromicDistance");
	}

	public GeoPipeline findClosestPointsTo(Coordinate point, double maxDistanceInKm) {
		Envelope extent = OrthodromicDistance.suggestSearchWindow(point, maxDistanceInKm);
		return GeoPipeline.start(this, new SearchIntersectWindow(this, extent))
			.calculateOrthodromicDistance(point)
			.propertyFilter("OrthodromicDistance", maxDistanceInKm, FilterPipe.Filter.LESS_THAN_EQUAL)
			.sort("OrthodromicDistance");
	}

	public SpatialDatabaseRecord add(Coordinate coordinate) {
		return add(getGeometryFactory().createPoint(coordinate));
	}

	public SpatialDatabaseRecord add(double x, double y) {
		return add(getGeometryFactory().createPoint(new Coordinate(x, y)));
	}
}