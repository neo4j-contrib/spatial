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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.neo4j.gis.spatial.query.SearchPointsWithinOrthodromicDistance;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

public class SimplePointLayer extends EditableLayerImpl {
	public static final int LIMIT_RESULTS = 100;

	public List<SpatialDatabaseRecord> findClosestPointsTo(Coordinate point) {
		Envelope extent = SpatialTopologyUtils.createEnvelopeForGeometryDensityEstimate(this, point, LIMIT_RESULTS);
		SearchPointsWithinOrthodromicDistance distanceQuery = new SearchPointsWithinOrthodromicDistance(point, extent, true);
		return findClosestPoints(distanceQuery);
	}

	public List<SpatialDatabaseRecord> findClosestPointsTo(Coordinate point, double distanceInKm) {
		SearchPointsWithinOrthodromicDistance distanceQuery = new SearchPointsWithinOrthodromicDistance(point, distanceInKm, true);
		return findClosestPoints(distanceQuery);
	}

	private List<SpatialDatabaseRecord> findClosestPoints(SearchPointsWithinOrthodromicDistance distanceQuery) {
		getIndex().executeSearch(distanceQuery);
		List<SpatialDatabaseRecord> results = distanceQuery.getResults();
		Collections.sort(results, new Comparator<SpatialDatabaseRecord>(){

			public int compare(SpatialDatabaseRecord arg0, SpatialDatabaseRecord arg1) {
				return ((Double) arg0.getUserData()).compareTo((Double) arg1.getUserData());
			}
		});
		return results;
	}

	public SpatialDatabaseRecord add(Coordinate coordinate) {
		return add(getGeometryFactory().createPoint(coordinate));
	}

	public SpatialDatabaseRecord add(double x, double y) {
		return add(getGeometryFactory().createPoint(new Coordinate(x, y)));
	}

}
