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

import java.util.List;

import org.neo4j.gis.spatial.query.SearchIntersectWindow;
import org.neo4j.graphdb.Transaction;

import com.vividsolutions.jts.geom.Envelope;

/**
 * @author Davide Savazzi
 */
public class TestSearch extends Neo4jTestCase {
	private boolean enabled = false;

	public void testOne() {
		// TODO: This old test from Davide is no longer valid. Either remove or
		// update
		if (enabled) {
			Layer layer;
			SpatialIndexReader rtreeIndex;
			Envelope bbox;

			Transaction tx = graphDb().beginTx();
			try {
				SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
				layer = spatialService.getLayer("roads");
				rtreeIndex = layer.getIndex();
				bbox = rtreeIndex.getLayerBoundingBox();

				tx.success();
			} finally {
				tx.finish();
			}

			double minx = Math.ceil(bbox.getMinX());
			double miny = Math.ceil(bbox.getMinY());
			double maxx = minx + 3; // Math.floor(bbox.getMaxX());
			double maxy = miny + 3; // Math.floor(bbox.getMaxY());
			double interval = 0.1;

			System.out.println("bbox used: " + minx + "," + miny + "," + maxx + "," + maxy);

			int counter = 0;
			int results = 0;

			long qMinTime = Long.MAX_VALUE;
			int qMinTimeResult = 0;

			long qMaxTime = Long.MIN_VALUE;
			int qMaxTimeResult = 0;

			long start = System.currentTimeMillis();
			for (double x = minx; x < maxx; x = x + interval) {
				for (double y = miny; y < maxy; y = y + interval) {
					tx = graphDb().beginTx();
					try {
						Search searchQuery = new SearchIntersectWindow(new Envelope(x, x + interval, y, y + interval));

						long qStart = System.currentTimeMillis();
						rtreeIndex.executeSearch(searchQuery);
						List<SpatialDatabaseRecord> rtreeResults = searchQuery.getResults();
						long qStop = System.currentTimeMillis();
						long qElapsedTime = qStop - qStart;

						// System.out.println(query + " -> " +
						// rtreeResults.size() + " in " + qElapsedTime);

						if (qElapsedTime < qMinTime) {
							qMinTime = qElapsedTime;
							qMinTimeResult = rtreeResults.size();
						}

						if (qElapsedTime > qMaxTime) {
							qMaxTime = qElapsedTime;
							qMaxTimeResult = rtreeResults.size();
						}

						counter++;
						results += rtreeResults.size();
						tx.success();
					} finally {
						tx.finish();
					}
				}
			}
			long stop = System.currentTimeMillis();
			long elapsedTime = stop - start;
			System.out.println("queries: " + counter + ", results: " + results + ", avg: " + (elapsedTime / counter) + ", min: " + qMinTime
			        + " (for " + qMinTimeResult + " results), max: " + qMaxTime + " (for " + qMaxTimeResult + " results)");

			// System.out.println("geometrie calcolate: " +
			// SearchIntersectWindow.geometriecalcolate);
			// System.out.println("falsi positivi: " +
			// SearchIntersectWindow.falsipositivi);
		}
	}

	public void testTwo() {
		// TODO: This old test from Davide is no longer valid. Either remove or
		// update. Currently it appears to rely on some exiting geometries in
		// the database.
		double xmin = 0;
		double xmax = 0;
		double ymin = 0;
		double ymax = 0;
		if (enabled) {
			Transaction tx = graphDb().beginTx();
			try {
				SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
				Layer layer = spatialService.getLayer("roads");
				SpatialIndexReader spatialIndex = layer.getIndex();

				Search searchQuery = new SearchIntersectWindow(new Envelope(xmin, xmax, ymin, ymax));
				spatialIndex.executeSearch(searchQuery);
				List<SpatialDatabaseRecord> results = searchQuery.getResults();
				System.out.println("Search returned: " + results);
				tx.success();
			} finally {
				tx.finish();
			}
		}
	}

}