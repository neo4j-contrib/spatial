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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class TestSpatialQueries extends Neo4jTestCase {

	/**
	 * This test case is designed to capture the conditions described in the bug
	 * report at https://github.com/neo4j/neo4j-spatial/issues/11. There are
	 * three geometries, one pPoint and two LineStrings, one short and one long.
	 * The short LineString is closer to the Point, but SearchClosest returns
	 * the long LineString.
	 */
	@Test
	public void testSearchClosestWithShortLongLineStrings() throws ParseException {
		String layerName = "test";
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		Geometry shortLineString;
		Geometry longLineString;
		Geometry point;
		try (Transaction tx = graphDb().beginTx()) {
			EditableLayer layer = spatial.getOrCreateEditableLayer(tx, layerName, "WKT", null);
			WKTReader wkt = new WKTReader(layer.getGeometryFactory());
			shortLineString = wkt.read("LINESTRING(16.3493032 48.199882,16.3479487 48.1997337)");
			longLineString = wkt.read(
					"LINESTRING(16.3178388 48.1979135,16.3195494 48.1978011,16.3220815 48.197824,16.3259696 48.1978297,16.3281211 48.1975952,16.3312482 48.1968743,16.3327931 48.1965196,16.3354641 48.1959911,16.3384376 48.1959609,16.3395792 48.1960223,16.3458708 48.1970974,16.3477719 48.1975147,16.348008 48.1975665,16.3505572 48.1984533,16.3535613 48.1994545,16.3559474 48.2011765,16.3567056 48.2025723,16.3571261 48.2038308,16.3578393 48.205176)");
			point = wkt.read("POINT(16.348243 48.199678)");
			layer.add(tx, shortLineString);
			layer.add(tx, longLineString);
			tx.commit();
		}

		// First calculate the distances explicitly
		Geometry closestGeom = null;
		double closestDistance = Double.MAX_VALUE;
		System.out.println("Calculating explicit distance to the point " + point + ":");
		for (Geometry geom : new Geometry[]{shortLineString, longLineString}) {
			double distance = point.distance(geom);
			System.out.println("\tDistance " + distance + " to " + geom);
			if (distance < closestDistance) {
				closestDistance = distance;
				closestGeom = geom;
			}
		}
		assertNotNull(closestGeom, "Expected to find a clistestGeom");
		System.out.println("Found closest: " + closestGeom);
		System.out.println();

		// Now use the SearchClosest class to perform the search for the closest
		System.out.println("Searching for geometries close to " + point);
		GeoPipeline pipeline;
		try (Transaction tx = graphDb().beginTx()) {
			Layer layer = spatial.getLayer(tx, layerName);
			pipeline = GeoPipeline.startNearestNeighborSearch(tx, layer, point.getCoordinate(), 100)
					.sort("Distance")
					.getMin("Distance");
			for (SpatialRecord result : pipeline) {
				System.out.println("\tGot search result: " + result);
				assertEquals(closestGeom.toString(), result.getGeometry().toString(), "Did not find the closest");
			}
			tx.commit();
		}

		// Repeat with an envelope
		try (Transaction tx = graphDb().beginTx()) {
			Layer layer = spatial.getLayer(tx, layerName);
			Envelope env = new Envelope(point.getCoordinate().x, point.getCoordinate().x, point.getCoordinate().y,
					point.getCoordinate().y);
			env.expandToInclude(shortLineString.getEnvelopeInternal());
			env.expandToInclude(longLineString.getEnvelopeInternal());
			pipeline = GeoPipeline.startNearestNeighborSearch(tx, layer, point.getCoordinate(), env)
					.sort("Distance")
					.getMin("Distance");
			System.out.println("Searching for geometries close to " + point + " within " + env);
			for (SpatialRecord result : pipeline) {
				System.out.println("\tGot search result: " + result);
				assertEquals(closestGeom.toString(), result.getGeometry().toString(), "Did not find the closest");
			}
			tx.commit();
		}

		// Repeat with a buffer big enough to work
		try (Transaction tx = graphDb().beginTx()) {
			Layer layer = spatial.getLayer(tx, layerName);
			double buffer = 0.0001;
			pipeline = GeoPipeline.startNearestNeighborSearch(tx, layer, point.getCoordinate(), buffer)
					.sort("Distance")
					.getMin("Distance");
			System.out.println("Searching for geometries close to " + point + " within buffer " + buffer);
			for (SpatialRecord result : pipeline) {
				System.out.println("\tGot search result: " + result);
				assertEquals(closestGeom.toString(), result.getGeometry().toString(), "Did not find the closest");
			}
			tx.commit();
		}

		// Repeat with a buffer too small to work correctly
		//TODO: Since the new Envelope class in graph-collections seems to not have the same bug as the old JTS Envelope, this test case no longer works. We should think of a new test case.
//		buffer = 0.00001;
//		closest = new SearchClosest(point, buffer);
//		System.out.println("Searching for geometries close to " + point + " within buffer " + buffer);
//		layer.getIndex().executeSearch(closest);
//		for (SpatialDatabaseRecord result : closest.getExtendedResults()) {
//			System.out.println("\tGot search result: " + result);
//			// NOTE the test below is negative, because the buffer was badly chosen
//			assertThat("Unexpectedly found the closest", result.getGeometry().toString(), is(not(closestGeom.toString())));
//		}

		// Repeat with the new limit API
		try (Transaction tx = graphDb().beginTx()) {
			Layer layer = spatial.getLayer(tx, layerName);
			int limit = 10;
			pipeline = GeoPipeline.startNearestNeighborSearch(tx, layer, point.getCoordinate(), limit)
					.sort("Distance")
					.getMin("Distance");
			System.out.println(
					"Searching for geometries close to " + point + " within automatic window designed to get about "
							+ limit + " geometries");
			for (SpatialRecord result : pipeline) {
				System.out.println("\tGot search result: " + result);
				MatcherAssert.assertThat("Did not find the closest", result.getGeometry().toString(),
						is(closestGeom.toString()));
			}
			tx.commit();
		}
	}
}
