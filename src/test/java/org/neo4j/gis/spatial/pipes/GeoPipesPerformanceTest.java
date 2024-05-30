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
package org.neo4j.gis.spatial.pipes;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.Neo4jTestCase;
import org.neo4j.gis.spatial.SimplePointLayer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class GeoPipesPerformanceTest extends Neo4jTestCase {

	private final int records = 10000;
	private final int chunkSize = records / 10;

	@BeforeEach
	public void setUp() throws Exception {
		super.setUp(true);
		loadSamplePointData();
	}

	private void loadSamplePointData() {
		try (Transaction tx = graphDb().beginTx()) {
			SpatialDatabaseService spatial = new SpatialDatabaseService(
					new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
			SimplePointLayer layer = spatial.createSimplePointLayer(tx, "GeoPipesPerformanceTest");
			System.out.println("Creating database of " + records + " point records");
			for (int i = 0; i < records; i++) {
				double x = 10.0 + Math.random() * 10.0;
				double y = 10.0 + Math.random() * 10.0;
				String name = "Fake Geometry " + i;
				// System.out.println("Creating point '" + name +
				// "' at location x:" + x + " y:" + y);
				SpatialDatabaseRecord record = layer.add(tx, x, y);
				record.getGeomNode().setProperty("name", name);
			}
			tx.commit();
			System.out.println("Finished writing " + records + " point records to database");
		} catch (Exception e) {
			System.err.println("Error initializing database: " + e);
		}
	}

	class TimeRecord {

		int chunk;
		int time;
		int count;

		TimeRecord(int chunk, int time, int count) {
			this.chunk = chunk;
			this.time = time;
			this.count = count;
		}

		public float average() {
			if (count > 0) {
				return (float) time / (float) count;
			}
			return 0;
		}

		@Override
		public String toString() {
			if (count > 0) {
				return chunk + ": " + average() + "ms per record (" + count + " records over " + time + "ms)";
			}
			return chunk + ": INVALID (" + count + " records over " + time + "ms)";
		}
	}

	@Test
	public void testQueryPerformance() {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		try (Transaction tx = graphDb().beginTx()) {
			Layer layer = spatial.getLayer(tx, "GeoPipesPerformanceTest");
			// String[] keys = {"id","name","address","city","state","zip"};
			String[] keys = {"id", "name"};
			Coordinate loc = new Coordinate(15.0, 15.0);
			GeoPipeline flowList = GeoPipeline.startNearestNeighborLatLonSearch(tx, layer, loc, records)
					.copyDatabaseRecordProperties(tx, keys);
			int i = 0;
			ArrayList<TimeRecord> totals = new ArrayList<TimeRecord>();
			long prevTime = System.currentTimeMillis();
			long prevChunk = 0;
			while (flowList.hasNext()) {
				GeoPipeFlow geoPipeFlow = flowList.next();
				// System.out.println("Result: " + geoPipeFlow.countRecords() +
				// " records");
				int chunk = i / chunkSize;
				if (chunk != prevChunk) {
					long time = System.currentTimeMillis();
					totals.add(new TimeRecord(chunk, (int) (time - prevTime), chunkSize));
					prevTime = time;
					prevChunk = chunk;
				}
				i++;
			}
			if (i % chunkSize > 0) {
				totals.add(new TimeRecord(totals.size(), (int) (System.currentTimeMillis() - prevTime), i % chunkSize));
			}
			int total = 0;
			int count = 0;
			System.out.println("Measured " + totals.size() + " groups of reads of up to " + chunkSize + " records");
			for (TimeRecord rec : totals) {
				total += rec.time;
				count += rec.count;
				System.out.println("\t" + rec);
				float average = (float) rec.time / (float) rec.count;
				assertTrue(rec.average() < 2 * average, "Expected record average of " + rec.average()
						+ " to not be substantially larger than running average "
						+ average);
			}
			tx.commit();
		}
	}

	@Test
	public void testPagingPerformance() {
		SpatialDatabaseService spatial = new SpatialDatabaseService(
				new IndexManager((GraphDatabaseAPI) graphDb(), SecurityContext.AUTH_DISABLED));
		try (Transaction tx = graphDb().beginTx()) {
			Layer layer = spatial.getLayer(tx, "GeoPipesPerformanceTest");
			// String[] keys = {"id","name","address","city","state","zip"};
			String[] keys = {"id", "name"};
			Coordinate loc = new Coordinate(15.0, 15.0);
			ArrayList<TimeRecord> totals = new ArrayList<TimeRecord>();
			long prevTime = System.currentTimeMillis();
			for (int chunk = 0; chunk < 20; chunk++) {
				int low = chunk * chunkSize;
				int high = (chunk + 1) * chunkSize - 1;
				GeoPipeline flowList = GeoPipeline.startNearestNeighborLatLonSearch(tx, layer, loc, records)
						.range(low, high).copyDatabaseRecordProperties(tx, keys);
				if (!flowList.hasNext()) {
					break;
				}
				int count = 0;
				while (flowList.hasNext()) {
					GeoPipeFlow geoPipeFlow = flowList.next();
					// System.out.println("Result: " + geoPipeFlow.countRecords() +
					// " records");
					count++;
				}
				flowList.reset();
				long time = System.currentTimeMillis();
				totals.add(new TimeRecord(chunk, (int) (time - prevTime), count));
				prevTime = time;
			}
			int total = 0;
			int count = 0;
			System.out.println("Measured " + totals.size() + " groups of reads of up to " + chunkSize + " records");
			for (TimeRecord rec : totals) {
				total += rec.time;
				count += rec.count;
				System.out.println("\t" + rec);
				float average = (float) rec.time / (float) rec.count;
				// assertTrue("Expected record average of " + rec.average() +
				// " to not be substantially larger than running average "
				// + average, rec.average() < 2 * average);
			}
			tx.commit();
		}
	}
}
