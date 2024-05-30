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

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.PrintStream;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.rtree.ProgressLoggingListener;

public class ProgressLoggingListenerTest {

	@Test
	public void testProgressLoggingListnerWithAllLogs() {
		int unitsOfWork = 10;
		long timeWait = 10;
		long throttle = 20;
		testProgressLoggingListenerWithSpecifiedWaits(unitsOfWork, timeWait, throttle, unitsOfWork + 2);
	}

	@Test
	public void testProgressLoggingListnerWithOnlyStartAndEnd() {
		int unitsOfWork = 10;
		long timeWait = 1000;
		long throttle = 10;
		testProgressLoggingListenerWithSpecifiedWaits(unitsOfWork, timeWait, throttle, 3);
	}

	private static void testProgressLoggingListenerWithSpecifiedWaits(int unitsOfWork, long timeWait, long throttle,
			int expectedLogCount) {
		// When running maven-surefire System.out is replaced with a PrintStream that mockito cannot spy on, so we need to wrap it here
		PrintStream wrapped = new PrintStream(System.out);
		PrintStream out = spy(wrapped);
		Listener listener = new ProgressLoggingListener("test", out).setTimeWait(timeWait);
		listener.begin(unitsOfWork);
		for (int step = 0; step < unitsOfWork; step++) {
			listener.worked(1);
			try {
				Thread.sleep(throttle);
			} catch (InterruptedException e) {
			}
		}
		listener.done();
		verify(out).println("Starting test");
		//noinspection RedundantStringFormatCall
		verify(out).println(String.format("%.2f (10/10) - Completed test", 100f));
		verify(out, times(expectedLogCount)).println(Mockito.anyString());
	}
}
