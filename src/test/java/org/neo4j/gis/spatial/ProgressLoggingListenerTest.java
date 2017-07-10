/**
 * Copyright (c) 2010-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j.
 * <p>
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial;

import org.junit.Test;

import static org.mockito.Mockito.*;

import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.rtree.ProgressLoggingListener;

import java.io.PrintStream;
import java.util.Locale;

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

    private void testProgressLoggingListenerWithSpecifiedWaits(int unitsOfWork, long timeWait, long throttle, int expectedLogCount){
        PrintStream out = spy(System.out);
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
        verify(out).println(String.format(Locale.US, "%.2f (10/10) - Completed test", 100f));
        verify(out, times(expectedLogCount)).println(anyString());
    }
}
