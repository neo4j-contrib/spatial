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

import java.io.PrintStream;
import org.neo4j.gis.spatial.rtree.Listener;


/**
 * This listener simply logs progress to System.out.
 *
 * @author Craig Taverner
 */
public class ConsoleListener implements Listener {

	private final PrintStream out;
	private int total = 0;
	private int current = 0;

	public ConsoleListener() {
		this(System.out);
	}

	public ConsoleListener(PrintStream out) {
		this.out = out;
	}

	@Override
	public void begin(int unitsOfWork) {
		total = unitsOfWork;
		current = 0;
	}

	@Override
	public void worked(int workedSinceLastNotification) {
		current += workedSinceLastNotification;
		if (total < 1) {
			out.println("Completed " + current);
		} else if (total == 100) {
			out.println(current + "%: completed");
		} else {
			int perc = (int) (100.0 * current / total);
			out.println(perc + "%: completed " + current + " / " + total);
		}
	}

	@Override
	public void done() {
	}

}
