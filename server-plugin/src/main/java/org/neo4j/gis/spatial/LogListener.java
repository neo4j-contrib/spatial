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

import java.util.logging.Logger;
import org.neo4j.gis.spatial.rtree.Listener;

public class LogListener implements Listener {

	private final Logger logger;
	private int total = 0;
	private int current = 0;

	public LogListener(Logger logger) {
		this.logger = logger;
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
			logger.info("Completed " + current);
		} else if (total == 100) {
			logger.info(current + "%: completed");
		} else {
			int perc = (int) (100.0 * current / total);
			logger.fine(perc + "%: completed " + current + " / " + total);
		}
	}

	@Override
	public void done() {
	}

}
