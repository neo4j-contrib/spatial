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
package org.neo4j.gis.spatial.pipes.processing;

import org.neo4j.gis.spatial.pipes.AbstractGroupGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;


public class DensityIslands extends AbstractGroupGeoPipe {

	private final double density;

	/**
	 * @param density maximum distance between vertices
	 */
	public DensityIslands(double density) {
		this.density = density;
	}

	@Override
	protected void group(GeoPipeFlow pipeFlow) {
		boolean islandFound = false;
		// determine if geometry is next to an islands else add
		// geometry as a new islands.
		for (int i = 0; i < groups.size() && !islandFound; i++) {
			if (pipeFlow.getGeometry().distance(groups.get(i).getGeometry()) <= density) {
				// TODO test it with points
				groups.get(i).setGeometry(groups.get(i).getGeometry().union(pipeFlow.getGeometry()));
				groups.get(i).merge(pipeFlow);
				islandFound = true;
			}
		}

		if (!islandFound) {
			groups.add(pipeFlow);
		}
	}
}
