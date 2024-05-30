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

/**
 * Unites geometries of every item contained in the pipeline.
 * This pipe groups every item in the pipeline in a single item containing the geometry output
 * of the union.
 */
public class UnionAll extends AbstractGroupGeoPipe {

	@Override
	protected void group(GeoPipeFlow flow) {
		if (groups.isEmpty()) {
			groups.add(flow);
		} else {
			GeoPipeFlow result = groups.get(0);
			result.setGeometry(result.getGeometry().union(flow.getGeometry()));
			result.merge(flow);
		}
	}

}
