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

import java.util.Comparator;
import org.neo4j.gis.spatial.pipes.AbstractGroupGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;

/**
 * Computes the minimum value of the specified property and
 * discard items with a value greater than the minimum.
 */
public class Min extends AbstractGroupGeoPipe {

	private final String property;
	private Comparator<Object> comparator;

	public Min(String property, Comparator<Object> comparator) {
		this.property = property;
		this.comparator = comparator;
	}

	public Min(String property) {
		this.property = property;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Override
	protected void group(GeoPipeFlow flow) {
		if (flow.getProperties().get(property) == null) {
			return;
		}

		if (groups.isEmpty()) {
			groups.add(flow);
		} else {
			Object min = groups.get(0).getProperties().get(property);
			Object other = flow.getProperties().get(property);

			int comparison;
			if (comparator == null) {
				comparison = ((Comparable) other).compareTo(min);
			} else {
				comparison = comparator.compare(other, min);
			}

			if (comparison < 0) {
				// new min value
				groups.clear();
				groups.add(flow);
			} else if (comparison == 0) {
				groups.add(flow);
			}
		}
	}
}
