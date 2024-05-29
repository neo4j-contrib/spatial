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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.neo4j.gis.spatial.pipes.AbstractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;

/**
 * Sort items in the pipeline comparing values of the given property.
 */
public class Sort extends AbstractGeoPipe {

	private final List<GeoPipeFlow> sortedFlow;
	private final Comparator<GeoPipeFlow> comparator;
	private Iterator<GeoPipeFlow> flowIterator;

	public Sort(final String property, final Comparator<Object> propertyComparator) {
		this.sortedFlow = new ArrayList<>();
		this.comparator = (o1, o2) -> {
			Object p1 = o1.getProperties().get(property);
			Object p2 = o2.getProperties().get(property);

			if (p1 == null && p2 == null) {
				return 0;
			}
			if (p1 == null) {
				return -1;
			}
			if (p2 == null) {
				return 1;
			}
			return propertyComparator.compare(p1, p2);
		};
	}

	public Sort(String property, final boolean asc) {
		this(property, (o1, o2) -> {
			if (!(o1 instanceof Comparable<?> comparable)) {
				throw new IllegalArgumentException("Property value is not comparable");
			}
			//noinspection unchecked
			int result = ((Comparable<Object>) comparable).compareTo(o2);
			if (!asc) {
				result *= -1;
			}
			return result;
		});
	}

	@Override
	public GeoPipeFlow processNextStart() {
		if (flowIterator == null) {
			while (true) {
				try {
					sortedFlow.add(starts.next());
				} catch (NoSuchElementException e) {
					break;
				}
			}

			sortedFlow.sort(comparator);
			flowIterator = sortedFlow.iterator();
		}

		return flowIterator.next();
	}

}
