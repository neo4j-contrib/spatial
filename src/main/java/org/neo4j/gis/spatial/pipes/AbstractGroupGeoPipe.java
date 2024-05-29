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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public abstract class AbstractGroupGeoPipe extends AbstractGeoPipe {

	protected final List<GeoPipeFlow> groups = new ArrayList<>();
	protected Iterator<GeoPipeFlow> groupIterator = null;

	@Override
	public GeoPipeFlow processNextStart() {
		if (groupIterator == null) {
			while (true) {
				try {
					group(starts.next());
				} catch (NoSuchElementException e) {
					break;
				}
			}
			groupIterator = groups.iterator();
		}

		return groupIterator.next();
	}

	/**
	 * Subclasses should override this method
	 */
	protected void group(GeoPipeFlow flow) {
		groups.add(flow);
	}
}
