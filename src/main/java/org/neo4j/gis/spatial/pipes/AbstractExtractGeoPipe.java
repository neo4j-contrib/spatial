/**
 * Copyright (c) 2010-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.pipes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Abstract pipe for GeoPipelines that extract or create new items from every given item.
 */
public class AbstractExtractGeoPipe extends AbstractGeoPipe {

	protected List<GeoPipeFlow> extracts = new ArrayList<GeoPipeFlow>();
	protected Iterator<GeoPipeFlow> extractIterator = null;
	
	@Override
	public GeoPipeFlow processNextStart() {
		if (extractIterator != null) {
			if (extractIterator.hasNext()) {
				return extractIterator.next();
			} else {
				extractIterator = null;
				extracts.clear();
			}
		}
		
		do {
			extract(process(starts.next()));
		} while (extracts.size() == 0);

		extractIterator = extracts.iterator();
		return extractIterator.next();
	}
	
	/**
	 * Subclasses should override this method
	 */
	protected void extract(GeoPipeFlow flow) {
		extracts.add(flow);
	}
}