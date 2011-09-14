/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.gis.spatial.pipes.processing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.neo4j.gis.spatial.Layer;

import com.tinkerpop.pipes.AbstractPipe;
import com.vividsolutions.jts.geom.Geometry;

public class ToDensityIslands extends AbstractPipe {

	private double density = 20;
	private List<Geometry> islands = new ArrayList<Geometry>();
	private boolean hasReturned;
	private Iterator<Geometry> islandsIterator;

	/**
	 * 
	 * @param density
	 */
	public ToDensityIslands(Layer layer, double density) {
		super();
		this.density = density;
	}

	public Geometry processNextStart() {

		while (true) {

			NEXT: while (this.starts.hasNext()) {

				final Geometry geometry = (Geometry) this.starts.next();

				// TODO: Cleaner solution.
				if (islands.size() == 0) {
					islands.add(geometry);
					continue NEXT;
				}

				for (int i = 0; i < islands.size(); i++) {

					// Determine if geometry is next to a islands else add
					// geometry as a new islands.
					if (!this.islands.isEmpty()
							&& geometry.distance(this.islands.get(i)) <= density) {

						Geometry islandsGeom = this.islands.get(i);
						// TODO: test it with points
						this.islands.set(i, islandsGeom.union(geometry));
						continue NEXT;
					} else {
						islands.add(geometry);
						continue NEXT;
					}
				}
			}

			// TODO: Do a cleaner break!
			if (hasReturned) {
				throw new NoSuchElementException();
			}
			this.hasReturned = true;

			islandsIterator = islands.iterator();

			return islandsIterator.next();

		}

	}

}
