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
import java.util.List;
import java.util.NoSuchElementException;

import org.neo4j.gis.spatial.SpatialDatabaseRecord;

import com.tinkerpop.pipes.AbstractPipe;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;

public class ToDensityIslands<S, E> extends
		AbstractPipe<SpatialDatabaseRecord, Geometry> {

	private double density = 20;
	private List<Geometry> islands = new ArrayList<Geometry>();
	private boolean hasReturned;

	/**
	 * 
	 * @param density
	 */
	public ToDensityIslands(double density) {
		this.density = density;
	}

	public Geometry processNextStart() {

		while (true) {

			NEXT: while (this.starts.hasNext()) {

				final SpatialDatabaseRecord record = this.starts.next();
				Geometry geometry = record.getGeometry();

				// TODO: Cleaner solution.
				if (islands.size() == 0) {
					islands.add(geometry);
					continue NEXT;
				}

				for (int i = 0; i < islands.size(); i++) {
					System.out.println("break it");
					if (!this.islands.isEmpty()
							&& geometry.distance(this.islands.get(i)) <= density) {
						System.out.println("add to island");
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

	
			GeometryCollection collect = new GeometryCollection(
					(Geometry[]) islands.toArray(new Geometry[0]),
					new GeometryFactory());
	
			
			//TODO: Do a cleaner break!
			if(hasReturned) {
				throw new NoSuchElementException();
			}
			this.hasReturned = true;
			return collect;

		}
		
	}

}
