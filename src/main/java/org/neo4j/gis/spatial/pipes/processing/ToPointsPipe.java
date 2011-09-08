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

import org.neo4j.gis.spatial.SpatialDatabaseRecord;

import com.tinkerpop.pipes.AbstractPipe;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class ToPointsPipe<S, E> extends
		AbstractPipe<SpatialDatabaseRecord, Point> {

	private Geometry curGeometry;
	private int curPos;

	public Point processNextStart() {
		while (true) {

			if (curGeometry == null || curPos >= curGeometry.getNumPoints()) {
				final SpatialDatabaseRecord record = this.starts.next();
				curGeometry = record.getGeometry();
				curPos = 0;
			}

			GeometryFactory geometryFactory = new GeometryFactory();
			return geometryFactory.createPoint(curGeometry.getCoordinates()[curPos++]);
		}

	}
}
