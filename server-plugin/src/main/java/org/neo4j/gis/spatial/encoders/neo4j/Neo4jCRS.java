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
package org.neo4j.gis.spatial.encoders.neo4j;

import org.neo4j.gis.spatial.Constants;
import org.neo4j.values.storable.CoordinateReferenceSystem;

public class Neo4jCRS implements org.neo4j.graphdb.spatial.CRS {

	protected final CoordinateReferenceSystem crs;

	public Neo4jCRS(CoordinateReferenceSystem crs) {
		this.crs = crs;
	}

	@Override
	public int getCode() {
		return crs.getCode();
	}

	@Override
	public String getType() {
		return crs.getType();
	}

	@Override
	public String getHref() {
		return crs.getHref();
	}

	public int dimensions() {
		return crs.getDimension();
	}

	public static Neo4jCRS findCRS(String crs) {
		return switch (crs) {      // name in Neo4j CRS table
			case "WGS-84", "WGS84(DD)" ->   // name in geotools crs library
					makeCRS(Constants.SRID_COORDINATES_2D);
			case "Cartesian" -> makeCRS(7203);
			default -> throw new IllegalArgumentException("Cypher type system does not support CRS: " + crs);
		};
	}

	public static Neo4jCRS makeCRS(final int code) {
		return new Neo4jCRS(CoordinateReferenceSystem.get(code));
	}
}
