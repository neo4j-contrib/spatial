/*
 * Copyright (c) 2010-2020 "Neo4j,"
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
package org.neo4j.gis.spatial;

import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public interface SpatialRecord {

	String getId();

	Geometry getGeometry();

	boolean hasProperty(Transaction tx, String name);

	String[] getPropertyNames(Transaction tx);

	Object getProperty(Transaction tx, String name);

	Map<String, Object> getProperties(Transaction tx);

	Node getGeomNode();
}
