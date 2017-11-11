/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.index;

import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.graphdb.Node;

import java.util.List;


public interface SpatialIndexWriter extends SpatialIndexReader {

	void add(Node geomNode);
	void add(List<Node> geomNodes);

	void remove(long geomNodeId, boolean deleteGeomNode, boolean throwExceptionIfNotFound);
	
	void removeAll(boolean deleteGeomNodes, Listener monitor);
	
	void clear(Listener monitor);
	
}
