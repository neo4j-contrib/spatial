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
package org.neo4j.gis.spatial.index;

import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Transaction;

public interface LayerIndexReader extends SpatialIndexReader {

	/**
	 * The index used by a layer is dynamically constructed from a property of the layer node. As such it needs to be
	 * constructed with a default, no-arg constructor and then initialized with necessary parameters, such as the layer.
	 *
	 * @param indexManager for setting up index files on disk
	 * @param layer        object containing and controlling this index
	 */
	void init(Transaction tx, IndexManager indexManager, Layer layer);

	Layer getLayer();

	SearchRecords search(Transaction tx, SearchFilter filter);

}
