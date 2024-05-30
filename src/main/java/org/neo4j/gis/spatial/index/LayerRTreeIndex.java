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

import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.gis.spatial.rtree.SpatialIndexVisitor;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * The RTreeIndex is the first and still standard index for Neo4j Spatial. It
 * implements both SpatialIndexReader and SpatialIndexWriter for read and write
 * support. In addition, it implements SpatialTreeIndex which allows it to be
 * wrapped with modifying search functions to that custom classes can be used to
 * perform filtering searches on the tree.
 */
public class LayerRTreeIndex extends RTreeIndex implements LayerTreeIndexReader, Constants {

	private Layer layer;

	@Override
	public void init(Transaction tx, IndexManager indexManager, Layer layer) {
		init(tx, layer, 100);
	}

	public void init(Transaction tx, Layer layer, int maxNodeReferences) {
		super.init(tx, layer.getLayerNode(tx), layer.getGeometryEncoder(), maxNodeReferences);
		this.layer = layer;
	}

	@Override
	public void visit(Transaction tx, SpatialIndexVisitor visitor, Node indexNode) {
		super.visit(tx, visitor, indexNode);
	}

	@Override
	public Layer getLayer() {
		return layer;
	}

	@Override
	public SearchRecords search(Transaction tx, SearchFilter filter) {
		return new SearchRecords(layer, searchIndex(tx, filter));
	}

}
