/*
 * Copyright (c) 2010-2017 "Neo Technology,"
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.index;

import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.filter.SearchRecords;

/**
 * The RTreeIndex is the first and still standard index for Neo4j Spatial. It
 * implements both SpatialIndexReader and SpatialIndexWriter for read and write
 * support. In addition it implements SpatialTreeIndex which allows it to be
 * wrapped with modifying search functions to that custom classes can be used to
 * perform filtering searches on the tree.
 */
public class LayerRTreeIndex extends RTreeIndex implements LayerTreeIndexReader, Constants {

    private Layer layer;

    public void init(Layer layer) {
        init(layer, 100);
    }

    public void init(Layer layer, int maxNodeReferences) {
        super.init(layer.getSpatialDatabase().getDatabase(), layer.getLayerNode(), layer.getGeometryEncoder(), maxNodeReferences);
        this.layer = layer;
    }

    @Override
    public Layer getLayer() {
        return layer;
    }

    @Override
    public SearchRecords search(SearchFilter filter) {
        return new SearchRecords(layer, searchIndex(filter));
    }

}
