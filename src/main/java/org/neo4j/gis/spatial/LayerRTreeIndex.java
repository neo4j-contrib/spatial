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
package org.neo4j.gis.spatial;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.neo4j.collections.rtree.RTreeIndex;
import org.neo4j.collections.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;


/**
 * The RTreeIndex is the first and still standard index for Neo4j Spatial. It
 * implements both SpatialIndexReader and SpatialIndexWriter for read and write
 * support. In addition it implements SpatialTreeIndex which allows it to be
 * wrapped with modifying search functions to that custom classes can be used to
 * perform filtering searches on the tree.
 */
public class LayerRTreeIndex extends RTreeIndex implements LayerTreeIndexReader, Constants {

	// Constructor
	
	public LayerRTreeIndex(GraphDatabaseService database, Layer layer) {
		this(database, layer, 100);		
	}
	
	public LayerRTreeIndex(GraphDatabaseService database, Layer layer, int maxNodeReferences) {
		super(database, layer.getLayerNode(), layer.getGeometryEncoder(), maxNodeReferences);
		
		this.layer = layer;
	}
	
	
	// Public methods
	
	@Override
	public Layer getLayer() {
		return layer;
	}
	
	@Override
	public SpatialDatabaseRecord get(Long geomNodeId) {
		Node geomNode = database.getNodeById(geomNodeId);			
		// be sure geomNode is inside this RTree
		findLeafContainingGeometryNode(geomNode, true);

		return new SpatialDatabaseRecord(layer,geomNode);
	}
	
	@Override
	public List<SpatialDatabaseRecord> get(Set<Long> geomNodeIds) {
		List<SpatialDatabaseRecord> results = new ArrayList<SpatialDatabaseRecord>();
		for (Long geomNodeId : geomNodeIds) {
			results.add(get(geomNodeId));
		}
		return results;
	}
		
	@Override
	public SearchRecords search(SearchFilter filter) {
		return new SearchRecords(layer, searchIndex(filter));
	}	
	
	
	// Attributes
	
	private GraphDatabaseService database;
	private Layer layer;
}