package org.neo4j.gis.spatial;

import org.neo4j.graphdb.Node;

/**
 * Spatial Indexes based on tree structures can implement the following methods common to maintaining and searching tree structures.
 * @author craig
 */
public interface SpatialTreeIndex extends SpatialIndexReader {
	Node getIndexRoot();
	void visit(SpatialIndexVisitor visitor, Node indexNode);
}
