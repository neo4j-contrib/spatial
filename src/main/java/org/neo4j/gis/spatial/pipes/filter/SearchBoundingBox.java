package org.neo4j.gis.spatial.pipes.filter;

import org.neo4j.collections.rtree.Envelope;
import org.neo4j.gis.spatial.AbstractLayerSearch;
import org.neo4j.graphdb.Node;

import com.tinkerpop.pipes.filter.FilterPipe.Filter;
import com.tinkerpop.pipes.util.PipeHelper;

public class SearchBoundingBox extends AbstractLayerSearch {
	
	private double minLon;
	private double maxLon;
	private double maxLat;
	private double minLat;
	
	public SearchBoundingBox(double minLon, double minLat, double maxLon,
			double maxLat) {
		this.minLon = minLon;
		this.minLat = minLat;
		this.maxLon = maxLon;
		this.maxLat = maxLat;
	}

	public boolean needsToVisit(Envelope indexNodeEnvelope) {
		return true;
	}

	public void onIndexReference(Node geomNode) {

		Object bboxObj = geomNode.getProperty("bbox");
		if(bboxObj instanceof double[]) {
			double[] geomNodeBbox = (double[]) bboxObj;
			if (geomNode.hasProperty("bbox")
					&& PipeHelper.compareObjects(Filter.GREATER_THAN_EQUAL, geomNodeBbox[0] , minLon)
					&& PipeHelper.compareObjects(Filter.GREATER_THAN_EQUAL, geomNodeBbox[2] , minLat)
					&& PipeHelper.compareObjects(Filter.LESS_THAN_EQUAL, geomNodeBbox[1] , maxLon)
					&& PipeHelper.compareObjects(Filter.LESS_THAN_EQUAL, geomNodeBbox[3] , maxLat)) {
				add(geomNode);
			}
		}
		
		
		
	}

}
