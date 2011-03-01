package org.neo4j.gis.spatial;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.neo4j.gis.spatial.query.SearchPointsWithinOrthodromicDistance;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

public class SimplePointLayer extends EditableLayerImpl {
	public static final int LIMIT_RESULTS = 100;

	public List<SpatialDatabaseRecord> findClosestPointsTo(Coordinate point) {
		int count = getIndex().count();
		double scale = (double) LIMIT_RESULTS / (double) count;
		Envelope bbox = getIndex().getLayerBoundingBox();
		double width = bbox.getWidth() * scale;
		double height = bbox.getWidth() * scale;
		Envelope extent = new Envelope(point);
		extent.expandToInclude(point.x - width / 2.0, point.y - height / 2.0);
		extent.expandToInclude(point.x + width / 2.0, point.y + height / 2.0);
		SearchPointsWithinOrthodromicDistance distanceQuery = new SearchPointsWithinOrthodromicDistance(point, extent, true);
		return findClosestPoints(distanceQuery);
	}

	public List<SpatialDatabaseRecord> findClosestPointsTo(Coordinate point, double distance) {
		SearchPointsWithinOrthodromicDistance distanceQuery = new SearchPointsWithinOrthodromicDistance(point, 10.0, true);
		return findClosestPoints(distanceQuery);
	}

	private List<SpatialDatabaseRecord> findClosestPoints(SearchPointsWithinOrthodromicDistance distanceQuery) {
		getIndex().executeSearch(distanceQuery);
		List<SpatialDatabaseRecord> results = distanceQuery.getResults();
		Collections.sort(results, new Comparator<SpatialDatabaseRecord>(){

			public int compare(SpatialDatabaseRecord arg0, SpatialDatabaseRecord arg1) {
				return ((Double) arg0.getUserData()).compareTo((Double) arg1.getUserData());
			}
		});
		return results;
	}

}
