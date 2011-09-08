package org.neo4j.gis.spatial.pipes.filter;

import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.neo4j.collections.rtree.Envelope;
import org.neo4j.gis.spatial.AbstractLayerSearch;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.geotools.data.Neo4jFeatureBuilder;
import org.neo4j.graphdb.Node;
import org.opengis.feature.simple.SimpleFeature;

public class SearchCQL extends AbstractLayerSearch {
	
	private Neo4jFeatureBuilder featureBuilder;
	private String cqlPredicate;
	private Layer layer;

	public SearchCQL(Layer layer, String cqlPredicate) {
		this.cqlPredicate = cqlPredicate;
		this.layer = layer;
		this.featureBuilder = new Neo4jFeatureBuilder(layer);
	}

	public boolean needsToVisit(Envelope indexNodeEnvelope) {
		return true;
	}

	public void onIndexReference(Node geomNode) {
		
		try {
			org.opengis.filter.Filter filter = ECQL.toFilter(cqlPredicate);
			 SimpleFeature feature = featureBuilder.buildFeature(new SpatialDatabaseRecord(this.layer, geomNode));
	         if(filter.evaluate(feature)) {
	        	 add(geomNode);
	         }
			
		} catch (CQLException e) {
			throw new SpatialDatabaseException("CQLException: " + e.getMessage());
		}
		
		
	}

}
