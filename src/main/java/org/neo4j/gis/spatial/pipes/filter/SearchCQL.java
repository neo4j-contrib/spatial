/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
