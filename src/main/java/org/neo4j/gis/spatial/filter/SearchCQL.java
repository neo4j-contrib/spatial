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
package org.neo4j.gis.spatial.filter;

import org.geotools.data.neo4j.Neo4jFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.neo4j.collections.rtree.Envelope;
import org.neo4j.collections.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.Utilities;
import org.neo4j.graphdb.Node;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Find geometries that have at least one point in common with the given
 * geometry
 */
public class SearchCQL implements SearchFilter {
	
	private Neo4jFeatureBuilder featureBuilder;
	private Layer layer;
	private org.opengis.filter.Filter filter;
	private Envelope filterEnvelope;
	
	public SearchCQL(Layer layer, org.opengis.filter.Filter filter) {
		this.layer = layer;
		this.featureBuilder = new Neo4jFeatureBuilder(layer);
		this.filter = filter;	
	    this.filterEnvelope = Utilities.extractEnvelopeFromFilter(filter);		
	}
	
	public SearchCQL(Layer layer, String cql) {
		this.layer = layer;
		this.featureBuilder = new Neo4jFeatureBuilder(layer);
		try {
			this.filter = ECQL.toFilter(cql);
		    this.filterEnvelope = Utilities.extractEnvelopeFromFilter(filter);					
		} catch (CQLException e) {
			throw new SpatialDatabaseException("CQLException: " + e.getMessage());
		}
	}

	@Override
	public boolean needsToVisit(Envelope envelope) {
        return filterEnvelope == null || filterEnvelope.intersects(envelope);
	}

	@Override
	public boolean geometryMatches(Node geomNode) {
		SimpleFeature feature = featureBuilder.buildFeature(new SpatialDatabaseRecord(this.layer, geomNode));
		return filter.evaluate(feature);
	}

}