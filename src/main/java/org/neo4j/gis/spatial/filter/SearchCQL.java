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
package org.neo4j.gis.spatial.filter;

import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.geotools.data.Neo4jFeatureBuilder;
import org.neo4j.graphdb.Node;

import org.neo4j.collections.rtree.Envelope;
import org.neo4j.collections.rtree.filter.SearchAll;
import org.neo4j.collections.rtree.filter.SearchFilter;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Find geometries that have at least one point in common with the given
 * geometry
 * 
 * @author Davide Savazzi
 * @author Craig Taverner
 */
public class SearchCQL implements SearchFilter {
	private Neo4jFeatureBuilder featureBuilder;
	private Layer layer;
	private org.opengis.filter.Filter filter;

	public SearchCQL(Layer layer, String cql) {
		this.layer = layer;
		this.featureBuilder = new Neo4jFeatureBuilder(layer);
		try {
			filter = ECQL.toFilter(cql);
		} catch (CQLException e) {
			throw new SpatialDatabaseException("CQLException: " + e.getMessage());
		}
	}

	@Override
	public boolean needsToVisit(Envelope envelope) {
		return true;
	}

	@Override
	public boolean geometryMatches(Node geomNode) {
		SimpleFeature feature = featureBuilder.buildFeature(new SpatialDatabaseRecord(this.layer, geomNode));
		return filter.evaluate(feature);
	}

}