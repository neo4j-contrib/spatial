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
package org.neo4j.gis.spatial.pipes.filtering;

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.filter.Filter;
import org.geotools.data.neo4j.Neo4jFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.pipes.AbstractFilterGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.graphdb.Transaction;

/**
 * Filter geometries using a CQL query.
 */
public class FilterCQL extends AbstractFilterGeoPipe {

	private final Neo4jFeatureBuilder featureBuilder;
	private final Filter filter;
	private final Transaction tx;

	public FilterCQL(Transaction tx, Layer layer, String cqlPredicate) throws CQLException {
		this.tx = tx;
		this.featureBuilder = Neo4jFeatureBuilder.fromLayer(tx, layer);
		this.filter = ECQL.toFilter(cqlPredicate);
	}

	@Override
	protected boolean validate(GeoPipeFlow flow) {
		SimpleFeature feature = featureBuilder.buildFeature(tx, flow.getRecord());
		return filter.evaluate(feature);
	}
}
