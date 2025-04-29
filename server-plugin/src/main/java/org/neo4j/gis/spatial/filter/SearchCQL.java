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
package org.neo4j.gis.spatial.filter;

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.data.neo4j.Neo4jFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.Utilities;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * Find geometries that have at least one point in common with the given
 * geometry
 */
public class SearchCQL implements SearchFilter {

	private final Neo4jFeatureBuilder featureBuilder;
	private final Layer layer;
	private final org.geotools.api.filter.Filter filter;
	private final Envelope filterEnvelope;

	public SearchCQL(Transaction tx, Layer layer, org.geotools.api.filter.Filter filter) {
		this.layer = layer;
		this.featureBuilder = Neo4jFeatureBuilder.fromLayer(tx, layer);
		this.filter = filter;
		this.filterEnvelope = Utilities.extractEnvelopeFromFilter(filter);
	}

	public SearchCQL(Transaction tx, Layer layer, String cql) {
		this.layer = layer;
		this.featureBuilder = Neo4jFeatureBuilder.fromLayer(tx, layer);
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
	public boolean geometryMatches(Transaction tx, Node geomNode) {
		SimpleFeature feature = featureBuilder.buildFeature(tx, new SpatialDatabaseRecord(this.layer, geomNode));
		return filter.evaluate(feature);
	}

}
