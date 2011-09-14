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
package org.neo4j.gis.spatial;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.collections.rtree.Envelope;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import com.vividsolutions.jts.geom.Geometry;


public abstract class AbstractLayerSearch implements LayerSearch {
	
	public AbstractLayerSearch() {
		this.results = new ArrayList<SpatialDatabaseRecord>();
	}
	
	
	// Public methods
	
	@Override
	public List<Node> getResults() {
		List<Node> r = new ArrayList<Node>(results.size());
		for (SpatialDatabaseRecord rec : results) {
			r.add(rec.getGeomNode());
		}
		return r;
	}

	@Override
	public void setLayer(Layer layer) {
		this.layer = layer;
	}	
		
	@Override
	public List<SpatialDatabaseRecord> getExtendedResults() {
		return results;
	}
	
	
	// Private methods
	
	protected Layer getLayer() {
		return layer;
	}
	
	protected void add(Node geomNode) {
		results.add(new SpatialDatabaseRecord(layer, geomNode));
	}

	protected void add(Node geomNode, Geometry geom) {
		results.add(new SpatialDatabaseRecord(layer, geomNode, geom));
	}
	
	protected void add(Node geomNode, Geometry geom, String property, Comparable<?> value) {
		SpatialDatabaseRecord result = new SpatialDatabaseRecord(layer, geomNode, geom);
		Transaction tx = geomNode.getGraphDatabase().beginTx();
		try {
			result.setProperty(property, value);
			tx.success();
		} finally {
			tx.finish();
		}
		result.setUserData(value);
		results.add(result);
	}
	
	protected Envelope getEnvelope(Node geomNode) {
		return layer.getGeometryEncoder().decodeEnvelope(geomNode);	
	}

	protected Geometry decode(Node geomNode) {
		return layer.getGeometryEncoder().decodeGeometry(geomNode);
	}
	
	protected void clearResults() {
		this.results.clear();
	}
	
	
	// Attributes

	private Layer layer;
	private List<SpatialDatabaseRecord> results;
}