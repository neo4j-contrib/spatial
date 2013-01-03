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

import org.neo4j.collections.rtree.filter.AbstractSearchEnvelopeIntersection;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.Utilities;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Geometry;

/**
 * @author Craig Taverner
 */
public abstract class AbstractSearchIntersection extends AbstractSearchEnvelopeIntersection {
	
	protected Geometry referenceGeometry;
	protected Layer layer;

	public AbstractSearchIntersection(Layer layer, Geometry referenceGeometry) {
		super(layer.getGeometryEncoder(), Utilities.fromJtsToNeo4j(referenceGeometry.getEnvelopeInternal()));
		this.referenceGeometry = referenceGeometry;
		this.layer = layer;
	}

	protected Geometry decode(Node geomNode) {
		return layer.getGeometryEncoder().decodeGeometry(geomNode);
	}

}