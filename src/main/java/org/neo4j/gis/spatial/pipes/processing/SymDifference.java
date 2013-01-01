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
package org.neo4j.gis.spatial.pipes.processing;

import org.neo4j.gis.spatial.pipes.AbstractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Computes the symmetric difference of the given geometry with item geometry.
 * Item geometry is replaced by pipe output unless an alternative property name is given in the constructor.
 */
public class SymDifference extends AbstractGeoPipe {
	
	private Geometry other;
	
	public SymDifference(Geometry other) {
		this.other = other;
	}		
	
	/**
	 * @param resultPropertyName property name to use for geometry output
	 */	
	public SymDifference(Geometry other, String resultPropertyName) {
		super(resultPropertyName);
		this.other = other;
	}		
	
	@Override	
	protected GeoPipeFlow process(GeoPipeFlow flow) {
		setGeometry(flow, flow.getGeometry().symDifference(other));			
		return flow;
	}
}