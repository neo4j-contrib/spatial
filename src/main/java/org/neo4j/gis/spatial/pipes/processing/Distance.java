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
 * Calculates distance between the given geometry and item geometry for each item in the pipeline.
 */
public class Distance extends AbstractGeoPipe {

	private Geometry reference;
	
	public Distance(Geometry reference) {
		this.reference = reference;
	}		

	/**
	 * @param resultPropertyName property name to use for geometry output
	 */	
	public Distance(Geometry reference, String resultPropertyName) {
		super(resultPropertyName);
		this.reference = reference;
	}	

	@Override	
	protected GeoPipeFlow process(GeoPipeFlow flow) {
		setProperty(flow, flow.getGeometry().distance(reference));
		return flow;
	}	
	
}