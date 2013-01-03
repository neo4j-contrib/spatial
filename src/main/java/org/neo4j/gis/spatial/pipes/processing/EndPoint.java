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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Find the ending point of item geometry.
 * Item geometry is replaced by pipe output unless an alternative property name is given in the constructor.
 */
public class EndPoint extends AbstractGeoPipe {
	
	private GeometryFactory geomFactory;
	
	public EndPoint(GeometryFactory geomFactory) {
		this.geomFactory = geomFactory;
	}		
	
	/**
	 * @param resultPropertyName property name to use for geometry output
	 */	
	public EndPoint(GeometryFactory geomFactory, String resultPropertyName) {
		super(resultPropertyName);
		this.geomFactory = geomFactory;
	}	

	@Override	
	protected GeoPipeFlow process(GeoPipeFlow flow) {
		Coordinate[] coords = flow.getGeometry().getCoordinates();
		setGeometry(flow, geomFactory.createPoint(coords[coords.length - 1]));
		return flow;
	}	
	
}