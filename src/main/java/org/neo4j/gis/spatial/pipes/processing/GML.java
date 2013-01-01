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

import com.vividsolutions.jts.io.gml2.GMLWriter;


/**
 * Encodes item geometry to GML.
 */
public class GML extends AbstractGeoPipe {

	public GML() {
	}		
	
	/**
	 * @param resultPropertyName property name to use for geometry output
	 */	
	public GML(String resultPropertyName) {
		super(resultPropertyName);
	}	

	@Override	
	protected GeoPipeFlow process(GeoPipeFlow flow) {
		GMLWriter gmlWriter = new GMLWriter();
		setProperty(flow, gmlWriter.write(flow.getGeometry()));		
		return flow;
	}	
	
}