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
package org.neo4j.gis.spatial.pipes;

import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;
import com.vividsolutions.jts.geom.Geometry;

public abstract class AbstractGeoPipe extends AbstractPipe<GeoPipeFlow, GeoPipeFlow> {

	protected String resultPropertyName = null;
	
	protected AbstractGeoPipe() {
	}
	
	protected AbstractGeoPipe(String resultPropertyName) {
		this.resultPropertyName = resultPropertyName;
	}
	
	@Override
	protected GeoPipeFlow processNextStart() throws NoSuchElementException {
		GeoPipeFlow flow = null;
		do {
			flow = process(starts.next());
		} while (flow == null);
		
		return flow;
	}
	
	/**
	 * Subclasses should override this method
	 */
	protected GeoPipeFlow process(GeoPipeFlow flow) {
		return flow;
	}

	protected void setGeometry(GeoPipeFlow flow, Geometry geometry) {
		if (resultPropertyName != null) {
			flow.getProperties().put(resultPropertyName, geometry);
		} else {
			flow.setGeometry(geometry);
		}
	}
	
	protected void setProperty(GeoPipeFlow flow, Object result) {
		if (resultPropertyName != null) {
			flow.getProperties().put(resultPropertyName, result);
		} else {
			flow.getProperties().put(generatePropertyName(), result);
		}		
	}
	
	protected String generatePropertyName() {
		String className = getClass().getName();
		if (className.indexOf(".") != -1) {
			className = className.substring(className.lastIndexOf(".") + 1);
		}
		
		resultPropertyName = className;
		return resultPropertyName;
	}
}
