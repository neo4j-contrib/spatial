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
package org.neo4j.gis.spatial.pipes;

import java.util.NoSuchElementException;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.pipes.impl.AbstractPipe;

/**
 * Abstract pipe for GeoPipelines.
 * Filters, processes, extracts or groups GeoPipeFlow objects.
 */
public abstract class AbstractGeoPipe extends AbstractPipe<GeoPipeFlow, GeoPipeFlow> {

	protected String resultPropertyName = null;

	protected AbstractGeoPipe() {
	}

	/**
	 * @param resultPropertyName name to use for the property containing Pipe output
	 */
	protected AbstractGeoPipe(String resultPropertyName) {
		this();
		this.resultPropertyName = resultPropertyName;
	}

	@Override
	protected GeoPipeFlow processNextStart() throws NoSuchElementException {
		GeoPipeFlow flow;
		do {
			flow = process(starts.next());
		} while (flow == null);

		return flow;
	}

	/**
	 * Subclasses should override this method.
	 */
	protected GeoPipeFlow process(GeoPipeFlow flow) {
		return flow;
	}

	/**
	 * Puts pipe geometry output in the given GeoPipeFlow.
	 */
	protected void setGeometry(GeoPipeFlow flow, Geometry geometry) {
		if (resultPropertyName != null) {
			flow.getProperties().put(resultPropertyName, geometry);
		} else {
			flow.setGeometry(geometry);
		}
	}

	/**
	 * Puts pipe output in the given GeoPipeFlow.
	 */
	protected void setProperty(GeoPipeFlow flow, Object result) {
		if (resultPropertyName != null) {
			flow.getProperties().put(resultPropertyName, result);
		} else {
			flow.getProperties().put(generatePropertyName(), result);
		}
	}

	/**
	 * Creates a default property name, used if no name has been specified.
	 */
	protected String generatePropertyName() {
		String className = getClass().getName();
		if (className.contains(".")) {
			className = className.substring(className.lastIndexOf(".") + 1);
		}

		resultPropertyName = className;
		return resultPropertyName;
	}
}
