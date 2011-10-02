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
package org.neo4j.gis.spatial.pipes.filtering;

import org.neo4j.gis.spatial.pipes.AbstractFilterGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;

import com.tinkerpop.pipes.filter.FilterPipe;
import com.tinkerpop.pipes.util.PipeHelper;


public class FilterProperty extends AbstractFilterGeoPipe {

	private String key;
	private Object value;
	private FilterPipe.Filter comparison;
	
	public FilterProperty(String key, Object value) {
		this(key, value, FilterPipe.Filter.EQUAL);
	}	
	
	public FilterProperty(String key, Object value, FilterPipe.Filter comparison) {
		this.key = key;
		this.value = value;
		this.comparison = comparison;
	}

	@Override
	protected boolean validate(GeoPipeFlow flow) {
		return PipeHelper.compareObjects(comparison, flow.getProperties().get(key), value);
	}
}