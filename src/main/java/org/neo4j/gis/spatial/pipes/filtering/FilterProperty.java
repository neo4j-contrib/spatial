/*
 * Copyright (c) 2010-2020 "Neo4j,"
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
package org.neo4j.gis.spatial.pipes.filtering;

import org.neo4j.gis.spatial.pipes.AbstractFilterGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.impl.FilterPipe;

/**
 * Filter by property value.
 */
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
	    final Object leftObject = flow.getProperties().get(key);
	    return switch (comparison) {
	        case EQUAL -> (leftObject == null) ? value == null : leftObject.equals(value);
	        case NOT_EQUAL -> (leftObject == null) ? value != null : !leftObject.equals(value);
	        case GREATER_THAN, LESS_THAN, GREATER_THAN_EQUAL, LESS_THAN_EQUAL -> {
	            if (leftObject == null || value == null) yield false;
	            if (!(leftObject instanceof Comparable comparable))
	                throw new IllegalArgumentException("leftObject is not Comparable");
	           
                int compareRes = comparable.compareTo(value);
                yield switch (comparison) {
                    case GREATER_THAN -> compareRes > 0;
                    case LESS_THAN -> compareRes < 0;
                    case GREATER_THAN_EQUAL -> compareRes >= 0;
                    case LESS_THAN_EQUAL -> compareRes <= 0;
                    default -> throw new AssertionError("Unexpected comparison: " + comparison);
                };
	        }
	        default -> throw new IllegalArgumentException("Invalid state as no valid filter was provided");
	    };	
	    
    }
}