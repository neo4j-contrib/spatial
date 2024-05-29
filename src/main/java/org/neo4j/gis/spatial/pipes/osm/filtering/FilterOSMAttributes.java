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
package org.neo4j.gis.spatial.pipes.osm.filtering;

import org.neo4j.gis.spatial.osm.OSMRelation;
import org.neo4j.gis.spatial.pipes.AbstractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.impl.FilterPipe;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;


public class FilterOSMAttributes extends AbstractGeoPipe {

	private final String key;
	private final Object value;
	private final FilterPipe.Filter comparison;

	public FilterOSMAttributes(String key, Object value) {
		this(key, value, FilterPipe.Filter.EQUAL);
	}

	public FilterOSMAttributes(String key, Object value, FilterPipe.Filter comparison) {
		this.key = key;
		this.value = value;
		this.comparison = comparison;
	}

	@Override
	protected GeoPipeFlow process(GeoPipeFlow flow) {
		Node geomNode = flow.getRecord().getGeomNode();
		Node waysNode = geomNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode();
		Node tagNode = waysNode.getSingleRelationship(OSMRelation.TAGS, Direction.OUTGOING).getEndNode();
		if (tagNode.hasProperty(key)
				&& comparison.compare(tagNode.getProperty(key), value)) {
			return flow;
		}
		return null;
	}
}
