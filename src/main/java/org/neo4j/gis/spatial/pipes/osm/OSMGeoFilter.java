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
package org.neo4j.gis.spatial.pipes.osm;

import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.osm.OSMRelation;
import org.neo4j.gis.spatial.pipes.GeoFilter;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import com.tinkerpop.pipes.filter.FilterPipe;
import com.tinkerpop.pipes.util.PipeHelper;

public class OSMGeoFilter extends GeoFilter {

	public OSMGeoFilter(Layer layer) {
		super(layer);
	}

	public GeoFilter attributes(final String key, final String value, final FilterPipe.Filter filter) {
		return this.add(new GeometryFilter() {

			@Override
			public boolean geometryMatches(Node geomNode) {
				Node waysNode = geomNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode();
				Node tagNode = waysNode.getSingleRelationship(OSMRelation.TAGS, Direction.OUTGOING).getEndNode();
				return tagNode.hasProperty(key) && PipeHelper.compareObjects(filter, tagNode.getProperty(key), value);
			}
		});
	}

}
