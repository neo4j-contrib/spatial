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
package org.neo4j.gis.spatial.query;

import org.neo4j.gis.spatial.AbstractLayerSearch;
import org.neo4j.gis.spatial.EnvelopeUtils;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.operation.distance.DistanceOp;


/**
 * @author Davide Savazzi
 */
public class SearchWithinDistance extends AbstractLayerSearch {

	public SearchWithinDistance(Point point, double distance) {
		this.point = point;
		this.distance = distance;
		
		jtsBbox = point.getEnvelopeInternal();
		jtsBbox.expandBy(distance);
		bbox = EnvelopeUtils.fromJtsToNeo4j(jtsBbox);
	}

	public boolean needsToVisit(org.neo4j.collections.rtree.Envelope indexNodeEnvelope) {
		return indexNodeEnvelope.intersects(bbox);
	}
	
	public void onIndexReference(Node geomNode) {
	    double bboxDistance = getEnvelope(geomNode).distance(bbox);
	    if (bboxDistance <= distance) {
	    	Geometry geometry = decode(geomNode);
	    	if (DistanceOp.isWithinDistance(geometry, point, distance)) add(geomNode, geometry);
	    }
	}
	

	private Point point;
	private double distance;
	private org.neo4j.collections.rtree.Envelope bbox;
	private com.vividsolutions.jts.geom.Envelope jtsBbox;
}