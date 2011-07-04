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

import org.neo4j.gis.spatial.AbstractSearch;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

/**
 * @author Davide Savazzi
 */
public class SearchClosest extends AbstractSearch {

	public SearchClosest(Geometry other) {
		this(other, null);
	}

	/**
	 * Search for the closest objects within the Envelope window containing the
	 * buffer region.
	 * 
	 * @param other
	 *            Geometry to use
	 * @param buffer
	 *            around this object for creating the searchWindow envelope
	 */
	public SearchClosest(Geometry other, double buffer) {
		this(other, makeBufferEnvelope(other, buffer));
	}

	private static Envelope makeBufferEnvelope(Geometry other, double buffer) {
		return other.buffer(buffer).getEnvelopeInternal();
	}

	/**
	 * Search for the closest objects within the Envelope window.
	 * 
	 * @param other
	 *            Geometry to use
	 * @param searchWindow
	 *            Envelope within objects will be considered, or null to
	 *            consider all
	 */
	public SearchClosest(Geometry other, Envelope searchWindow) {
		this.other = other;
		this.searchWindow = searchWindow;
	}

	public boolean needsToVisit(Envelope indexNodeEnvelope) {
		return searchWindow == null || indexNodeEnvelope.intersects(searchWindow);
	}

	public final void onIndexReference(Node geomNode) {
		Envelope geomEnvelope = getEnvelope(geomNode);
		if (searchWindow == null || geomEnvelope.intersects(searchWindow)) {
			onEnvelopeIntersection(geomNode, geomEnvelope);
		}
	}

	protected void onEnvelopeIntersection(Node geomNode, Envelope geomEnvelope) {
		Geometry geometry = decode(geomNode);
		double distance = geometry.distance(other);
		if (distance < minDistance) {
			clearResults();
			minDistance = distance;
			add(geomNode, geometry);
		} else if (distance == minDistance) {
			add(geomNode, geometry);
		}
	}

	protected Geometry other;
	protected Envelope searchWindow;
	protected double minDistance = Double.MAX_VALUE;
}