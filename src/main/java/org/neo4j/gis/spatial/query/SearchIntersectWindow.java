/*
 * Copyright (c) 2010
 *
 * This program is free software: you can redistribute it and/or modify
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
import org.neo4j.gis.spatial.Layer;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;


/**
 * TODO: remove (optimize SearchIntersect)
 * 
 * @author Davide Savazzi
 */
public class SearchIntersectWindow extends AbstractSearch {

	public SearchIntersectWindow(Envelope window) {
		this.window = window;
	}
	
	public void setLayer(Layer layer) {
		super.setLayer(layer);
		this.windowGeom = layer.getGeometryFactory().toGeometry(window);		
	}
	
	public boolean needsToVisit(Node indexNode) {
		return getEnvelope(indexNode).intersects(window);
	}
	
	public final void onIndexReference(Node geomNode) {	
		Envelope geomEnvelope = getEnvelope(geomNode);
		
		if (window.covers(geomEnvelope)) {
			add(geomNode);
		} else if (window.intersects(geomEnvelope)) {
			Geometry geometry = decode(geomNode);
			if (geometry.intersects(windowGeom)) {
				add(geomNode, geometry);
			}
		}
	}	

	private Envelope window;
	private Geometry windowGeom;
}