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
package org.neo4j.gis.spatial.index;

import org.neo4j.gis.spatial.Constants;
import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;


/**
 * @author Davide Savazzi
 */
public class AbstractSpatialIndex implements Constants {

	// Private methods
	
	protected Geometry getGeometry(GeometryFactory geomFactory, Node geomRootNode) {
		Coordinate[] coordinates = getCoordinates(geomRootNode);
		
		Integer type = (Integer) geomRootNode.getProperty(PROP_TYPE);
		if (GTYPE_POINT == type) {
			return geomFactory.createPoint(coordinates[0]);
		} else if (GTYPE_MULTILINESTRING == type) {
			// TODO *Multi* not yet supported
			return geomFactory.createLineString(coordinates);
		} else if (GTYPE_POLYGON == type) {
			// TODO Polygon holes not yet supported
			return geomFactory.createPolygon(geomFactory.createLinearRing(coordinates), null);
		} else {
			// TODO
			throw new IllegalArgumentException("type not yet supported: " + type);
		}
	}
	
	private Coordinate[] getCoordinates(Node geomRootNode) {
		double[] coordinates = (double[]) geomRootNode.getProperty(PROP_ORDINATES);
		Coordinate[] result = new Coordinate[coordinates.length / 2];
		for (int i = 0; i < result.length; i++) {
			result[i] = new Coordinate(coordinates[i * 2], coordinates[i * 2 + 1]);
		}
		return result;
	}	
	
	protected Envelope getEnvelope(Node node) {
		double[] bbox = (double[]) node.getProperty(PROP_BBOX);
			
		// Envelope parameters: xmin, xmax, ymin, ymax)
		return new Envelope(bbox[0], bbox[2], bbox[1], bbox[3]);
	}

}
