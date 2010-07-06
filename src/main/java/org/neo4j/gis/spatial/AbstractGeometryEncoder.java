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
package org.neo4j.gis.spatial;

import org.neo4j.graphdb.PropertyContainer;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;


/**
 * @author Davide Savazzi
 */
public abstract class AbstractGeometryEncoder implements GeometryEncoder, Constants {


	// Public methods
	
	public void init(Layer layer) {
		this.layer = layer;
	}
	
	public void encodeGeometry(Geometry geometry, PropertyContainer container) {
		container.setProperty(PROP_TYPE, encodeGeometryType(geometry.getGeometryType()));

        Envelope mbb = geometry.getEnvelopeInternal();
        container.setProperty(PROP_BBOX, new double[] {mbb.getMinX(), mbb.getMinY(), mbb.getMaxX(), mbb.getMaxY()});

        encodeGeometryShape(geometry, container);
	}

	public Envelope decodeEnvelope(PropertyContainer container) {
		double[] bbox = (double[]) container.getProperty(PROP_BBOX);
		
		// Envelope parameters: xmin, xmax, ymin, ymax)
		return new Envelope(bbox[0], bbox[2], bbox[1], bbox[3]);
	}

	
	// Protected methods
	
	protected abstract void encodeGeometryShape(Geometry geometry, PropertyContainer container);
	
	protected Integer encodeGeometryType(String jtsGeometryType) {
        // TODO: Consider alternatives for specifying type, like relationship to type category
        // objects (or similar indexing structure)		
		if ("Point".equals(jtsGeometryType)) {
			return GTYPE_POINT;
		} else if ("MultiPoint".equals(jtsGeometryType)) {
			return GTYPE_MULTIPOINT;
		} else if ("LineString".equals(jtsGeometryType)) {
			return GTYPE_LINESTRING;
		} else if ("MultiLineString".equals(jtsGeometryType)) {
			return GTYPE_MULTILINESTRING;
		} else if ("Polygon".equals(jtsGeometryType)) {
			return GTYPE_POLYGON;
		} else if ("MultiPolygon".equals(jtsGeometryType)) {
			return GTYPE_MULTIPOLYGON;
		} else {
			throw new IllegalArgumentException("unknown type:" + jtsGeometryType);
		}
	}
	
	
	// Attributes
	
	protected Layer layer;
}