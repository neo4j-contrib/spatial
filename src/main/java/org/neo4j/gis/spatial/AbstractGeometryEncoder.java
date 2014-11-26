/**
 * Copyright (c) 2010-2013 "Neo Technology,"
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
package org.neo4j.gis.spatial;

import org.apache.commons.lang.ArrayUtils;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;

import com.vividsolutions.jts.geom.Geometry;


/**
 * @author Davide Savazzi
 */
public abstract class AbstractGeometryEncoder implements GeometryEncoder, Constants {

	protected String bboxProperty = PROP_BBOX;

	// Public methods

	@Override	
	public void init(Layer layer) {
		this.layer = layer;
	}

	public void encodeEnvelope(Envelope mbb, PropertyContainer container) {
		container.setProperty(bboxProperty, new double[] { mbb.getMinX(), mbb.getMinY(), mbb.getMaxX(), mbb.getMaxY() });
	}

	@Override	
	public void encodeGeometry(Geometry geometry, PropertyContainer container) {
		container.setProperty(PROP_TYPE, encodeGeometryType(geometry.getGeometryType()));

		encodeEnvelope(Utilities.fromJtsToNeo4j(geometry.getEnvelopeInternal()), container);

		encodeGeometryShape(geometry, container);
	}

	@Override
	public Envelope decodeEnvelope(PropertyContainer container) {
	    double[] bbox = new double[] { 0,0,0,0 };
	    Object bboxProp = container.getProperty(bboxProperty);
		if (bboxProp instanceof Double[]) {
		    bbox = ArrayUtils.toPrimitive((Double[]) bboxProp);
		} else if (bboxProp instanceof double[]) {
	        bbox = (double[]) bboxProp;
	    }
		
		// Envelope parameters: xmin, xmax, ymin, ymax
		return new Envelope(bbox[0], bbox[2], bbox[1], bbox[3]);
	}

	
	// Protected methods

	protected abstract void encodeGeometryShape(Geometry geometry, PropertyContainer container);

	protected Integer encodeGeometryType(String jtsGeometryType) {
		// TODO: Consider alternatives for specifying type, like relationship to
		// type category
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

	/**
	 * This method wraps the hasProperty(String) method on the geometry node.
	 * This means the default way of storing attributes is simply as properties
	 * of the geometry node. This behaviour can be changed by other domain
	 * models with different encodings.
	 * 
	 * @param geomNode
	 * @param attribute
	 *            to test
	 * @return
	 */
	public boolean hasAttribute(Node geomNode, String name) {
		return geomNode.hasProperty(name);
	}

	/**
	 * This method wraps the getProperty(String,null) method on the geometry
	 * node. This means the default way of storing attributes is simply as
	 * properties of the geometry node. This behaviour can be changed by other
	 * domain models with different encodings. If the property does not exist,
	 * the method returns null.
	 * 
	 * @param geomNode
	 * @param attribute
	 *            to test
	 * @return attribute, or null
	 */
	public Object getAttribute(Node geomNode, String name) {
		return geomNode.getProperty(name, null);
	}

	
	// Attributes

	protected Layer layer;
}