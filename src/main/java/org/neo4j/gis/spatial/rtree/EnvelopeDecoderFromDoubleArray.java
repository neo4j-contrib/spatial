/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.rtree;

import org.neo4j.graphdb.PropertyContainer;


/**
 * 
 * The property must contain an array of double: xmin, ymin, xmax, ymax.
 */
public class EnvelopeDecoderFromDoubleArray implements EnvelopeDecoder {

	public EnvelopeDecoderFromDoubleArray(String propertyName) {
		this.propertyName = propertyName;
	}
	
	@Override	
	public Envelope decodeEnvelope(PropertyContainer container) {
	    Object propValue = container.getProperty(propertyName);
	    
	    if (propValue instanceof Double[]) {
	    	Double[] bbox = (Double[]) propValue;
			return new Envelope(bbox[0], bbox[2], bbox[1], bbox[3]);
		} else if (propValue instanceof double[]) {
			double[] bbox = (double[]) propValue;
			return new Envelope(bbox[0], bbox[2], bbox[1], bbox[3]);
	    } else {
	    	// invalid content
	    	return new Envelope();
	    }
	}

	private String propertyName;
}