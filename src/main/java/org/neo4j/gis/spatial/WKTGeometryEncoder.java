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

import org.neo4j.gis.spatial.encoders.AbstractSinglePropertyEncoder;
import org.neo4j.gis.spatial.encoders.Configurable;
import org.neo4j.graphdb.PropertyContainer;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * @author Davide Savazzi
 */
public class WKTGeometryEncoder extends AbstractSinglePropertyEncoder implements Configurable{

    // Public methods
	
	public Geometry decodeGeometry(PropertyContainer container) {
		try {
			WKTReader reader = new WKTReader(layer.getGeometryFactory());
			return reader.read((String) container.getProperty(geomProperty));
		} catch (ParseException e) {
			throw new SpatialDatabaseException(e.getMessage(), e);
		}
	}
	
	// Protected methods
	
	protected void encodeGeometryShape(Geometry geometry, PropertyContainer container) {
        WKTWriter writer = new WKTWriter();
        container.setProperty(geomProperty, writer.write(geometry));
	}

}