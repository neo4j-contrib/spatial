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

import org.neo4j.gis.spatial.encoders.Configurable;
import org.neo4j.graphdb.PropertyContainer;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;


/**
 * @author Davide Savazzi
 */
public class WKBGeometryEncoder extends AbstractGeometryEncoder implements Configurable{

	private String wkbProperty=PROP_WKB;


    // Public methods
	
	public Geometry decodeGeometry(PropertyContainer container) {
		try {
			WKBReader reader = new WKBReader(layer.getGeometryFactory());
			return reader.read((byte[]) container.getProperty(wkbProperty));
		} catch (ParseException e) {
			throw new SpatialDatabaseException(e.getMessage(), e);
		}
	}
	
	
	// Protected methods
	
	protected void encodeGeometryShape(Geometry geometry, PropertyContainer container) {
        WKBWriter writer = new WKBWriter();
        container.setProperty(wkbProperty, writer.write(geometry));
	}
	
	@Override    
    public void setConfiguration( String configuration )
    {
        if ( configuration != null && configuration.trim().length() > 0 )
        {
            wkbProperty = configuration;
        }
    }


    @Override
    public String getConfiguration()
    {
        return wkbProperty;
    }
}