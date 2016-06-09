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
package org.neo4j.gis.spatial.encoders;

import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.PropertyContainer;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Simple encoder that stores point geometries as two x/y properties.
 * 
 * @author craig
 */
public class SimplePointEncoder extends AbstractGeometryEncoder implements
        Configurable
{
    public static final String DEFAULT_X = "longitude";
    public static final String DEFAULT_Y = "latitude";
    protected GeometryFactory geometryFactory;
    protected String xProperty = DEFAULT_X;
    protected String yProperty = DEFAULT_Y;

    protected GeometryFactory getGeometryFactory()
    {
        if ( geometryFactory == null ) geometryFactory = new GeometryFactory();
        return geometryFactory;
    }

    @Override
    protected void encodeGeometryShape( Geometry geometry,
            PropertyContainer container )
    {
        container.setProperty(
                "gtype",
                SpatialDatabaseService.convertJtsClassToGeometryType( geometry.getClass() ) );
        Coordinate[] coords = geometry.getCoordinates();
        container.setProperty( xProperty, coords[0].x );
        container.setProperty( yProperty, coords[0].y );
    }

    @Override
    public Geometry decodeGeometry( PropertyContainer container )
    {
        double x = ( (Number) container.getProperty( xProperty ) ).doubleValue();
        double y = ( (Number) container.getProperty( yProperty ) ).doubleValue();
        Coordinate coordinate = new Coordinate( x, y );
        return getGeometryFactory().createPoint( coordinate );
    }
    
    @Override
    public String getConfiguration()
    {
        return xProperty + ":" + yProperty + ":" + bboxProperty;
    }

    @Override    
    public void setConfiguration( String configuration )
    {
        if ( configuration != null && configuration.trim().length() > 0)
        {
            String[] fields = configuration.split( ":" );
            if ( fields.length > 0 ) xProperty = fields[0];
            if ( fields.length > 1 ) yProperty = fields[1];
            if ( fields.length > 2 ) bboxProperty = fields[2];
        }
    }
}
