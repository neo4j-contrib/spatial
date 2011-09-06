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
package org.neo4j.gis.spatial.pipes;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.Search;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.query.SearchAll;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;

public class GeoPipesTest implements GraphHolder
{

    private static ImpermanentGraphDatabase graphdb;
    private static Layer layer = null;
    public final static String LAYER_NAME = "two-street.osm";
    public final static int COMMIT_INTERVAL = 100;

    @Test
    public void testFluent()
    {
        assertEquals( 2, layer.filter().all().count() );
        assertEquals( 24, layer.filter().all().process().countPoints() );
    }

    @Before
    public void load( ) throws Exception
    {
        try
        {
            loadTestOsmData( LAYER_NAME, COMMIT_INTERVAL );
            SpatialDatabaseService spatialService = new SpatialDatabaseService(
                    graphdb );
            layer = spatialService.getLayer( LAYER_NAME );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    public static void loadTestOsmData( String layerName, int commitInterval )
            throws Exception
    {
        String osmPath = "./" + layerName;
        System.out.println( "\n=== Loading layer " + layerName + " from "
                            + osmPath + " ===" );
        // reActivateDatabase(false, false, false);
        OSMImporter importer = new OSMImporter( layerName );
        importer.importFile( graphdb, osmPath );
        importer.reIndex( graphdb, commitInterval );
    }

    @BeforeClass
    public static void setUp() throws Exception
    {
        graphdb = new ImpermanentGraphDatabase();
    }
    @Override
    public GraphDatabaseService graphdb()
    {
        return graphdb();
    }

}
