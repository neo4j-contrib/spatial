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
package org.neo4j.gis.spatial;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.neo4j.gis.spatial.query.SearchWithin;
import org.neo4j.gis.spatial.server.plugin.SpatialPlugin;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import com.vividsolutions.jts.geom.Envelope;

public class ServerPluginTest extends Neo4jTestCase
{

    private static final String LAYER = "layer";
    private static final String LON = "lon";
    private static final String LAT = "lat";
    private SpatialPlugin plugin;

    @Before
    public void setUp() throws Exception
    {
        super.setUp();
//        restartTx();
        plugin = new SpatialPlugin();

    }

    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
    }

    @Test
    public void testCreateLayer()
    {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(
                graphDb() );
        assertNull( spatialService.getLayer( LAYER ) );
        plugin.addLayer( graphDb(), LAYER, LAT, LON );

        assertNotNull( spatialService.getLayer( LAYER ) );
    }

    @Test
    public void testAddPointToLayerWithDefaults()
    {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(
                graphDb() );
        plugin.addLayer( graphDb(), LAYER, LAT, LON );
        assertNotNull( spatialService.getLayer( LAYER ) );
        Layer layer2 = spatialService.getLayer(LAYER);
        SearchWithin withinQuery = new SearchWithin(
                layer2.getGeometryFactory().toGeometry(
                        new Envelope( 15.0, 16.0, 60.0, 61.0 ) ) );
        layer2.getIndex().executeSearch( withinQuery );
        List<SpatialDatabaseRecord> results = withinQuery.getResults();
        assertEquals( 0, results.size() );

        Transaction tx2 = graphDb().beginTx();
        Node point = graphDb().createNode();
        point.setProperty( LAT, 60.1 );
        point.setProperty( LON, 15.2 );
        point.setProperty( "bbox", new double[] { 15.2, 60.1, 15.2, 60.1 } );
        plugin.addPointToLayer( graphDb(), point, LAYER );
        tx2.success();
        tx2.finish();
        layer2.getIndex().executeSearch( withinQuery );
        results = withinQuery.getResults();
        assertEquals( 1, results.size() );

    }
}
