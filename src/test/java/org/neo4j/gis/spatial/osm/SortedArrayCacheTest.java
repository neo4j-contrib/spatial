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
package org.neo4j.gis.spatial.osm;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class SortedArrayCacheTest
{

    private static final int ONE_MILLION = 1000*1000;
    private SortedArrayIdCache cache = new SortedArrayIdCache();

    @Before
    public void before() {
    }
    @Test
    public void testUpper()
    {
            cache.add( 10 );
            assertEquals( 0, cache.getNodeIdFor( 10 ).intValue() );
    }
    @Test
    public void testLower()
    {
            cache.add( 100 );
            cache.add( 10 );
            cache.add( 5 );
            cache.add( 3 );
            cache.add( 1 );
            assertEquals( 0, cache.getNodeIdFor( 1 ).intValue() );
            assertEquals( 1, cache.getNodeIdFor( 3 ).intValue() );
            assertEquals( 2, cache.getNodeIdFor( 5 ).intValue() );
            assertEquals( 3, cache.getNodeIdFor( 10 ).intValue() );
            assertEquals( 4, cache.getNodeIdFor( 100 ).intValue() );
            assertEquals( null, cache.getNodeIdFor( 102 ) );
    }
    
    @Test(timeout=2000)
    public void testLargeCache() throws Exception
    {
        long time=System.currentTimeMillis();
        for (int i=0;i<ONE_MILLION;i++) {
            cache.add( i );
        }
        for (int i=0;i<ONE_MILLION;i++) {
            if (i != cache.getNodeIdFor( i )) throw new AssertionError(String.format("%d != %d",i,cache.getNodeIdFor( i )));
        }
        System.out.println((System.currentTimeMillis()-time)+" ms.");
        assertEquals(ONE_MILLION-1,cache.getNodeIdFor(ONE_MILLION-1).intValue());
        
    }
}
