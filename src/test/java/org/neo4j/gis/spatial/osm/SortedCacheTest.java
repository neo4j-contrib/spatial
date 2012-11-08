/**
 * Copyright (c) 2010-2012 "Neo Technology,"
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

public class SortedCacheTest
{

    private ForeignKeyCache cache;

    @Before
    public void before() {
        cache = new ForeignKeyCache( 1, 10, 6 );
    }
    @Test
    public void testUpper()
    {
            cache.put( 10, 1 );
            assertEquals( 1, cache.getNodeIdFor( 10 ) );
    }
    @Test
    public void testLower()
    {
            cache.put( 1, 1 );
            assertEquals( 1, cache.getNodeIdFor( 1 ) );
    }

    @Test(expected=IndexOutOfBoundsException.class)
    public void testOutsideUpper()
    {
            cache.put( 20, 1 );
    }
    @Test(expected=IndexOutOfBoundsException.class)
    public void testOutsideLower()
    {
            cache.put( 0, 1 );
    }

}
