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

import java.util.Arrays;

public class SortedArrayIdCache
{
    long[] data;
    int count;
    private int offset;

    public SortedArrayIdCache(int size) {
        this.data = new long[size];
    }
    public SortedArrayIdCache() {
        this(64);
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public void add(long otherId) {
        if (count==-1) throw new IllegalStateException("Array already compacted");
        if (count==data.length) {
            long[] newData = new long[data.length*2];
            System.arraycopy( data, 0, newData, 0, data.length );
            this.data = newData;
        }
        data[count++]=otherId;
    }
    
    private void compact(){
        long[] newData = new long[count];
        System.arraycopy( data, 0, newData, 0, count );
        this.data = newData;
        Arrays.sort( data );
        count = -1;
    }
    
    public Long getNodeIdFor(long otherId) {
        if (count!=-1) compact();
        final int index = Arrays.binarySearch(data, otherId);
        if (index<0) return null;
        return (long)index +offset;
    }
    public int size()
    {
        return count == -1 ? data.length : count;
    }
    public int afterLastId() {
        return size() + offset;
    }
}
