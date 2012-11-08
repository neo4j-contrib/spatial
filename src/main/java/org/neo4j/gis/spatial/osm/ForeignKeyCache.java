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

public class ForeignKeyCache implements IdCache
{
    private int[][] buckets;
    private int minForeignKeyId;
    private int nrOfBuckets;
    private int bucketSize;

    public ForeignKeyCache(int minForeignKeyId, int maxForeignKeyId, int nrOfBuckets) {
        this.minForeignKeyId = minForeignKeyId;
        this.nrOfBuckets = nrOfBuckets;
        this.bucketSize = (int) Math.ceil(  (maxForeignKeyId-minForeignKeyId+1)/nrOfBuckets)+1;
        buckets = new int[(nrOfBuckets)][0];
    }
    
    public void put(int foreignKeyId, int nodeId) {
        if(foreignKeyId < minForeignKeyId || foreignKeyId > nrOfBuckets*bucketSize+minForeignKeyId) {
            throw new ArrayIndexOutOfBoundsException(String.format( "cannot index keys outside the cache range [%d,%d] got: %d",minForeignKeyId, nrOfBuckets*bucketSize+minForeignKeyId,foreignKeyId ));
        }
        int bucketIndex = bucketIndex( foreignKeyId );
        if(buckets[bucketIndex].length == 0) {
            buckets[bucketIndex] = new int[bucketSize];
        }
        int[] bucket = buckets[bucketIndex];
        bucket[index(foreignKeyId, bucketIndex)]=nodeId;
    }
    
    private int index( int foreignKeyId, int bucketIndex )
    {
        return (foreignKeyId-minForeignKeyId)/nrOfBuckets;
    }

    public int getNodeIdFor(int foreignKeyId) {
        int bucketIndex = bucketIndex( foreignKeyId );
        return buckets[bucketIndex][index( foreignKeyId, bucketIndex )];
    }

    private int bucketIndex( int foreignKeyId )
    {
        return (foreignKeyId-minForeignKeyId)%nrOfBuckets;
    }

    public void put( long parseLong, long id )
    {
        put((int) parseLong, (int) id);
        
    }
}
