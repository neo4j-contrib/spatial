/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;


/**
 * Base class for the meta model tests.
 */
public abstract class Neo4jTestCase extends TestCase
{
	private File basePath = new File( "target/var" );
    private File dbPath = new File( basePath, "neo4j-db" );
    private GraphDatabaseService graphDb;
    private Transaction tx;

    @Override
    protected void setUp() throws Exception
    {
        setUp(true);
    }

    protected void setUp(boolean deleteDb) throws Exception
    {
        super.setUp();
        if (deleteDb) {
            deleteFileOrDirectory(dbPath);
        }
        graphDb = new EmbeddedGraphDatabase(dbPath.getAbsolutePath());
        tx = graphDb.beginTx();
    }

    @Override
    protected void tearDown() throws Exception
    {
        tx.success();
        tx.finish();
        beforeShutdown();
        graphDb.shutdown();
        super.tearDown();
    }
    
    protected void beforeShutdown()
    {
    }
    
    protected File getBasePath()
    {
        return basePath;
    }
    
    protected File getNeoPath()
    {
        return dbPath;
    }
    
    protected void deleteFileOrDirectory( File file )
    {
        if ( !file.exists() )
        {
            return;
        }
        
        if ( file.isDirectory() )
        {
            for ( File child : file.listFiles() )
            {
                deleteFileOrDirectory( child );
            }
        }
        else
        {
            file.delete();
        }
    }

    protected void restartTx()
    {
        restartTx( true );
    }
    
    protected void restartTx( boolean success )
    {
        if ( success )
        {
            tx.success();
        }
        else
        {
            tx.failure();
        }
        tx.finish();
        tx = graphDb.beginTx();
    }

    protected GraphDatabaseService graphDb()
    {
        return graphDb;
    }
    
    protected <T> void assertCollection( Collection<T> collection,
        T... expectedItems )
    {
        String collectionString = join( ", ", collection.toArray() );
        assertEquals( collectionString, expectedItems.length,
            collection.size() );
        for ( T item : expectedItems )
        {
            assertTrue( collection.contains( item ) );
        }
    }

    protected <T> Collection<T> asCollection( Iterable<T> iterable )
    {
        List<T> list = new ArrayList<T>();
        for ( T item : iterable )
        {
            list.add( item );
        }
        return list;
    }

    protected <T> String join( String delimiter, T... items )
    {
        StringBuffer buffer = new StringBuffer();
        for ( T item : items )
        {
            if ( buffer.length() > 0 )
            {
                buffer.append( delimiter );
            }
            buffer.append( item.toString() );
        }
        return buffer.toString();
    }

    protected <T> int countIterable( Iterable<T> iterable )
    {
        int counter = 0;
        Iterator<T> itr = iterable.iterator();
        while ( itr.hasNext() )
        {
            itr.next();
            counter++;
        }
        return counter;
    }
}
