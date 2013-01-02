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

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.collections.graphdb.impl.EmbeddedGraphDatabase;
import org.neo4j.gis.spatial.ConsoleListener;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public class ImportOsmTest
{

    private static GraphDatabaseService db;
    String storeDir = "target/db/"+System.currentTimeMillis();

    @Test
    @Ignore
    public void test() throws Exception
    {
        OSMImporter importer = new OSMImporter("layer1", new ConsoleListener());
        BatchInserter batchInserter = getBatchInserter(storeDir);
        importer.importFile(batchInserter, "osm_files/maharashtra.osm", false);
        batchInserter.shutdown();
        GraphDatabaseService gdb = getGDB( storeDir );
        importer.reIndex( gdb );
        gdb.shutdown();

    }

    private BatchInserter getBatchInserter(String storeDir) throws Exception
    {
        
        return  new BatchInserterImpl(storeDir );
    }
    
    private GraphDatabaseService getGDB(String storeDir) throws Exception
    {
        
        return  new EmbeddedGraphDatabase(storeDir );
    }

}
