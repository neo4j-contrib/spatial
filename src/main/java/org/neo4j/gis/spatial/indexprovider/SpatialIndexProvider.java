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
package org.neo4j.gis.spatial.indexprovider;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.KernelExtension;

@Service.Implementation( KernelExtension.class )
public class SpatialIndexProvider extends IndexProvider
{

    private static final String DATASOURCE_NAME = "nioneodb";
    private EmbeddedGraphDatabase db;


    public SpatialIndexProvider( )
    {
        super( "spatial" );
        
    }


    @Override
    protected void load( KernelData kernel )
    {
        db = (EmbeddedGraphDatabase)kernel.graphDatabase();
    }


    @Override
    public String getDataSourceName()
    {
        return DATASOURCE_NAME;
    }


    @Override
    public Index<Node> nodeIndex( String indexName, Map<String, String> config )
    {
        return new LayerNodeIndex(indexName, db, config);
    }


    @Override
    public RelationshipIndex relationshipIndex( String indexName,
            Map<String, String> config )
    {
        throw new UnsupportedOperationException("Spatial relationship indexing is not supported at the moment. Please use the node index.");
    }


    @Override
    public Map<String, String> fillInDefaults( Map<String, String> config )
    {
        return null;
    }


    @Override
    public boolean configMatches( Map<String, String> storedConfig,
            Map<String, String> config )
    {
        // TODO Auto-generated method stub
        return false;
    }

}
