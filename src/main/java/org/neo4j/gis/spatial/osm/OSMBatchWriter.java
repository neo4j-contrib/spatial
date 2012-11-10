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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.neo4j.collections.rtree.Envelope;
import org.neo4j.gis.spatial.osm.OSMImporter.GeometryMetaData;
import org.neo4j.gis.spatial.osm.OSMImporter.StatsManager;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.SimpleRelationship;

class OSMBatchWriter extends OSMWriter<Long>
{
    private BatchInserter batchInserter;
    private BatchInserterIndexProvider batchIndexService;
    private HashMap<String, BatchInserterIndex> batchIndices = new HashMap<String, BatchInserterIndex>();
    private long osm_root;
    private long currentChangesetId = -1;
    private long currentChangesetNode = -1;
    private long currentUserId = -1;
    private long currentUserNode = -1;
    private long usersNode = -1;
    private HashMap<Long, Long> changesetNodes = new HashMap<Long, Long>();
    private SortedArrayIdCache cache;
    long offsettedNodeId;

    OSMBatchWriter( BatchInserter batchGraphDb,
            StatsManager statsManager, OSMImporter osmImporter, String dataset )
    {
        super( statsManager, osmImporter );
        this.batchInserter = batchGraphDb;
        this.batchIndexService = new LuceneBatchInserterIndexProvider(
                batchGraphDb );
        this.cache = OSMImporter.initializeCache( dataset );
        this.cache.setOffset(1); // root node
        this.offsettedNodeId = cache.afterLastId();
    }

    private BatchInserterIndex indexFor( String indexName )
    {
        BatchInserterIndex index = batchIndices.get( indexName );
        if ( index == null )
        {
            index = batchIndexService.nodeIndex( indexName,
                    MapUtil.stringMap( "type", "exact" ) );
            batchIndices.put( indexName, index );
        }
        return index;
    }

    @Override
    public Long getOrCreateOSMDataset( String name )
    {
        if ( osm_dataset == null || osm_dataset <= 0 )
        {
            osm_root = getOrCreateNode( "osm_root", "osm",
                    batchInserter.getReferenceNode(), OSMRelation.OSM );
            osm_dataset = getOrCreateNode( name, "osm", osm_root,
                    OSMRelation.OSM );
        }
        return osm_dataset;
    }

    private long findNode( BatchInserter batchInserter, String name,
            long parent, RelationshipType relType )
    {
        for ( SimpleRelationship relationship : batchInserter.getRelationships( parent ) )
        {
            if ( relationship.getType().name().equals( relType.name() ) )
            {
                long node = relationship.getEndNode();
                Object nodeName = batchInserter.getNodeProperties( node ).get(
                        "name" );
                if ( nodeName != null && name.equals( nodeName.toString() ) )
                {
                    return node;
                }
            }
        }
        return -1;
    }

    @Override
    protected Long getOrCreateNode( String name, String type, Long parent,
            RelationshipType relType )
    {
        long node = findNode( batchInserter, name, parent, relType );
        if ( node < 0 )
        {
            HashMap<String, Object> properties = new HashMap<String, Object>();
            properties.put( "name", name );
            properties.put( "type", type );
            node = createNode( properties );
            batchInserter.createRelationship( parent, node, relType, null );
        }
        return node;
    }

    public String toString()
    {
        return "OSMBatchWriter: BatchInserter[" + batchInserter.toString()
               + "]:IndexService[" + batchIndexService.toString() + "]";
    }

    @Override
    protected void setDatasetProperties( Map<String, Object> extraProperties )
    {
        LinkedHashMap<String, Object> properties = new LinkedHashMap<String, Object>();
        properties.putAll( batchInserter.getNodeProperties( osm_dataset ) );
        properties.putAll( extraProperties );
        batchInserter.setNodeProperties( osm_dataset, properties );
    }

    protected void addNodeTags( Long node,
            Map<String, Object> tags, String type )
    {
        logNodeAddition( tags, type );
        if ( node > 0 && tags.size() > 0 )
        {
            statsManager.addToTagStats( type, tags.keySet() );
            long id = createNode( tags );
            batchInserter.createRelationship( node, id, OSMRelation.TAGS,
                    null );
            tags.clear();
        }
    }

    @Override
    protected void addNodeGeometry( Long node, int gtype, Envelope bbox,
            int vertices )
    {
        if ( node > 0 && bbox.isValid() && vertices > 0 )
        {
            LinkedHashMap<String, Object> properties = new LinkedHashMap<String, Object>();
            if ( gtype == OSMImporter.GTYPE_GEOMETRY )
                gtype = vertices > 1 ? OSMImporter.GTYPE_MULTIPOINT : OSMImporter.GTYPE_POINT;
            properties.put( "gtype", gtype );
            properties.put( "vertices", vertices );
            properties.put(
                    "bbox",
                    new double[] { bbox.getMinX(), bbox.getMaxX(),
                            bbox.getMinY(), bbox.getMaxY() } );
            long id = createNode( properties );
            batchInserter.createRelationship( node, id, OSMRelation.GEOM,
                    null );
            properties.clear();
            statsManager.addGeomStats( gtype );
        }
    }

    @Override
    protected Long addNode( String name, Map<String, Object> properties,
            String indexKey )
    {
        if ( indexKey != null && properties.containsKey( indexKey ) )
        {
            long indexId = Long.parseLong( properties.get( indexKey ).toString());
            properties.put( indexKey, indexId );
        }
        return createNode( properties );
    }

    @Override
    protected Long addCachedNode( String name, Map<String, Object> properties,
            String indexKey )
    {
        long osmId = Long.parseLong( properties.get( indexKey ).toString() );
        properties.put( indexKey, osmId );
        long id = (long) cache.getNodeIdFor( osmId );
        ///System.out.printf("cached node %d = %s%n",id,properties);

        batchInserter.createNode( id, properties );
        return id;
    }

    
    private long createNode( Map<String, Object> properties )
    {
        long id = offsettedNodeId;
        batchInserter.createNode( id , properties );
        ///System.out.printf("added node %d = %s%n",id,properties);
        offsettedNodeId++;
        return id;
    }

    protected Long addNodeWithCheck( String name,
            Map<String, Object> properties, String indexKey )
    {
        // TODO: This code allows for importing into existing data, but
        // slows the import down by almost three times. The problem is that
        // the batchIndexService cannot switch efficiently between read and
        // write mode. Rather we should use pure GraphDatabaseService API
        // for update mode.
        long id = -1;
        Object indexValue = ( indexKey == null ) ? null
                : properties.get( indexKey );
        if ( indexValue != null
             && ( createdNodes + foundNodes < 100 || foundNodes > 10 ) )
        {
            id = indexFor( name ).get( indexKey, properties.get( indexKey ) ).getSingle();
        }
        if ( id < 0 )
        {
            id = createNode( properties );
            if ( indexValue != null )
            {
                Map<String, Object> props = new HashMap<String, Object>();
                props.put( indexKey, properties.get( indexKey ) );
                indexFor( name ).add( id, props );
            }
            createdNodes++;
        }
        else
        {
            foundNodes++;
        }
        return id;
    }

    @Override
    protected void createRelationship( Long from, Long to,
            RelationshipType relType, Map<String, Object> relProps )
    {
        batchInserter.createRelationship( from, to, relType, relProps );
    }

    protected void optimize()
    {
        // TODO: optimize
        // batchIndexService.optimize();
        for ( String index : new String[] { "node", "way", "changeset",
                "user" } )
        {
            indexFor( index ).flush();
        }
    }

    @Override
    protected long getDatasetId()
    {
        return osm_dataset;
    }

    @Override
    protected Long getSingleNode( String name, String string, Object value )
    {
        return indexFor( name ).get( string, value ).getSingle();
    }

    @Override
    protected Map<String, Object> getNodeProperties( Long member )
    {
        return batchInserter.getNodeProperties( member );
    }

    @Override
    protected Long getOSMNode( long osmId, Long changesetNode )
    {
//        if ( currentChangesetNode != changesetNode
//             || changesetNodes.isEmpty() )
//        {
//            currentChangesetNode = changesetNode;
//            changesetNodes.clear();
//            for ( SimpleRelationship rel : batchInserter.getRelationships( changesetNode ) )
//            {
//                if ( rel.getType().name().equals(
//                        OSMRelation.CHANGESET.name() ) )
//                {
//                    Long node = rel.getStartNode();
//                    Map<String, Object> props = batchInserter.getNodeProperties( node );
//                    Long nodeOsmId = (Long) props.get( "node_osm_id" );
//                    if ( nodeOsmId != null )
//                    {
//                        changesetNodes.put( nodeOsmId, node );
//                    }
//                }
//            }
//        }
//        Long node = changesetNodes.get( osmId );
//        if ( node == null )
//        {
//            logNodeFoundFrom( "node-index" );
//            //return indexFor( INDEX_NAME_NODE ).get( "node_osm_id", osmId ).getSingle();
//            return (long) cache.getNodeIdFor( (int) osmId );
//        }
//        else
//        {
//            logNodeFoundFrom( "changeset" );
//            return node;
//        }
        return cache.getNodeIdFor(osmId);
    }

    @Override
    protected void updateGeometryMetaDataFromMember( Long member,
            GeometryMetaData metaGeom, Map<String, Object> nodeProps )
    {
        for ( SimpleRelationship rel : batchInserter.getRelationships( member ) )
        {
            if ( rel.getType().equals( OSMRelation.GEOM ) )
            {
                nodeProps = getNodeProperties( rel.getEndNode() );
                metaGeom.checkSupportedGeometry( (Integer) nodeProps.get( "gtype" ) );
                metaGeom.expandToIncludeBBox( nodeProps );
            }
        }
    }

    @Override
    protected void finish()
    {
        HashMap<String, Object> dsProps = new HashMap<String, Object>(
                batchInserter.getNodeProperties( osm_dataset ) );
        updateDSCounts( dsProps, "relationCount", relationCount );
        updateDSCounts( dsProps, "wayCount", wayCount );
        updateDSCounts( dsProps, "nodeCount", nodeCount );
        updateDSCounts( dsProps, "poiCount", poiCount );
        updateDSCounts( dsProps, "changesetCount", changesetCount );
        updateDSCounts( dsProps, "userCount", userCount );
        setDatasetProperties( dsProps );
        batchIndexService.shutdown();
        batchIndexService = null;
    }

    private void updateDSCounts( HashMap<String, Object> dsProps,
            String name, int count )
    {
        Integer current = (Integer) dsProps.get( name );
        dsProps.put( name, ( current == null ? 0 : current ) + count );
    }

    @Override
    protected Long createProxyNode()
    {
        return createNode( null );
    }

    Map<Long,Long> changesetCache = new HashMap<Long, Long>();
    @Override
    protected Long getChangesetNode( Map<String, Object> nodeProps )
    {
        long changeset = Long.parseLong( nodeProps.remove( "changeset" ).toString() );
        getUserNode( nodeProps );
        if ( changeset != currentChangesetId )
        {
            currentChangesetId = changeset;
            changesetNodes.clear();
            if (changesetCache.containsKey(changeset)) {
                return changesetCache.get(changeset);
            }
/*            IndexHits<Long> results = indexFor( "changeset" ).get("changeset", currentChangesetId );

            Long foundNode = results.getSingle();
            if ( foundNode != null )
            {
                currentChangesetNode = foundNode;
                changesetCache.put(changeset,foundNode);
            }
            else
*/
            {
                LinkedHashMap<String, Object> changesetProps = new LinkedHashMap<String, Object>();
                changesetProps.put( "changeset", currentChangesetId );
                changesetProps.put( "timestamp",
                        nodeProps.get( "timestamp" ) );
                currentChangesetNode = (Long) addNode( "changeset",
                        changesetProps, "changeset" );
                changesetCache.put(changeset,currentChangesetNode);
                if ( currentUserNode > 0 )
                {
                    createRelationship( currentChangesetNode,
                            currentUserNode, OSMRelation.USER );
                }
            }
        }
        return currentChangesetNode;
    }

    Map<Long,Long> userCache=new HashMap<Long, Long>();
    @Override
    protected Long getUserNode( Map<String, Object> nodeProps )
    {
        try
        {
            long uid = Long.parseLong( nodeProps.remove( "uid" ).toString() );
            String name = nodeProps.remove( "user" ).toString();
            if ( uid != currentUserId )
            {
                currentUserId = uid;
                if (userCache.containsKey(uid)) {
                    return userCache.get(uid);
                }
/*
                IndexHits<Long> results = indexFor( OSMImporter.INDEX_NAME_USER ).get("uid", currentUserId);
                if ( results.size() > 0 )
                {
                    currentUserNode = results.getSingle();
                    userCache.put(uid,currentUserNode);
                }
                else
*/
                {
                    LinkedHashMap<String, Object> userProps = new LinkedHashMap<String, Object>();
                    userProps.put( "uid", currentUserId );
                    userProps.put( "name", name );
                    userProps.put( "timestamp", nodeProps.get( "timestamp" ) );
                    currentUserNode = (Long) addNode( "user", userProps, "uid");
                    userCache.put(uid,currentUserNode);
                    indexFor( OSMImporter.INDEX_NAME_USER ).flush();
                    if ( usersNode < 0 )
                    {
                        usersNode = createNode( Collections.<String,Object>emptyMap() );
                        createRelationship( osm_dataset, usersNode,
                                OSMRelation.USERS );
                    }
                    createRelationship( usersNode, currentUserNode,
                            OSMRelation.OSM_USER );
                }
//                results.close();
            }
        }
        catch ( Exception e )
        {
            currentUserId = -1;
            currentUserNode = -1;
            logMissingUser( nodeProps );
        }
        return currentUserNode;
    }

}