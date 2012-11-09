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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.neo4j.collections.rtree.Envelope;
import org.neo4j.gis.spatial.osm.OSMImporter.GeometryMetaData;
import org.neo4j.gis.spatial.osm.OSMImporter.StatsManager;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.AbstractGraphDatabase;

class OSMGraphWriter extends OSMWriter<Node>
{
    private GraphDatabaseService graphDb;
    private Node osm_root;
    private long currentChangesetId = -1;
    private Node currentChangesetNode;
    private long currentUserId = -1;
    private Node currentUserNode;
    private Node usersNode;
    private HashMap<Long, Node> changesetNodes = new HashMap<Long, Node>();
    private Transaction tx;
    private int checkCount = 0;
    private int txInterval;
    private boolean relatxedTxFlush = false;

    OSMGraphWriter( GraphDatabaseService graphDb,
            StatsManager statsManager, OSMImporter osmImporter,
            int txInterval, boolean relatxedTxFlush )
    {
        super( statsManager, osmImporter );
        this.graphDb = graphDb;
        this.txInterval = txInterval;
        this.relatxedTxFlush = relatxedTxFlush;
        if ( this.txInterval < 100 )
        {
            System.err.println( "Warning: Unusually short txInterval, expect bad insert performance" );
        }
        checkTx(); // Opens transaction for future writes
    }

    private void successTx()
    {
        if ( tx != null )
        {
            tx.success();
            tx.finish();
            tx = null;
            checkCount = 0;
        }
    }

    private void checkTx()
    {
        if ( checkCount++ > txInterval || tx == null )
        {
            successTx();
            if ( relatxedTxFlush )
            {
                tx = ( (AbstractGraphDatabase) graphDb ).tx().unforced().begin();
            }
            else
            {
                tx = graphDb.beginTx();
            }
        }
    }

    private Index<Node> indexFor( String indexName )
    {
        // return graphDb.index().forNodes( indexName,
        // MapUtil.stringMap("type", "exact") );
        return graphDb.index().forNodes( indexName );
    }

    private Node findNode( String name, Node parent,
            RelationshipType relType )
    {
        for ( Relationship relationship : parent.getRelationships( relType,
                Direction.OUTGOING ) )
        {
            Node node = relationship.getEndNode();
            if ( name.equals( node.getProperty( "name" ) ) )
            {
                return node;
            }
        }
        return null;
    }

    @Override
    protected Node getOrCreateNode( String name, String type, Node parent,
            RelationshipType relType )
    {
        Node node = findNode( name, parent, relType );
        if ( node == null )
        {
            node = graphDb.createNode();
            node.setProperty( "name", name );
            node.setProperty( "type", type );
            parent.createRelationshipTo( node, relType );
            checkTx();
        }
        return node;
    }

    @Override
    protected Node getOrCreateOSMDataset( String name )
    {
        if ( osm_dataset == null )
        {
            osm_root = getOrCreateNode( "osm_root", "osm",
                    graphDb.getReferenceNode(), OSMRelation.OSM );
            osm_dataset = getOrCreateNode( name, "osm", osm_root,
                    OSMRelation.OSM );
        }
        return osm_dataset;
    }

    @Override
    protected void setDatasetProperties(
            Map<String, Object> extractProperties )
    {
        for ( String key : extractProperties.keySet() )
        {
            osm_dataset.setProperty( key, extractProperties.get( key ) );
        }
    }

    private void addProperties( PropertyContainer node,
            Map<String, Object> properties )
    {
        for ( String property : properties.keySet() )
        {
            node.setProperty( property, properties.get( property ) );
        }
    }

    @Override
    protected void addNodeTags( Node node,
            Map<String, Object> tags, String type )
    {
        logNodeAddition( tags, type );
        if ( node != null && tags.size() > 0 )
        {
            statsManager.addToTagStats( type, tags.keySet() );
            Node tagsNode = graphDb.createNode();
            addProperties( tagsNode, tags );
            node.createRelationshipTo( tagsNode, OSMRelation.TAGS );
            tags.clear();
        }
    }

    @Override
    protected void addNodeGeometry( Node node, int gtype, Envelope bbox,
            int vertices )
    {
        if ( node != null && bbox.isValid() && vertices > 0 )
        {
            if ( gtype == OSMImporter.GTYPE_GEOMETRY )
                gtype = vertices > 1 ? OSMImporter.GTYPE_MULTIPOINT : OSMImporter.GTYPE_POINT;
            Node geomNode = graphDb.createNode();
            geomNode.setProperty( "gtype", gtype );
            geomNode.setProperty( "vertices", vertices );
            geomNode.setProperty( "bbox", new double[] { bbox.getMinX(),
                    bbox.getMaxX(), bbox.getMinY(), bbox.getMaxY() } );
            node.createRelationshipTo( geomNode, OSMRelation.GEOM );
            statsManager.addGeomStats( gtype );
        }
    }

    @Override
    protected Node addCachedNode(String name, Map<String, Object> properties, String indexKey) {
        return addNode(name,properties,indexKey);
    }

    @Override
    protected Node addNode( String name, Map<String, Object> properties,
            String indexKey )
    {
        Node node = graphDb.createNode();
        if ( indexKey != null && properties.containsKey( indexKey ) )
        {
            indexFor( name ).add( node, indexKey, properties.get( indexKey ) );
            properties.put( indexKey,
                    Long.parseLong( properties.get( indexKey ).toString() ) );
        }
        addProperties( node, properties );
        checkTx();
        return node;
    }

    protected Node addNodeWithCheck( String name,
            Map<String, Object> properties, String indexKey )
    {
        Node node = null;
        Object indexValue = ( indexKey == null ) ? null
                : properties.get( indexKey );
        if ( indexValue != null
             && ( createdNodes + foundNodes < 100 || foundNodes > 10 ) )
        {
            node = indexFor( name ).get( indexKey,
                    properties.get( indexKey ) ).getSingle();
        }
        if ( node == null )
        {
            node = graphDb.createNode();
            addProperties( node, properties );
            if ( indexValue != null )
            {
                indexFor( name ).add( node, indexKey,
                        properties.get( indexKey ) );
            }
            createdNodes++;
            checkTx();
        }
        else
        {
            foundNodes++;
        }
        return node;
    }

    @Override
    protected void createRelationship( Node from, Node to,
            RelationshipType relType, Map<String, Object> relProps )
    {
        Relationship rel = from.createRelationshipTo( to, relType );
        if ( relProps != null && relProps.size() > 0 )
        {
            addProperties( rel, relProps );
        }
    }

    @Override
    protected long getDatasetId()
    {
        return osm_dataset.getId();
    }

    @Override
    protected Node getSingleNode( String name, String string, Object value )
    {
        return indexFor( name ).get( string, value ).getSingle();
    }

    @Override
    protected Map<String, Object> getNodeProperties( Node node )
    {
        LinkedHashMap<String, Object> properties = new LinkedHashMap<String, Object>();
        for ( String property : node.getPropertyKeys() )
        {
            properties.put( property, node.getProperty( property ) );
        }
        return properties;
    }

    @Override
    protected Node getOSMNode( long osmId, Node changesetNode )
    {
        if ( currentChangesetNode != changesetNode
             || changesetNodes.isEmpty() )
        {
            currentChangesetNode = changesetNode;
            changesetNodes.clear();
            for ( Relationship rel : changesetNode.getRelationships(
                    OSMRelation.CHANGESET, Direction.INCOMING ) )
            {
                Node node = rel.getStartNode();
                Long nodeOsmId = (Long) node.getProperty( "node_osm_id",
                        null );
                if ( nodeOsmId != null )
                {
                    changesetNodes.put( nodeOsmId, node );
                }
            }
        }
        Node node = changesetNodes.get( osmId );
        if ( node == null )
        {
            logNodeFoundFrom( "node-index" );
            return indexFor( "node" ).get( "node_osm_id", osmId ).getSingle();
        }
        else
        {
            logNodeFoundFrom( "changeset" );
            return node;
        }
    }

    @Override
    protected void updateGeometryMetaDataFromMember( Node member,
            GeometryMetaData metaGeom, Map<String, Object> nodeProps )
    {
        for ( Relationship rel : member.getRelationships( OSMRelation.GEOM ) )
        {
            nodeProps = getNodeProperties( rel.getEndNode() );
            metaGeom.checkSupportedGeometry( (Integer) nodeProps.get( "gtype" ) );
            metaGeom.expandToIncludeBBox( nodeProps );
        }
    }

    @Override
    protected void finish()
    {
        osm_dataset.setProperty( "relationCount",
                (Integer) osm_dataset.getProperty( "relationCount", 0 )
                        + relationCount );
        osm_dataset.setProperty( "wayCount",
                (Integer) osm_dataset.getProperty( "wayCount", 0 )
                        + wayCount );
        osm_dataset.setProperty( "nodeCount",
                (Integer) osm_dataset.getProperty( "nodeCount", 0 )
                        + nodeCount );
        osm_dataset.setProperty( "poiCount",
                (Integer) osm_dataset.getProperty( "poiCount", 0 )
                        + poiCount );
        osm_dataset.setProperty( "changesetCount",
                (Integer) osm_dataset.getProperty( "changesetCount", 0 )
                        + changesetCount );
        osm_dataset.setProperty( "userCount",
                (Integer) osm_dataset.getProperty( "userCount", 0 )
                        + userCount );
        successTx();
    }

    @Override
    protected Node createProxyNode()
    {
        return graphDb.createNode();
    }

    @Override
    protected Node getChangesetNode( Map<String, Object> nodeProps )
    {
        long changeset = Long.parseLong( nodeProps.remove(
                OSMImporter.INDEX_NAME_CHANGESET ).toString() );
        getUserNode( nodeProps );
        if ( changeset != currentChangesetId )
        {
            currentChangesetId = changeset;
            IndexHits<Node> result = indexFor( OSMImporter.INDEX_NAME_CHANGESET ).get(
                    OSMImporter.INDEX_NAME_CHANGESET, currentChangesetId );
            if ( result.size() > 0 )
            {
                currentChangesetNode = result.getSingle();
            }
            else
            {
                LinkedHashMap<String, Object> changesetProps = new LinkedHashMap<String, Object>();
                changesetProps.put( OSMImporter.INDEX_NAME_CHANGESET,
                        currentChangesetId );
                changesetProps.put( "timestamp",
                        nodeProps.get( "timestamp" ) );
                currentChangesetNode = (Node) addNode(
                        OSMImporter.INDEX_NAME_CHANGESET, changesetProps,
                        OSMImporter.INDEX_NAME_CHANGESET );
                changesetCount++;
                if ( currentUserNode != null )
                {
                    createRelationship( currentChangesetNode,
                            currentUserNode, OSMRelation.USER );
                }
            }
            result.close();
        }
        return currentChangesetNode;
    }

    @Override
    protected Node getUserNode( Map<String, Object> nodeProps )
    {
        try
        {
            long uid = Long.parseLong( nodeProps.remove( "uid" ).toString() );
            String name = nodeProps.remove( OSMImporter.INDEX_NAME_USER ).toString();
            if ( uid != currentUserId )
            {
                currentUserId = uid;
                IndexHits<Node> result = indexFor( OSMImporter.INDEX_NAME_USER ).get(
                        "uid", currentUserId );
                if ( result.size() > 0 )
                {
                    currentUserNode = indexFor( OSMImporter.INDEX_NAME_USER ).get(
                            "uid", currentUserId ).getSingle();
                }
                else
                {
                    LinkedHashMap<String, Object> userProps = new LinkedHashMap<String, Object>();
                    userProps.put( "uid", currentUserId );
                    userProps.put( "name", name );
                    userProps.put( "timestamp", nodeProps.get( "timestamp" ) );
                    currentUserNode = (Node) addNode( OSMImporter.INDEX_NAME_USER,
                            userProps, "uid" );
                    userCount++;
                    // if (currentChangesetNode != null) {
                    // currentChangesetNode.createRelationshipTo(currentUserNode,
                    // OSMRelation.USER);
                    // }
                    if ( usersNode == null )
                    {
                        usersNode = graphDb.createNode();
                        osm_dataset.createRelationshipTo( usersNode,
                                OSMRelation.USERS );
                    }
                    usersNode.createRelationshipTo( currentUserNode,
                            OSMRelation.OSM_USER );
                }
                result.close();
            }
        }
        catch ( Exception e )
        {
            currentUserId = -1;
            currentUserNode = null;
            logMissingUser( nodeProps );
        }
        return currentUserNode;
    }

    public String toString()
    {
        return "OSMGraphWriter: DatabaseService[" + graphDb
               + "]:txInterval[" + this.txInterval + "]";
    }

}