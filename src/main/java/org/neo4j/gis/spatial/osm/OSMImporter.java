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

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.collections.MapUtils;
import org.geotools.referencing.datum.DefaultEllipsoid;
import org.neo4j.gis.spatial.utilities.ReferenceNodes;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.rtree.NullListener;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProviderNewImpl;
import org.neo4j.kernel.Traversal;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.*;

public class OSMImporter implements Constants
{
    public static DefaultEllipsoid WGS84 = DefaultEllipsoid.WGS84;
    public static String INDEX_NAME_CHANGESET = "changeset";
    public static String INDEX_NAME_USER = "user";
    public static String INDEX_NAME_NODE = "node";
    public static String INDEX_NAME_WAY = "node";

    protected boolean nodesProcessingFinished = false;
    private String layerName;
    private StatsManager stats = new StatsManager();
    private long osm_dataset = -1;
    private Listener monitor;
    private com.vividsolutions.jts.geom.Envelope filterEnvelope = null;

    private Charset charset = Charset.defaultCharset();

    private static class TagStats
    {
        private String name;
        private int count = 0;
        private HashMap<String, Integer> stats = new HashMap<String, Integer>();

        TagStats( String name )
        {
            this.name = name;
        }

        int add( String key )
        {
            count++;
            if ( stats.containsKey( key ) )
            {
                int num = stats.get( key );
                stats.put( key, ++num );
                return num;
            }
            else
            {
                stats.put( key, 1 );
                return 1;
            }
        }

        /**
         * Return only reasonably commonly used tags.
         * 
         * @return
         */
        public String[] getTags()
        {
            if ( stats.size() > 0 )
            {
                int threshold = count / ( stats.size() * 20 );
                ArrayList<String> tags = new ArrayList<String>();
                for ( String key : stats.keySet() )
                {
                    if ( key.equals( "waterway" ) )
                    {
                        System.out.println( "debug[" + key + "]: "
                                            + stats.get( key ) );
                    }
                    if ( stats.get( key ) > threshold ) tags.add( key );
                }
                Collections.sort( tags );
                return tags.toArray( new String[tags.size()] );
            }
            else
            {
                return new String[0];
            }
        }

        public String toString()
        {
            return "TagStats[" + name + "]: " + asList( getTags() );
        }
    }

    private static class StatsManager
    {
        private HashMap<String, TagStats> tagStats = new HashMap<String, TagStats>();
        private HashMap<Integer, Integer> geomStats = new HashMap<Integer, Integer>();;

        protected TagStats getTagStats( String type )
        {
            if ( !tagStats.containsKey( type ) )
            {
                tagStats.put( type, new TagStats( type ) );
            }
            return tagStats.get( type );
        }

        protected int addToTagStats( String type, String key )
        {
            getTagStats( "all" ).add( key );
            return getTagStats( type ).add( key );
        }

        protected int addToTagStats( String type, Collection<String> keys )
        {
            int count = 0;
            for ( String key : keys )
            {
                count += addToTagStats( type, key );
            }
            return count;
        }

        protected void printTagStats()
        {
            System.out.println( "Tag statistics for " + tagStats.size()
                                + " types:" );
            for ( String key : tagStats.keySet() )
            {
                TagStats stats = tagStats.get( key );
                System.out.println( "\t" + key + ": " + stats );
            }
        }

        protected void addGeomStats( Node geomNode )
        {
            if ( geomNode != null )
            {
                addGeomStats( (Integer) geomNode.getProperty( PROP_TYPE, null ) );
            }
        }

        protected void addGeomStats( Integer geom )
        {
            Integer count = geomStats.get( geom );
            geomStats.put( geom, count == null ? 1 : count + 1 );
        }

        protected void dumpGeomStats()
        {
            System.out.println( "Geometry statistics for " + geomStats.size()
                                + " geometry types:" );
            for ( Object key : geomStats.keySet() )
            {
                Integer count = geomStats.get( key );
                System.out.println( "\t"
                                    + SpatialDatabaseService.convertGeometryTypeToName( (Integer) key )
                                    + ": " + count );
            }
            geomStats.clear();
        }

    }

    public OSMImporter( String layerName )
    {
        this( layerName, null );
    }

    public OSMImporter( String layerName, Listener monitor )
    {
        this( layerName, null, null );
    }

    public OSMImporter( String layerName, Listener monitor, com.vividsolutions.jts.geom.Envelope filterEnvelope )
    {
        this.layerName = layerName;
        if ( monitor == null ) monitor = new NullListener();
        this.monitor = monitor;
        this.filterEnvelope = filterEnvelope;
    }

    public void reIndex( GraphDatabaseService database )
    {
        reIndex( database, 10000, true, false );
    }

    public void reIndex( GraphDatabaseService database, int commitInterval )
    {
        reIndex( database, commitInterval, true, false );
    }

    public void reIndex( GraphDatabaseService database, int commitInterval,
            boolean includePoints, boolean includeRelations )
    {
        if ( commitInterval < 1 )
            throw new IllegalArgumentException( "commitInterval must be >= 1" );
        System.out.println( "Re-indexing with GraphDatabaseService: "
                            + database + " (class: " + database.getClass()
                            + ")" );

        setLogContext( "Index" );
        SpatialDatabaseService spatialDatabase = new SpatialDatabaseService(
                database );
        OSMLayer layer = (OSMLayer) spatialDatabase.getOrCreateLayer(
                layerName, OSMGeometryEncoder.class, OSMLayer.class );
        // TODO: The next line creates the relationship between the dataset and
        // layer, but this seems more like a side-effect and should be done
        // explicitly
        OSMDataset dataset = layer.getDataset( osm_dataset );
        layer.clear(); // clear the index without destroying underlying data

        long startTime = System.currentTimeMillis();
        org.neo4j.graphdb.traversal.TraversalDescription traversal = Traversal.description().depthFirst()
                .evaluator(Evaluators.excludeStartPosition())
                .relationships(OSMRelation.WAYS, Direction.OUTGOING)
                .relationships(OSMRelation.NEXT, Direction.OUTGOING);

        Transaction tx = database.beginTx();
        boolean useWays = false;
        int count = 0;
        try
        {
            layer.setExtraPropertyNames( stats.getTagStats( "all" ).getTags() );
            if ( useWays )
            {
                beginProgressMonitor( dataset.getWayCount() );
                for ( Node way : traversal.traverse(database.getNodeById( osm_dataset )).nodes() )
                {
                    updateProgressMonitor( count );
                    incrLogContext();
                    stats.addGeomStats( layer.addWay( way, true ) );
                    if ( includePoints )
                    {
                        Node first = way.getSingleRelationship(
                                OSMRelation.FIRST_NODE, Direction.OUTGOING ).getEndNode();
                        for ( Node proxy : first.traverse( Order.DEPTH_FIRST,
                                StopEvaluator.END_OF_GRAPH,
                                ReturnableEvaluator.ALL, OSMRelation.NEXT,
                                Direction.OUTGOING ) )
                        {
                            Node node = proxy.getSingleRelationship(
                                    OSMRelation.NODE, Direction.OUTGOING ).getEndNode();
                            stats.addGeomStats( layer.addWay( node, true ) );
                        }
                    }
                    if ( ++count % commitInterval == 0 )
                    {
                        tx.success();
                        tx.close();
                        tx = database.beginTx();
                    }
                } // TODO ask charset to user?
            }
            else
            {
                beginProgressMonitor( dataset.getChangesetCount() );
                for ( Node changeset : dataset.getAllChangesetNodes() )
                {
                    updateProgressMonitor( count );
                    incrLogContext();
                    for ( Relationship rel : changeset.getRelationships(
                            OSMRelation.CHANGESET, Direction.INCOMING ) )
                    {
                        stats.addGeomStats( layer.addWay( rel.getStartNode(),
                                true ) );
                    }
                    if ( ++count % commitInterval == 0 )
                    {
                        tx.success();
                        tx.close();
                        tx = database.beginTx();
                    }
                } // TODO ask charset to user?
            }
            tx.success();
        }
        finally
        {
            endProgressMonitor();
            tx.close();
        }

        long stopTime = System.currentTimeMillis();
        log( "info | Re-indexing elapsed time in seconds: "
             + ( 1.0 * ( stopTime - startTime ) / 1000.0 ) );
        stats.dumpGeomStats();
    }

    private static class GeometryMetaData
    {
        private Envelope bbox = new Envelope();
        private int vertices = 0;
        private int geometry = -1;

        public GeometryMetaData( int type )
        {
            this.geometry = type;
        }

        public int getGeometryType()
        {
            return geometry;
        }

        public void expandToIncludePoint( double[] location )
        {
            bbox.expandToInclude( location[0], location[1] );
            vertices++;
            geometry = -1;
        }

        public void expandToIncludeBBox( Map<String, Object> nodeProps )
        {
            double[] sbb = (double[]) nodeProps.get( PROP_BBOX );
            bbox.expandToInclude( sbb[0], sbb[2] );
            bbox.expandToInclude( sbb[1], sbb[3] );
            vertices += (Integer) nodeProps.get( "vertices" );
        }

        public void checkSupportedGeometry( Integer memGType )
        {
            if ( ( memGType == null || memGType != GTYPE_LINESTRING )
                 && geometry != GTYPE_POLYGON )
            {
                geometry = -1;
            }
        }

        public void setPolygon()
        {
            geometry = GTYPE_POLYGON;
        }

        public boolean isValid()
        {
            return geometry > 0;
        }

        public int getVertices()
        {
            return vertices;
        }

        public Envelope getBBox()
        {
            return bbox;
        }
    }

    private static abstract class OSMWriter<T>
    {
        protected StatsManager statsManager;
        protected OSMImporter osmImporter;
        protected T osm_dataset;

        private OSMWriter( StatsManager statsManager, OSMImporter osmImporter )
        {
            this.statsManager = statsManager;
            this.osmImporter = osmImporter;
        }

        public static OSMWriter<Long> fromBatchInserter(
                BatchInserter batchInserter, StatsManager stats,
                OSMImporter osmImporter )
        {
            return new OSMBatchWriter( batchInserter, stats, osmImporter );
        }

        public static OSMWriter<Node> fromGraphDatabase(
                GraphDatabaseService graphDb, StatsManager stats,
                OSMImporter osmImporter, int txInterval, boolean relaxedTxFlush )
        {
            return new OSMGraphWriter( graphDb, stats, osmImporter, txInterval, relaxedTxFlush );
        }

        protected abstract T getOrCreateNode( String name, String type,
                T parent, RelationshipType relType );

        protected abstract T getOrCreateOSMDataset( String name );

        protected abstract void setDatasetProperties(
                Map<String, Object> extractProperties );

        protected abstract void addNodeTags( T node,
                LinkedHashMap<String, Object> tags, String type );

        protected abstract void addNodeGeometry( T node, int gtype,
                Envelope bbox, int vertices );

        protected abstract T addNode( String name,
                Map<String, Object> properties, String indexKey );

        protected abstract void createRelationship( T from, T to,
                RelationshipType relType, LinkedHashMap<String, Object> relProps );

        protected void createRelationship( T from, T to,
                RelationshipType relType )
        {
            createRelationship( from, to, relType, null );
        }

        protected HashMap<String, Integer> stats = new HashMap<String, Integer>();
        protected HashMap<String, LogCounter> nodeFindStats = new HashMap<String, LogCounter>();
        protected long logTime = 0;
        protected long findTime = 0;
        protected long firstFindTime = 0;
        protected long lastFindTime = 0;
        protected long firstLogTime = 0;
        protected static int foundNodes = 0;
        protected static int createdNodes = 0;
        protected int foundOSMNodes = 0;
        protected int missingUserCount = 0;

        protected void logMissingUser( Map<String, Object> nodeProps )
        {
            if ( missingUserCount++ < 10 )
            {
                System.err.println( "Missing user or uid: "
                                    + nodeProps.toString() );
            }
        }

        private class LogCounter
        {
            private long count = 0;
            private long totalTime = 0;
        }

        protected void logNodeFoundFrom( String key )
        {
            LogCounter counter = nodeFindStats.get( key );
            if ( counter == null )
            {
                counter = new LogCounter();
                nodeFindStats.put( key, counter );
            }
            counter.count++;
            foundOSMNodes++;
            long currentTime = System.currentTimeMillis();
            if ( lastFindTime > 0 )
            {
                counter.totalTime += currentTime - lastFindTime;
            }
            lastFindTime = currentTime;
            logNodesFound( currentTime );
        }

        protected void logNodesFound( long currentTime )
        {
            if ( firstFindTime == 0 )
            {
                firstFindTime = currentTime;
                findTime = currentTime;
            }
            if ( currentTime == 0 || currentTime - findTime > 1432 )
            {
                int duration = 0;
                if ( currentTime > 0 )
                {
                    duration = (int) ( ( currentTime - firstFindTime ) / 1000 );
                }
                System.out.println( new Date( currentTime ) + ": Found "
                                    + foundOSMNodes + " nodes during "
                                    + duration + "s way creation: " );
                for ( String type : nodeFindStats.keySet() )
                {
                    LogCounter found = nodeFindStats.get( type );
                    double rate = 0.0f;
                    if ( found.totalTime > 0 )
                    {
                        rate = ( 1000.0 * (float) found.count / (float) found.totalTime );
                    }
                    System.out.println( "\t" + type + ": \t" + found.count
                                        + "/" + ( found.totalTime / 1000 )
                                        + "s" + " \t(" + rate
                                        + " nodes/second)" );
                }
                findTime = currentTime;
            }
        }

        protected void logNodeAddition( LinkedHashMap<String, Object> tags,
                String type )
        {
            Integer count = stats.get( type );
            if ( count == null )
            {
                count = 1;
            }
            else
            {
                count++;
            }
            stats.put( type, count );
            long currentTime = System.currentTimeMillis();
            if ( firstLogTime == 0 )
            {
                firstLogTime = currentTime;
                logTime = currentTime;
            }
            if ( currentTime - logTime > 1432 )
            {
                System.out.println( new Date( currentTime )
                                    + ": Saving "
                                    + type
                                    + " "
                                    + count
                                    + " \t("
                                    + ( 1000.0 * (float) count / (float) ( currentTime - firstLogTime ) )
                                    + " " + type + "/second)" );
                logTime = currentTime;
            }
        }

        void describeLoaded()
        {
            logNodesFound( 0 );
            for ( String type : new String[] { "node", "way", "relation" } )
            {
                Integer count = stats.get( type );
                if ( count != null )
                {
                    System.out.println( "Loaded " + count + " " + type + "s" );
                }
            }
        }

        protected abstract long getDatasetId();

        private int missingNodeCount = 0;

        private void missingNode( long ndRef )
        {
            if ( missingNodeCount++ < 10 )
            {
                osmImporter.error( "Cannot find node for osm-id " + ndRef );
            }
        }

        private void describeMissing()
        {
            if ( missingNodeCount > 0 )
            {
                osmImporter.error( "When processing the ways, there were "
                                   + missingNodeCount + " missing nodes" );
            }
            if ( missingMemberCount > 0 )
            {
                osmImporter.error( "When processing the relations, there were "
                                   + missingMemberCount + " missing members" );
            }
        }

        private int missingMemberCount = 0;

        private void missingMember( String description )
        {
            if ( missingMemberCount++ < 10 )
            {
                osmImporter.error( "Cannot find member: " + description );
            }
        }

        protected T currentNode = null;
        protected T prev_way = null;
        protected T prev_relation = null;
        protected int nodeCount = 0;
        protected int poiCount = 0;
        protected int wayCount = 0;
        protected int relationCount = 0;
        protected int userCount = 0;
        protected int changesetCount = 0;

        /**
         * Add the BBox metadata to the dataset
         * 
         * @param bboxProperties
         */
        protected void addOSMBBox( Map<String, Object> bboxProperties )
        {
            T bbox = addNode( PROP_BBOX, bboxProperties, null );
            createRelationship( osm_dataset, bbox, OSMRelation.BBOX );
        }

        /**
         * Create a new OSM node from the specified attributes (including
         * location, user, changeset). The node is stored in the currentNode
         * field, so that it can be used in the subsequent call to
         * addOSMNodeTags after we close the XML tag for OSM nodes.
         * 
         * @param nodeProps HashMap of attributes for the OSM-node
         */
        protected void createOSMNode( Map<String, Object> nodeProps )
        {
            T changesetNode = getChangesetNode( nodeProps );
            currentNode = addNode( "node", nodeProps, "node_osm_id" );
            createRelationship( currentNode, changesetNode,
                    OSMRelation.CHANGESET );
            nodeCount++;
            debugNodeWithId( currentNode, "node_osm_id", new long[] { 8090260,
                    273534207 } );
        }

        private void addOSMNodeTags( boolean allPoints,
                LinkedHashMap<String, Object> currentNodeTags )
        {
            currentNodeTags.remove( "created_by" ); // redundant information
            // Nodes with tags get added to the index as point geometries
            if ( allPoints || currentNodeTags.size() > 0 )
            {
                Map<String, Object> nodeProps = getNodeProperties( currentNode );
                Envelope bbox = new Envelope();
                double[] location = new double[] {
                        (Double) nodeProps.get( "lon" ),
                        (Double) nodeProps.get( "lat" ) };
                bbox.expandToInclude( location[0], location[1] );
                addNodeGeometry( currentNode, GTYPE_POINT, bbox, 1 );
                poiCount++;
            }
            addNodeTags( currentNode, currentNodeTags, "node" );
        }

        protected void debugNodeWithId( T node, String idName, long[] idValues )
        {
            Map<String, Object> nodeProperties = getNodeProperties( node );
            String node_osm_id = nodeProperties.get( idName ).toString();
            for ( long idValue : idValues )
            {
                if ( node_osm_id.equals( Long.toString( idValue ) ) )
                {
                    System.out.println( "Debug node: " + node_osm_id );
                }
            }
        }

        protected void createOSMWay( Map<String, Object> wayProperties,
                ArrayList<Long> wayNodes, LinkedHashMap<String, Object> wayTags )
        {
            RoadDirection direction = getRoadDirection( wayTags );
            String name = (String) wayTags.get( "name" );
            int geometry = GTYPE_LINESTRING;
            boolean isRoad = wayTags.containsKey( "highway" );
            if ( isRoad )
            {
                wayProperties.put( "oneway", direction.toString() );
                wayProperties.put( "highway", wayTags.get( "highway" ) );
            }
            if ( name != null )
            {
                // Copy name tag to way because this seems like a valuable
                // location for
                // such a property
                wayProperties.put( "name", name );
            }
            String way_osm_id = (String) wayProperties.get( "way_osm_id" );
            if ( way_osm_id.equals( "28338132" ) )
            {
                System.out.println( "Debug way: " + way_osm_id );
            }
            T changesetNode = getChangesetNode( wayProperties );
            T way = addNode( INDEX_NAME_WAY, wayProperties, "way_osm_id" );
            createRelationship( way, changesetNode, OSMRelation.CHANGESET );
            if ( prev_way == null )
            {
                createRelationship( osm_dataset, way, OSMRelation.WAYS );
            }
            else
            {
                createRelationship( prev_way, way, OSMRelation.NEXT );
            }
            prev_way = way;
            addNodeTags( way, wayTags, "way" );
            Envelope bbox = new Envelope();
            T firstNode = null;
            T prevNode = null;
            T prevProxy = null;
            Map<String, Object> prevProps = null;
            LinkedHashMap<String, Object> relProps = new LinkedHashMap<String, Object>();
            HashMap<String, Object> directionProps = new HashMap<String, Object>();
            directionProps.put( "oneway", true );
            for ( long nd_ref : wayNodes )
            {
                // long pointNode =
                // batchIndexService.getSingleNode("node_osm_id", nd_ref);
                T pointNode = getOSMNode( nd_ref, changesetNode );
                if ( pointNode == null )
                {
                    /*
                     * This can happen if we import not whole planet, so some referenced
                     * nodes will be unavailable
                     */
                    missingNode( nd_ref );
                    continue;
                }
                T proxyNode = createProxyNode();
                if ( firstNode == null )
                {
                    firstNode = pointNode;
                }
                if ( prevNode == pointNode )
                {
                    continue;
                }
                createRelationship( proxyNode, pointNode, OSMRelation.NODE,
                        null );
                Map<String, Object> nodeProps = getNodeProperties( pointNode );
                double[] location = new double[] {
                        (Double) nodeProps.get( "lon" ),
                        (Double) nodeProps.get( "lat" ) };
                bbox.expandToInclude( location[0], location[1] );
                if ( prevProxy == null )
                {
                    createRelationship( way, proxyNode, OSMRelation.FIRST_NODE );
                }
                else
                {
                    relProps.clear();
                    double[] prevLoc = new double[] {
                            (Double) prevProps.get( "lon" ),
                            (Double) prevProps.get( "lat" ) };

                    double length = distance( prevLoc[0], prevLoc[1],
                            location[0], location[1] );
                    relProps.put( "length", length );

                    // We default to bi-directional (and don't store direction
                    // in the
                    // way node), but if it is one-way we mark it as such, and
                    // define
                    // the direction using the relationship direction
                    if ( direction == RoadDirection.BACKWARD )
                    {
                        createRelationship( proxyNode, prevProxy,
                                OSMRelation.NEXT, relProps );
                    }
                    else
                    {
                        createRelationship( prevProxy, proxyNode,
                                OSMRelation.NEXT, relProps );
                    }
                }
                prevNode = pointNode;
                prevProxy = proxyNode;
                prevProps = nodeProps;
            }
            // if (prevNode > 0) {
            // batchGraphDb.createRelationship(way, prevNode,
            // OSMRelation.LAST_NODE, null);
            // }
            if ( firstNode != null && prevNode == firstNode )
            {
                geometry = GTYPE_POLYGON;
            }
            if ( wayNodes.size() < 2 )
            {
                geometry = GTYPE_POINT;
            }
            addNodeGeometry( way, geometry, bbox, wayNodes.size() );
            this.wayCount++;
        }

        private void createOSMRelation( Map<String, Object> relationProperties,
                ArrayList<Map<String, Object>> relationMembers,
                LinkedHashMap<String, Object> relationTags )
        {
            String name = (String) relationTags.get( "name" );
            if ( name != null )
            {
                // Copy name tag to way because this seems like a valuable
                // location for
                // such a property
                relationProperties.put( "name", name );
            }
            T relation = addNode( "relation", relationProperties,
                    "relation_osm_id" );
            if ( prev_relation == null )
            {
                createRelationship( osm_dataset, relation,
                        OSMRelation.RELATIONS );
            }
            else
            {
                createRelationship( prev_relation, relation, OSMRelation.NEXT );
            }
            prev_relation = relation;
            addNodeTags( relation, relationTags, "relation" );
            // We will test for cases that invalidate multilinestring further
            // down
            GeometryMetaData metaGeom = new GeometryMetaData(
                    GTYPE_MULTILINESTRING );
            T prevMember = null;
            LinkedHashMap<String, Object> relProps = new LinkedHashMap<String, Object>();
            for ( Map<String, Object> memberProps : relationMembers )
            {
                String memberType = (String) memberProps.get( "type" );
                long member_ref = Long.parseLong( memberProps.get( "ref" ).toString() );
                if ( memberType != null )
                {
                    T member = getSingleNode( memberType, memberType
                                                          + "_osm_id",
                            member_ref );
                    if ( null == member || prevMember == member )
                    {
                        /*
                         * This can happen if we import not whole planet, so some
                         * referenced nodes will be unavailable
                         */
                        missingMember( memberProps.toString() );
                        continue;
                    }
                    if ( member == relation )
                    {
                        osmImporter.error( "Cannot add relation to same member: relation["
                                           + relationTags
                                           + "] - member["
                                           + memberProps + "]" );
                        continue;
                    }
                    Map<String, Object> nodeProps = getNodeProperties( member );
                    if ( memberType.equals( "node" ) )
                    {
                        double[] location = new double[] {
                                (Double) nodeProps.get( "lon" ),
                                (Double) nodeProps.get( "lat" ) };
                        metaGeom.expandToIncludePoint( location );
                    }
                    else if ( memberType.equals( "nodes" ) )
                    {
                        System.err.println( "Unexpected 'nodes' member type" );
                    }
                    else
                    {
                        updateGeometryMetaDataFromMember( member, metaGeom,
                                nodeProps );
                    }
                    relProps.clear();
                    String role = (String) memberProps.get( "role" );
                    if ( role != null && role.length() > 0 )
                    {
                        relProps.put( "role", role );
                        if ( role.equals( "outer" ) )
                        {
                            metaGeom.setPolygon();
                        }
                    }
                    createRelationship( relation, member, OSMRelation.MEMBER,
                            relProps );
                    // members can belong to multiple relations, in multiple
                    // orders, so NEXT will clash (also with NEXT between ways
                    // in original way load)
                    // if (prevMember < 0) {
                    // batchGraphDb.createRelationship(relation, member,
                    // OSMRelation.MEMBERS, null);
                    // } else {
                    // batchGraphDb.createRelationship(prevMember, member,
                    // OSMRelation.NEXT, null);
                    // }
                    prevMember = member;
                }
                else
                {
                    System.err.println( "Cannot process invalid relation member: "
                                        + memberProps.toString() );
                }
            }
            if ( metaGeom.isValid() )
            {
                addNodeGeometry( relation, metaGeom.getGeometryType(),
                        metaGeom.getBBox(), metaGeom.getVertices() );
            }
            this.relationCount++;
        }

        /**
         * This method should be overridden by implementation that are able to
         * perform database or index optimizations when requested, like the
         * batch inserter.
         */
        protected void optimize()
        {
        }

        protected abstract T getSingleNode( String name, String string,
                Object value );

        protected abstract Map<String, Object> getNodeProperties( T member );

        protected abstract T getOSMNode( long osmId, T changesetNode );

        protected abstract void updateGeometryMetaDataFromMember( T member,
                GeometryMetaData metaGeom, Map<String, Object> nodeProps );

        protected abstract void finish();

        protected abstract T createProxyNode();

        protected abstract T getChangesetNode( Map<String, Object> nodeProps );

        protected abstract T getUserNode( Map<String, Object> nodeProps );

    }

    private static class OSMGraphWriter extends OSMWriter<Node>
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

        private OSMGraphWriter( GraphDatabaseService graphDb,
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
                tx.close();
                tx = null;
                checkCount = 0;
            }
        }

        private void checkTx()
        {
            if ( checkCount++ > txInterval || tx == null )
            {
                successTx();
                tx = graphDb.beginTx();
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
                osm_root = ReferenceNodes.getReferenceNode(graphDb, "osm_root" );
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
                LinkedHashMap<String, Object> tags, String type )
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
                if ( gtype == GTYPE_GEOMETRY )
                    gtype = vertices > 1 ? GTYPE_MULTIPOINT : GTYPE_POINT;
                Node geomNode = graphDb.createNode();
                geomNode.setProperty( "gtype", gtype );
                geomNode.setProperty( "vertices", vertices );
                geomNode.setProperty( PROP_BBOX, new double[] { bbox.getMinX(),
                        bbox.getMaxX(), bbox.getMinY(), bbox.getMaxY() } );
                node.createRelationshipTo( geomNode, OSMRelation.GEOM );
                statsManager.addGeomStats( gtype );
            }
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
                RelationshipType relType, LinkedHashMap<String, Object> relProps )
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
                    INDEX_NAME_CHANGESET ).toString() );
            getUserNode( nodeProps );
            if ( changeset != currentChangesetId )
            {
                currentChangesetId = changeset;
                IndexHits<Node> result = indexFor( INDEX_NAME_CHANGESET ).get(
                        INDEX_NAME_CHANGESET, currentChangesetId );
                if ( result.size() > 0 )
                {
                    currentChangesetNode = result.getSingle();
                }
                else
                {
                    LinkedHashMap<String, Object> changesetProps = new LinkedHashMap<String, Object>();
                    changesetProps.put( INDEX_NAME_CHANGESET,
                            currentChangesetId );
                    changesetProps.put( "timestamp",
                            nodeProps.get( "timestamp" ) );
                    currentChangesetNode = (Node) addNode(
                            INDEX_NAME_CHANGESET, changesetProps,
                            INDEX_NAME_CHANGESET );
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
                String name = nodeProps.remove( INDEX_NAME_USER ).toString();
                if ( uid != currentUserId )
                {
                    currentUserId = uid;
                    IndexHits<Node> result = indexFor( INDEX_NAME_USER ).get(
                            "uid", currentUserId );
                    if ( result.size() > 0 )
                    {
                        currentUserNode = indexFor( INDEX_NAME_USER ).get(
                                "uid", currentUserId ).getSingle();
                    }
                    else
                    {
                        LinkedHashMap<String, Object> userProps = new LinkedHashMap<String, Object>();
                        userProps.put( "uid", currentUserId );
                        userProps.put( "name", name );
                        userProps.put( "timestamp", nodeProps.get( "timestamp" ) );
                        currentUserNode = (Node) addNode( INDEX_NAME_USER,
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

    private static class OSMBatchWriter extends OSMWriter<Long>
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

        private OSMBatchWriter( BatchInserter batchGraphDb,
                StatsManager statsManager, OSMImporter osmImporter )
        {
            super( statsManager, osmImporter );
            this.batchInserter = batchGraphDb;
            this.batchIndexService = new LuceneBatchInserterIndexProviderNewImpl(
                    batchGraphDb );
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
                osm_root = batchInserter.createNode(map("name", "osm_root"), DynamicLabel.label("ReferenceNode"));
                osm_dataset = getOrCreateNode( name, "osm", osm_root,
                        OSMRelation.OSM );
            }
            return osm_dataset;
        }

        private long findNode( BatchInserter batchInserter, String name,
                long parent, RelationshipType relType )
        {
            for ( BatchRelationship relationship : batchInserter.getRelationships( parent ) )
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
                node = batchInserter.createNode( properties );
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

        @Override
        protected void addNodeTags( Long node,
                LinkedHashMap<String, Object> tags, String type )
        {
            logNodeAddition( tags, type );
            if ( node != null && node > 0 && tags.size() > 0 )
            {
                statsManager.addToTagStats( type, tags.keySet() );
                long id = batchInserter.createNode( tags );
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
                if ( gtype == GTYPE_GEOMETRY )
                    gtype = vertices > 1 ? GTYPE_MULTIPOINT : GTYPE_POINT;
                properties.put( "gtype", gtype );
                properties.put( "vertices", vertices );
                properties.put(
                        PROP_BBOX,
                        new double[] { bbox.getMinX(), bbox.getMaxX(),
                                bbox.getMinY(), bbox.getMaxY() } );
                long id = batchInserter.createNode( properties );
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
            long id = -1;
            if ( indexKey != null && properties.containsKey( indexKey ) )
            {
                Map<String, Object> props = new HashMap<String, Object>();
                props.put( indexKey, properties.get( indexKey ).toString() );
                properties.put( indexKey,
                        Long.parseLong( properties.get( indexKey ).toString() ) );
                id = batchInserter.createNode( properties );
                indexFor( name ).add( id, props );
            }
            else
            {
                id = batchInserter.createNode( properties );
            }
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
                id = batchInserter.createNode( properties );
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
                RelationshipType relType, LinkedHashMap<String, Object> relProps )
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
            if ( currentChangesetNode != changesetNode
                 || changesetNodes.isEmpty() )
            {
                currentChangesetNode = changesetNode;
                changesetNodes.clear();
                for ( BatchRelationship rel : batchInserter.getRelationships( changesetNode ) )
                {
                    if ( rel.getType().name().equals(
                            OSMRelation.CHANGESET.name() ) )
                    {
                        Long node = rel.getStartNode();
                        Map<String, Object> props = batchInserter.getNodeProperties( node );
                        Long nodeOsmId = (Long) props.get( "node_osm_id" );
                        if ( nodeOsmId != null )
                        {
                            changesetNodes.put( nodeOsmId, node );
                        }
                    }
                }
            }
            Long node = changesetNodes.get( osmId );
            if ( node == null )
            {
                logNodeFoundFrom( "node-index" );
                return indexFor( INDEX_NAME_NODE ).get( "node_osm_id", osmId ).getSingle();
            }
            else
            {
                logNodeFoundFrom( "changeset" );
                return node;
            }
        }

        @Override
        protected void updateGeometryMetaDataFromMember( Long member,
                GeometryMetaData metaGeom, Map<String, Object> nodeProps )
        {
            for ( BatchRelationship rel : batchInserter.getRelationships( member ) )
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
            return batchInserter.createNode( null );
        }

        @Override
        protected Long getChangesetNode( Map<String, Object> nodeProps )
        {
            long changeset = Long.parseLong( nodeProps.remove( "changeset" ).toString() );
            getUserNode( nodeProps );
            if ( changeset != currentChangesetId )
            {
                currentChangesetId = changeset;
                changesetNodes.clear();
                IndexHits<Long> results = indexFor( "changeset" ).get(
                        "changeset", currentChangesetId );
                if ( results.size() > 0 )
                {
                    currentChangesetNode = results.getSingle();
                }
                else
                {
                    LinkedHashMap<String, Object> changesetProps = new LinkedHashMap<String, Object>();
                    changesetProps.put( "changeset", currentChangesetId );
                    changesetProps.put( "timestamp",
                            nodeProps.get( "timestamp" ) );
                    currentChangesetNode = (Long) addNode( "changeset",
                            changesetProps, "changeset" );
                    indexFor( "changeset" ).flush();
                    if ( currentUserNode > 0 )
                    {
                        createRelationship( currentChangesetNode,
                                currentUserNode, OSMRelation.USER );
                    }
                }
                results.close();
            }
            return currentChangesetNode;
        }

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
                    IndexHits<Long> results = indexFor( INDEX_NAME_USER ).get(
                            "uid", currentUserId );
                    if ( results.size() > 0 )
                    {
                        currentUserNode = results.getSingle();
                    }
                    else
                    {
                        LinkedHashMap<String, Object> userProps = new LinkedHashMap<String, Object>();
                        userProps.put( "uid", currentUserId );
                        userProps.put( "name", name );
                        userProps.put( "timestamp", nodeProps.get( "timestamp" ) );
                        currentUserNode = (Long) addNode( "user", userProps,
                                "uid" );
                        indexFor( INDEX_NAME_USER ).flush();
                        if ( usersNode < 0 )
                        {
                            usersNode = batchInserter.createNode( MapUtils.EMPTY_MAP );
                            createRelationship( osm_dataset, usersNode,
                                    OSMRelation.USERS );
                        }
                        createRelationship( usersNode, currentUserNode,
                                OSMRelation.OSM_USER );
                    }
                    results.close();
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

    public void importFile( GraphDatabaseService database, String dataset)
            throws IOException, XMLStreamException
    {
        importFile( database, dataset, false, 5000, false );
    }

    public void importFile( GraphDatabaseService database, String dataset,
            int txInterval, boolean relaxedTxFlush ) throws IOException, XMLStreamException
    {
        importFile( database, dataset, false, txInterval, relaxedTxFlush );
    }

    public void importFile( GraphDatabaseService database, String dataset,
            boolean allPoints, int txInterval, boolean relaxedTxFlush ) throws IOException,
            XMLStreamException
    {
        importFile(
                OSMWriter.fromGraphDatabase( database, stats, this, txInterval, relaxedTxFlush ),
                dataset, allPoints, charset );
    }

    public void importFile( BatchInserter batchInserter, String dataset )
            throws IOException, XMLStreamException
    {
        importFile( batchInserter, dataset, false );
    }

    public void importFile( BatchInserter batchInserter, String dataset,
            boolean allPoints ) throws IOException, XMLStreamException
    {
        importFile( OSMWriter.fromBatchInserter( batchInserter, stats, this ),
                dataset, allPoints, charset );
    }

    public static class CountedFileReader extends InputStreamReader
    {
        private long length = 0;
        private long charsRead = 0;

        public CountedFileReader( String path, Charset charset )
                                                                throws FileNotFoundException
        {
            super( new FileInputStream( path ), charset );
            this.length = ( new File( path ) ).length();
        }

        public CountedFileReader( File file, Charset charset )
                                                              throws FileNotFoundException
        {
            super( new FileInputStream( file ), charset );
            this.length = file.length();
        }

        public long getCharsRead()
        {
            return charsRead;
        }

        public long getlength()
        {
            return length;
        }

        public double getProgress()
        {
            return length > 0 ? (double) charsRead / (double) length : 0;
        }

        public int getPercentRead()
        {
            return (int) ( 100.0 * getProgress() );
        }

        public int read( char[] cbuf, int offset, int length )
                throws IOException
        {
            int read = super.read( cbuf, offset, length );
            if ( read > 0 ) charsRead += read;
            return read;
        }
    }

    private int progress = 0;
    private long progressTime = 0;

    private void beginProgressMonitor( int length )
    {
        monitor.begin( length );
        progress = 0;
        progressTime = System.currentTimeMillis();
    }

    private void updateProgressMonitor( int currentProgress )
    {
        if ( currentProgress > this.progress )
        {
            long time = System.currentTimeMillis();
            if ( time - progressTime > 1000 )
            {
                monitor.worked( currentProgress - progress );
                progress = currentProgress;
                progressTime = time;
            }
        }
    }

    private void endProgressMonitor()
    {
        monitor.done();
        progress = 0;
        progressTime = 0;
    }

    public void setCharset( Charset charset )
    {
        this.charset = charset;
    }

    public void importFile( OSMWriter<?> osmWriter, String dataset,
            boolean allPoints, Charset charset ) throws IOException,
            XMLStreamException
    {
        System.out.println( "Importing with osm-writer: " + osmWriter );
        osmWriter.getOrCreateOSMDataset( layerName );
        osm_dataset = osmWriter.getDatasetId();

        long startTime = System.currentTimeMillis();
        long[] times = new long[] { 0L, 0L, 0L, 0L };
        javax.xml.stream.XMLInputFactory factory = javax.xml.stream.XMLInputFactory.newInstance();
        CountedFileReader reader = new CountedFileReader( dataset, charset );
        javax.xml.stream.XMLStreamReader parser = factory.createXMLStreamReader( reader );
        int countXMLTags = 0;
        beginProgressMonitor( 100 );
        setLogContext( dataset );
        boolean startedWays = false;
        boolean startedRelations = false;
        try
        {
            ArrayList<String> currentXMLTags = new ArrayList<String>();
            int depth = 0;
            Map<String, Object> wayProperties = null;
            ArrayList<Long> wayNodes = new ArrayList<Long>();
            Map<String, Object> relationProperties = null;
            ArrayList<Map<String, Object>> relationMembers = new ArrayList<Map<String, Object>>();
            LinkedHashMap<String, Object> currentNodeTags = new LinkedHashMap<String, Object>();
            while ( true )
            {
                updateProgressMonitor( reader.getPercentRead() );
                incrLogContext();
                int event = parser.next();
                if ( event == javax.xml.stream.XMLStreamConstants.END_DOCUMENT )
                {
                    break;
                }
                switch ( event )
                {
                case javax.xml.stream.XMLStreamConstants.START_ELEMENT:
                    currentXMLTags.add( depth, parser.getLocalName() );
                    String tagPath = currentXMLTags.toString();
                    if ( tagPath.equals( "[osm]" ) )
                    {
                        osmWriter.setDatasetProperties( extractProperties( parser ) );
                    }
                    else if ( tagPath.equals( "[osm, bounds]" ) )
                    {
                        osmWriter.addOSMBBox( extractProperties( PROP_BBOX, parser ) );
                    }
                    else if ( tagPath.equals( "[osm, node]" ) )
                    {
                        // <node id="269682538" lat="56.0420950"
                        // lon="12.9693483" user="sanna" uid="31450"
                        // visible="true" version="1" changeset="133823"
                        // timestamp="2008-06-11T12:36:28Z"/>
                    	boolean includeNode = true;
                    	Map<String, Object> nodeProperties = extractProperties( "node", parser );
                    	if(filterEnvelope!=null) {
                    		includeNode = filterEnvelope.contains((Double)nodeProperties.get("lon"), (Double)nodeProperties.get("lat"));
                    	}
                    	if (includeNode) {
                    		osmWriter.createOSMNode( nodeProperties );
                    	}
                    }
                    else if ( tagPath.equals( "[osm, way]" ) )
                    {
                        // <way id="27359054" user="spull" uid="61533"
                        // visible="true" version="8" changeset="4707351"
                        // timestamp="2010-05-15T15:39:57Z">
                        if ( !startedWays )
                        {
                            startedWays = true;
                            times[0] = System.currentTimeMillis();
                            osmWriter.optimize();
                            times[1] = System.currentTimeMillis();
                        }
                        wayProperties = extractProperties( "way", parser );
                        wayNodes.clear();
                    }
                    else if ( tagPath.equals( "[osm, way, nd]" ) )
                    {
                        Map<String, Object> properties = extractProperties( parser );
                        wayNodes.add( Long.parseLong( properties.get( "ref" ).toString() ) );
                    }
                    else if ( tagPath.endsWith( "tag]" ) )
                    {
                        Map<String, Object> properties = extractProperties( parser );
                        currentNodeTags.put( properties.get( "k" ).toString(),
                                properties.get( "v" ).toString() );
                    }
                    else if ( tagPath.equals( "[osm, relation]" ) )
                    {
                        // <relation id="77965" user="Grillo" uid="13957"
                        // visible="true" version="24" changeset="5465617"
                        // timestamp="2010-08-11T19:25:46Z">
                        if ( !startedRelations )
                        {
                            startedRelations = true;
                            times[2] = System.currentTimeMillis();
                            osmWriter.optimize();
                            times[3] = System.currentTimeMillis();
                        }
                        relationProperties = extractProperties( "relation",
                                parser );
                        relationMembers.clear();
                    }
                    else if ( tagPath.equals( "[osm, relation, member]" ) )
                    {
                        relationMembers.add( extractProperties( parser ) );
                    }
                    if ( startedRelations )
                    {
                        if ( countXMLTags < 10 )
                        {
                            log( "Starting tag at depth " + depth + ": "
                                 + currentXMLTags.get( depth ) + " - "
                                 + currentXMLTags.toString() );
                            for ( int i = 0; i < parser.getAttributeCount(); i++ )
                            {
                                log( "\t" + currentXMLTags.toString() + ": "
                                     + parser.getAttributeLocalName( i ) + "["
                                     + parser.getAttributeNamespace( i ) + ","
                                     + parser.getAttributePrefix( i ) + ","
                                     + parser.getAttributeType( i ) + ","
                                     + "] = " + parser.getAttributeValue( i ) );
                            }
                        }
                        countXMLTags++;
                    }
                    depth++;
                    break;
                case javax.xml.stream.XMLStreamConstants.END_ELEMENT:
                    if ( currentXMLTags.toString().equals( "[osm, node]" ) )
                    {
                    	osmWriter.addOSMNodeTags( allPoints, currentNodeTags );
                    }
                    else if ( currentXMLTags.toString().equals( "[osm, way]" ) )
                    {
                        osmWriter.createOSMWay( wayProperties, wayNodes,
                                currentNodeTags );
                    }
                    else if ( currentXMLTags.toString().equals(
                            "[osm, relation]" ) )
                    {
                        osmWriter.createOSMRelation( relationProperties,
                                relationMembers, currentNodeTags );
                    }
                    depth--;
                    currentXMLTags.remove( depth );
                    // log("Ending tag at depth "+depth+": "+currentTags.get(depth));
                    break;
                default:
                    break;
                }
            }
        }
        finally
        {
            endProgressMonitor();
            parser.close();
            osmWriter.finish();
            this.osm_dataset = osmWriter.getDatasetId();
        }
        describeTimes( startTime, times );
        osmWriter.describeMissing();
        osmWriter.describeLoaded();

        long stopTime = System.currentTimeMillis();
        log( "info | Elapsed time in seconds: "
             + ( 1.0 * ( stopTime - startTime ) / 1000.0 ) );
        stats.dumpGeomStats();
        stats.printTagStats();
    }

    private void describeTimes( long startTime, long[] times )
    {
        long endTime = System.currentTimeMillis();
        log( "Completed load in " + ( 1.0 * ( endTime - startTime ) / 1000.0 )
             + "s" );
        log( "\tImported nodes:  " + ( 1.0 * ( times[0] - startTime ) / 1000.0 )
             + "s" );
        log( "\tOptimized index: " + ( 1.0 * ( times[1] - times[0] ) / 1000.0 )
             + "s" );
        log( "\tImported ways:   " + ( 1.0 * ( times[2] - times[1] ) / 1000.0 )
             + "s" );
        log( "\tOptimized index: " + ( 1.0 * ( times[3] - times[2] ) / 1000.0 )
             + "s" );
        log( "\tImported rels:   " + ( 1.0 * ( endTime - times[3] ) / 1000.0 )
             + "s" );
    }

    private Map<String, Object> extractProperties( XMLStreamReader parser )
    {
        return extractProperties( null, parser );
    }

    private Map<String, Object> extractProperties( String name,
            XMLStreamReader parser )
    {
        // <node id="269682538" lat="56.0420950" lon="12.9693483" user="sanna"
        // uid="31450" visible="true" version="1" changeset="133823"
        // timestamp="2008-06-11T12:36:28Z"/>
        // <way id="27359054" user="spull" uid="61533" visible="true"
        // version="8" changeset="4707351" timestamp="2010-05-15T15:39:57Z">
        // <relation id="77965" user="Grillo" uid="13957" visible="true"
        // version="24" changeset="5465617" timestamp="2010-08-11T19:25:46Z">
        LinkedHashMap<String, Object> properties = new LinkedHashMap<String, Object>();
        for ( int i = 0; i < parser.getAttributeCount(); i++ )
        {
            String prop = parser.getAttributeLocalName( i );
            String value = parser.getAttributeValue( i );
            if ( name != null && prop.equals( "id" ) )
            {
                prop = name + "_osm_id";
                name = null;
            }
            if ( prop.equals( "lat" ) || prop.equals( "lon" ) )
            {
                properties.put( prop, Double.parseDouble( value ) );
            }
            else if ( name != null && prop.equals( "version" ) )
            {
                properties.put( prop, Integer.parseInt( value ) );
            }
            else if ( prop.equals( "visible" ) )
            {
                if ( !value.equals( "true" ) && !value.equals( "1" ) )
                {
                    properties.put( prop, false );
                }
            }
            else if ( prop.equals( "timestamp" ) )
            {
                try
                {
                    Date timestamp = timestampFormat.parse( value );
                    properties.put( prop, timestamp.getTime() );
                }
                catch ( ParseException e )
                {
                    error( "Error parsing timestamp", e );
                }
            }
            else
            {
                properties.put( prop, value );
            }
        }
        if ( name != null )
        {
            properties.put( "name", name );
        }
        return properties;
    }

    /**
     * Retrieves the direction of the given road, i.e. whether it is a one-way road from its start node,
     * a one-way road to its start node or a two-way road.
     * @param wayProperties the property map of the road
     * @return BOTH if it's a two-way road, FORWARD if it's a one-way road from the start node,
     * or BACKWARD if it's a one-way road to the start node
     */
    public static RoadDirection getRoadDirection( Map<String, Object> wayProperties )
    {
        String oneway = (String) wayProperties.get( "oneway" );
        if ( null != oneway )
        {
            if ( "-1".equals( oneway ) ) return RoadDirection.BACKWARD;
            if ( "1".equals( oneway ) || "yes".equalsIgnoreCase( oneway )
                 || "true".equalsIgnoreCase( oneway ) )
                return RoadDirection.FORWARD;
        }
        return RoadDirection.BOTH;
    }

    /**
     * Calculate correct distance between 2 points on Earth.
     * 
     * @param latA
     * @param lonA
     * @param latB
     * @param lonB
     * @return distance in meters
     */
    public static double distance( double lonA, double latA, double lonB,
            double latB )
    {
        return WGS84.orthodromicDistance( lonA, latA, lonB, latB );
    }

    private void log( PrintStream out, String message, Exception e )
    {
        if ( logContext != null )
        {
            message = logContext + "[" + contextLine + "]: " + message;
        }
        out.println( message );
        if ( e != null )
        {
            e.printStackTrace( out );
        }
    }

    private void log( String message )
    {
        log( System.out, message, null );
    }

    private void error( String message )
    {
        log( System.err, message, null );
    }

    private void error( String message, Exception e )
    {
        log( System.err, message, e );
    }

    private String logContext = null;
    private int contextLine = 0;

    // "2008-06-11T12:36:28Z"
    private DateFormat timestampFormat = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss'Z'" );

    private void setLogContext( String context )
    {
        logContext = context;
        contextLine = 0;
    }

    private void incrLogContext()
    {
        contextLine++;
    }

    /**
     * This method allows for a console, command-line application for loading
     * one or more *.osm files into a new database.
     * 
     * @param args , the database directory followed by one or more osm files
     */
    public static void main( String[] args )
    {
        if ( args.length < 2 )
        {
            System.out.println( "Usage: osmimporter databasedir osmfile <..osmfiles..>" );
        }
        else
        {
            OSMImportManager importer = new OSMImportManager( args[0] );
            for ( int i = 1; i < args.length; i++ )
            {
                try
                {
                    importer.loadTestOsmData( args[i], 5000 );
                }
                catch ( Exception e )
                {
                    System.err.println( "Error importing OSM file '" + args[i]
                                        + "': " + e );
                    e.printStackTrace();
                }
                finally
                {
                    importer.shutdown();
                }
            }
        }
    }

    private static class OSMImportManager
    {
        private GraphDatabaseService graphDb;
        private BatchInserter batchInserter;
        private File dbPath;
        private boolean useBatchInserter = false;

        public OSMImportManager( String path )
        {
            setDbPath( path );
        }

        public void setDbPath( String path )
        {
            dbPath = new File( path );
            if ( dbPath.exists() )
            {
                if ( !dbPath.isDirectory() )
                {
                    throw new RuntimeException(
                            "Database path is an existing file: "
                                    + dbPath.getAbsolutePath() );
                }
            }
            else
            {
                dbPath.mkdirs();
            }
        }

        private void loadTestOsmData( String layerName, int commitInterval )
                throws Exception
        {
            String osmPath = layerName;
            System.out.println( "\n=== Loading layer " + layerName + " from "
                                + osmPath + " ===" );
            long start = System.currentTimeMillis();
            if ( useBatchInserter )
            {
                switchToBatchInserter();
                OSMImporter importer = new OSMImporter( layerName );
                importer.importFile( batchInserter, osmPath );
	        switchToEmbeddedGraphDatabase();
                importer.reIndex( graphDb, commitInterval );
            }
            else
            {
                switchToEmbeddedGraphDatabase();
	        OSMImporter importer = new OSMImporter( layerName );
	        importer.importFile( graphDb, osmPath, false, commitInterval, true );
	        importer.reIndex( graphDb, commitInterval );
            }
            shutdown();
            System.out.println( "=== Completed loading " + layerName + " in "
                                + ( System.currentTimeMillis() - start )
                                / 1000.0 + " seconds ===" );
        }

        private void switchToEmbeddedGraphDatabase()
        {
            shutdown();
            graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( dbPath.getAbsolutePath() );
        }

        private void switchToBatchInserter()
        {
            shutdown();
            batchInserter = BatchInserters.inserter(dbPath.getAbsolutePath());
	    //graphDb = new TestGraphDatabaseFactory().setFileSystem(Default)newImpermanentDatabase( dbPath.getAbsolutePath() );
	    graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( dbPath.getAbsolutePath() );
        }

        protected void shutdown()
        {
            if ( graphDb != null )
            {
                graphDb.shutdown();
                // batch
                graphDb = null;
                batchInserter = null;
            }
        }
    }
}
