package org.neo4j.gis.spatial;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.geotools.referencing.datum.DefaultEllipsoid;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.lucene.LuceneIndexBatchInserter;
import org.neo4j.index.lucene.LuceneIndexBatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.SimpleRelationship;

import com.vividsolutions.jts.geom.GeometryFactory;

public class OSMImporter implements Constants {
    public static DefaultEllipsoid WGS84 = DefaultEllipsoid.WGS84;

    public enum EOSMRelation implements RelationshipType {
        NODES, OTHER, NEXT, OSM;
    }

    public enum EDirection {
        BOTH, FORWARD, BACKWARD;
    }

    protected static final List<String> NODE_INDEXING_KEYS = new ArrayList<String>();
    static {
        NODE_INDEXING_KEYS.add("node_osm_id");
    }

    protected LuceneIndexBatchInserter batchIndexService;

    protected boolean nodesProcessingFinished = false;
    private String layerName;
    private long osm_root;
    private long osm_layer;

    public OSMImporter(String layerName) {
        this.layerName = layerName;
    }

    private Iterable<Node> getWays() {
        ArrayList<Node> ways = new ArrayList<Node>();
        return ways;
    }

    public void reIndex(GraphDatabaseService database, int commitInterval) {
        if (commitInterval < 1)
            throw new IllegalArgumentException("commitInterval must be >= 1");

        SpatialDatabaseService spatialDatabase = new SpatialDatabaseService(database);
        Layer layer = getOrCreateLayer(database, spatialDatabase, layerName);
        GeometryFactory geomFactory = layer.getGeometryFactory();

        long startTime = System.currentTimeMillis();
        Transaction tx = database.beginTx();
        int count = 0;
        try {
            for (Node way : getWays()) {

                if (++count % commitInterval == 0) {
                    tx.success();
                    tx.finish();
                    tx = database.beginTx();
                }
            } // TODO ask charset to user?
            tx.success();
        } finally {
            tx.finish();
        }

        long stopTime = System.currentTimeMillis();
        log("info | Re-indexing elapsed time in seconds: " + (1.0 * (stopTime - startTime) / 1000.0));
    }

    @SuppressWarnings("restriction")
    public void importFile(BatchInserter batchGraphDb, String dataset) throws IOException, XMLStreamException {
        batchIndexService = new LuceneIndexBatchInserterImpl(batchGraphDb);
        getOrCreateOSMLayer(batchGraphDb, layerName);

        long startTime = System.currentTimeMillis();
        javax.xml.stream.XMLInputFactory factory = javax.xml.stream.XMLInputFactory.newInstance();
        javax.xml.stream.XMLStreamReader parser = factory.createXMLStreamReader(new FileReader(dataset));
        int count = 0;
        boolean batchIndexServiceOptimized = false;
        try {
            ArrayList<String> currentTags = new ArrayList<String>();
            int depth = 0;
            long currentNode = -1;
            long prev_way = -1;
            Map<String, Object> wayProperties = null;
            ArrayList<Long> wayNodes = new ArrayList<Long>();
            LinkedHashMap<String, Object> currentNodeTags = new LinkedHashMap<String, Object>();
            while (true && count < 1000) {
                int event = parser.next();
                if (event == javax.xml.stream.XMLStreamConstants.END_DOCUMENT) {
                    break;
                }
                switch (event) {
                case javax.xml.stream.XMLStreamConstants.START_ELEMENT:
                    currentTags.add(depth, parser.getLocalName());
                    String tagPath = currentTags.toString();
                    if (tagPath.equals("[osm]")) {
                        LinkedHashMap<String,Object> properties = new LinkedHashMap<String,Object>();
                        properties.putAll(batchGraphDb.getNodeProperties(osm_layer));
                        properties.putAll(extractProperties(parser));
                        batchGraphDb.setNodeProperties(osm_layer, properties);
                    } else if (tagPath.equals("[osm, bounds]")) {
                        long bbox = addNode(batchGraphDb, "bbox", parser, null);
                        batchGraphDb.createRelationship(osm_layer, bbox, DynamicRelationshipType.withName("BBOX"), null);
                    } else if (tagPath.equals("[osm, node]")) {
                        currentNode = addNode(batchGraphDb, "node", parser, "node_osm_id");
                        batchIndexServiceOptimized = false;
                    } else if (tagPath.equals("[osm, way]")) {
                        if (!batchIndexServiceOptimized) {
                            batchIndexService.optimize();
                            batchIndexServiceOptimized = true;
                        }
                        wayProperties = extractProperties("way", parser);
                        wayNodes.clear();
                    } else if (tagPath.equals("[osm, way, nd]")) {
                        Map<String, Object> properties = extractProperties(parser);
                        wayNodes.add(Long.parseLong(properties.get("ref").toString()));
                    } else if (tagPath.endsWith("tag]")) {
                        Map<String, Object> properties = extractProperties(parser);
                        currentNodeTags.put(properties.get("k").toString(), properties.get("v").toString());
                    }
                    if (batchIndexServiceOptimized) {
                        if (count < 10) {
                            System.out.println("Starting tag at depth " + depth + ": " + currentTags.get(depth) + " - "
                                    + currentTags.toString());
                            for (int i = 0; i < parser.getAttributeCount(); i++) {
                                System.out.println("\t" + currentTags.toString() + ": " + parser.getAttributeLocalName(i) + "["
                                        + parser.getAttributeNamespace(i) + "," + parser.getAttributePrefix(i) + ","
                                        + parser.getAttributeType(i) + "," + "] = " + parser.getAttributeValue(i));
                            }
                        }
                        count++;
                    }
                    depth++;
                    break;
                case javax.xml.stream.XMLStreamConstants.END_ELEMENT:
                    if (currentTags.toString().equals("[osm, node]")) {
                        addNodeTags(batchGraphDb, currentNode, currentNodeTags);
                    } else if (currentTags.toString().equals("[osm, way]")) {
                        EDirection direction = isOneway(currentNodeTags);
                        boolean isRoad = currentNodeTags.containsKey("highway");
                        if (isRoad) {
                            wayProperties.put("oneway", direction.toString());
                            wayProperties.put("highway", currentNodeTags.get("highway"));
                        }
                        long way = addNode(batchGraphDb, "way", wayProperties, "way_osm_id");
                        if (prev_way < 0) {
                            batchGraphDb.createRelationship(osm_layer, way, EOSMRelation.OSM, null);
                        } else {
                            batchGraphDb.createRelationship(prev_way, way, EOSMRelation.NEXT, null);
                        }
                        prev_way = way;
                        addNodeTags(batchGraphDb, way, currentNodeTags);
                        long prevNode = -1;
                        LinkedHashMap<String, Object> relProps = new LinkedHashMap<String, Object>();
                        HashMap<String, Object> directionProps = new HashMap<String, Object>();
                        directionProps.put("oneway", true);
                        for (long nd_ref : wayNodes) {
                            long node = batchIndexService.getSingleNode("node_osm_id", nd_ref);
                            if (-1 == node || prevNode == node) {
                                /*
                                 * This can happen if we import not whole planet, so some referenced
                                 * nodes will be unavailable
                                 */
                                System.out.println("Cannot find node for osm-id: " + nd_ref);
                                continue;
                            }
                            if (prevNode < 0) {
                                batchGraphDb.createRelationship(way, node, EOSMRelation.NODES, null);
                            } else {
                                relProps.clear();
                                Map<String, Object> prevProps = batchGraphDb.getNodeProperties(prevNode);
                                Map<String, Object> nodeProps = batchGraphDb.getNodeProperties(node);

                                double length = distance((Double)prevProps.get("lat"), (Double)prevProps.get("lon"),
                                        (Double)nodeProps.get("lat"), (Double)nodeProps.get("lon"));
                                relProps.put("length", length);

                                // We default to bi-directional (and don't store direction in the
                                // way node), but if it is one-way we mark it as such, and define
                                // the direction using the relationship direction
                                if (direction == EDirection.BACKWARD) {
                                    batchGraphDb.createRelationship(node, prevNode, EOSMRelation.NEXT, relProps);
                                } else {
                                    batchGraphDb.createRelationship(prevNode, node, EOSMRelation.NEXT, relProps);
                                }
                            }
                            prevNode = node;
                        }
                    }
                    depth--;
                    currentTags.remove(depth);
                    // System.out.println("Ending tag at depth "+depth+": "+currentTags.get(depth));
                    break;
                default:
                    break;
                }
            }
        } finally {
            parser.close();
        }

        long stopTime = System.currentTimeMillis();
        log("info | Elapsed time in seconds: " + (1.0 * (stopTime - startTime) / 1000.0));
    }

    private void addNodeTags(BatchInserter batchGraphDb, long currentNode, LinkedHashMap<String, Object> currentNodeTags) {
        if (currentNode > 0 && currentNodeTags.size() > 0) {
            long id = batchGraphDb.createNode(currentNodeTags);
            batchGraphDb.createRelationship(currentNode, id, DynamicRelationshipType.withName("TAGS"), null);
            currentNodeTags.clear();
        }
    }

    private long addNode(BatchInserter batchInserter, String name, XMLStreamReader parser, String indexKey) {
        return addNode(batchInserter, name, extractProperties(name, parser), indexKey);
    }

    private long addNode(BatchInserter batchInserter, String name, Map<String, Object> properties, String indexKey) {
        long id = batchInserter.createNode(properties);
        if (indexKey != null && properties.containsKey(indexKey)) {
            batchIndexService.index(id, indexKey, properties.get(indexKey));
        }
        return id;
    }

    private Map<String, Object> extractProperties(XMLStreamReader parser) {
        return extractProperties(null, parser);
    }

    private Map<String, Object> extractProperties(String name, XMLStreamReader parser) {
        LinkedHashMap<String, Object> properties = new LinkedHashMap<String, Object>();
        for (int i = 0; i < parser.getAttributeCount(); i++) {
            String prop = parser.getAttributeLocalName(i);
            if (name != null && prop.equals("id")) {
                prop = name + "_osm_id";
                name = null;
            }
            if (prop.equals("lat") || prop.equals("lon")) {
                properties.put(prop, Double.parseDouble(parser.getAttributeValue(i)));
            } else {
                properties.put(prop, parser.getAttributeValue(i));
            }
        }
        if (name != null) {
            properties.put("name", name);
        }
        return properties;
    }

    // Private methods

    private Layer getOrCreateLayer(GraphDatabaseService database, SpatialDatabaseService spatialDatabase, String layerName) {
        Layer layer;
        Transaction tx = database.beginTx();
        try {
            if (spatialDatabase.containsLayer(layerName)) {
                layer = spatialDatabase.getLayer(layerName);
            } else {
                layer = spatialDatabase.createLayer(layerName);
            }
            tx.success();
        } finally {
            tx.finish();
        }
        return layer;
    }

    private Node findNode(GraphDatabaseService database, String name, Node parent, RelationshipType relType) {
        for (Relationship relationship : parent.getRelationships(relType, Direction.OUTGOING)) {
            Node node = relationship.getEndNode();
            if (name.equals(node.getProperty("name"))) {
                return node;
            }
        }
        return null;
    }

    private long findNode(BatchInserter batchInserter, String name, long parent, RelationshipType relType) {
        for (SimpleRelationship relationship : batchInserter.getRelationships(parent)) {
            if(relationship.getType() == relType) {
                long node = relationship.getEndNode();
                Object nodeName = batchInserter.getNodeProperties(node).get("name");
                if (nodeName != null && name.equals(nodeName.toString())) {
                    return node;
                }
            }
        }
        return -1;
    }

    private Node getOrCreateNode(GraphDatabaseService database, String name, Node parent, RelationshipType relType) {
        Node node = findNode(database, name, parent, relType);
        if (node == null) {
            node = database.createNode();
            node.setProperty("name", name);
            parent.createRelationshipTo(node, relType);
        }
        return node;
    }

    private long getOrCreateNode(BatchInserter batchInserter, String name, long parent, RelationshipType relType) {
        long node = findNode(batchInserter, name, parent, relType);
        if (node < 0) {
            HashMap<String,Object> properties = new HashMap<String,Object>();
            properties.put("name", name);
            node = batchInserter.createNode(properties);
            batchInserter.createRelationship(parent, node, relType, null);
        }
        return node;
    }

    public long getOrCreateOSMLayer(BatchInserter batchInserter, String name) {
        if (osm_layer <= 0) {
            osm_root = getOrCreateNode(batchInserter, "osm_root", batchInserter.getReferenceNode(), EOSMRelation.OSM);
            osm_layer = getOrCreateNode(batchInserter, name, osm_root, EOSMRelation.OSM);
        }
        return osm_layer;
    }

    /**
     * Detects if road has the only direction
     * 
     * @param wayProperties
     * @return 0 - road is opened in both directions <br/>
     *         1 - road is oneway (forward direction)<br/>
     *         2 - road is oneway (backward direction)
     */
    public static EDirection isOneway(Map<String, Object> wayProperties) {
        String oneway = (String)wayProperties.get("oneway");
        if (null != oneway) {
            if ("-1".equals(oneway))
                return EDirection.BACKWARD;
            if ("1".equals(oneway) || "yes".equalsIgnoreCase(oneway) || "true".equalsIgnoreCase(oneway))
                return EDirection.FORWARD;
        }
        return EDirection.BOTH;
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
    public static double distance(double latA, double lonA, double latB, double lonB) {
        return WGS84.orthodromicDistance(lonA, latA, lonB, latB);
    }

    private void log(String message) {
        System.out.println(message);
    }

    private void log(String message, Exception e) {
        System.out.println(message);
        e.printStackTrace();
    }

}
