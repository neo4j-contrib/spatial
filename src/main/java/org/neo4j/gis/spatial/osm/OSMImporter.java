package org.neo4j.gis.spatial.osm;

import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.geotools.referencing.datum.DefaultEllipsoid;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.index.lucene.LuceneIndexBatchInserter;
import org.neo4j.index.lucene.LuceneIndexBatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.SimpleRelationship;

import com.vividsolutions.jts.geom.Envelope;

public class OSMImporter implements Constants {
    public static DefaultEllipsoid WGS84 = DefaultEllipsoid.WGS84;

    private static HashMap<String, String[]> geometryTypesMap = new LinkedHashMap<String, String[]>();
    private static HashMap<String, String> tagKeyToGeometryMap = new LinkedHashMap<String, String>();
    static {
        geometryTypesMap.put("Point", new String[] {});
        geometryTypesMap.put("MultiPoint", new String[] {});
        geometryTypesMap.put("LineString", new String[] {"highway", "boundary"});
        geometryTypesMap.put("MultiLineString", new String[] {});
        geometryTypesMap.put("Polygon", new String[] {});
        geometryTypesMap.put("MultiPolygon", new String[] {});
        for (String geom : geometryTypesMap.keySet()) {
            String[] tagKeys = geometryTypesMap.get(geom);
            for (String key : tagKeys) {
                tagKeyToGeometryMap.put(key, geom);
            }
        }
    }

    private static String getGeomFromTags(Map<String, Object> tags) {
        // First process any "type" tag (as used in relation tags)
        String type = (String)tags.get("type");
        if (type != null) {
            for (String key : tagKeyToGeometryMap.keySet()) {
                if (key.toLowerCase().equals(type)) {
                    return key;
                }
            }
        }
        // Then try look for existence of some known tags (as used in way tags)
        for (String key : tagKeyToGeometryMap.keySet()) {
            if (tags.keySet().contains(key)) {
                return tagKeyToGeometryMap.get(key);
            }
        }
        return null;
    }

    protected static final List<String> NODE_INDEXING_KEYS = new ArrayList<String>();
    static {
        NODE_INDEXING_KEYS.add("node_osm_id");
    }

    protected LuceneIndexBatchInserter batchIndexService;

    protected boolean nodesProcessingFinished = false;
    private String layerName;
    private long osm_root;
    private long osm_dataset;
    private HashMap<String,TagStats> tagStats = new HashMap<String,TagStats>();
    
	private class TagStats {
		private String name;
		private int count = 0;
		private HashMap<String, Integer> stats = new HashMap<String, Integer>();

		TagStats(String name) {
			this.name = name;
		}

		int add(String key) {
			count++;
			if (stats.containsKey(key)) {
				int num = stats.get(key);
				stats.put(key, ++num);
				return num;
			} else {
				stats.put(key, 1);
				return 1;
			}
		}

		/**
		 * Return only reasonably commonly used tags.
		 * 
		 * @return
		 */
		public String[] getTags() {
			if (stats.size() > 0) {
				int threshold = count / (stats.size() * 3);
				ArrayList<String> tags = new ArrayList<String>();
				for (String key : stats.keySet()) {
					if (stats.get(key) > threshold)
						tags.add(key);
				}
				Collections.sort(tags);
				return tags.toArray(new String[tags.size()]);
			} else {
				return new String[0];
			}
		}

		public String toString() {
			return "TagStats[" + name + "]: " + getTags();
		}
	}

    public OSMImporter(String layerName) {
        this.layerName = layerName;
    }

    public void reIndex(GraphDatabaseService database, int commitInterval) {
        if (commitInterval < 1)
            throw new IllegalArgumentException("commitInterval must be >= 1");

        setLogContext("Index");
        SpatialDatabaseService spatialDatabase = new SpatialDatabaseService(database);
        OSMLayer layer = (OSMLayer)spatialDatabase.getOrCreateLayer(layerName, OSMGeometryEncoder.class, OSMLayer.class);
        OSMDataset dataset = layer.getDataset(osm_dataset);
        layer.clear();  // clear the index without destroying underlying data

        long startTime = System.currentTimeMillis();
        Transaction tx = database.beginTx();
        int count = 0;
        try {
            layer.setExtraPropertyNames(tagStats.get("way").getTags());
            for (Node way : database.getNodeById(osm_dataset).traverse(Order.DEPTH_FIRST, StopEvaluator.END_OF_GRAPH,
                    ReturnableEvaluator.ALL_BUT_START_NODE, OSMRelation.WAYS, Direction.OUTGOING, OSMRelation.NEXT,
                    Direction.OUTGOING)) {
                incrLogContext();
                layer.addWay(way);
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

    private int addToTagStats(String type, String key) {
    	if(!tagStats.containsKey(type)) {
    		tagStats.put(type,new TagStats(type));
    	}
    	return tagStats.get(type).add(key);
    }

    private int addToTagStats(String type, Collection<String> keys) {
    	int count = 0;
    	for(String  key:keys) {
    		count += addToTagStats(type, key);
    	}
    	return count;
    }

    @SuppressWarnings("restriction")
    public void importFile(BatchInserter batchGraphDb, String dataset) throws IOException, XMLStreamException {
        batchIndexService = new LuceneIndexBatchInserterImpl(batchGraphDb);
        getOrCreateOSMDataset(batchGraphDb, layerName);

        long startTime = System.currentTimeMillis();
        javax.xml.stream.XMLInputFactory factory = javax.xml.stream.XMLInputFactory.newInstance();
        javax.xml.stream.XMLStreamReader parser = factory.createXMLStreamReader(new FileReader(dataset));
        int count = 0;
        setLogContext(dataset);
        boolean startedWays = false;
        boolean startedRelations = false;
        try {
            ArrayList<String> currentXMLTags = new ArrayList<String>();
            int depth = 0;
            long currentNode = -1;
            long prev_way = -1;
            long prev_relation = -1;
            Map<String, Object> wayProperties = null;
            ArrayList<Long> wayNodes = new ArrayList<Long>();
            Map<String, Object> relationProperties = null;
            ArrayList<Map<String, Object>> relationMembers = new ArrayList<Map<String, Object>>();
            LinkedHashMap<String, Object> currentNodeTags = new LinkedHashMap<String, Object>();
            while (true) {
                incrLogContext();
                int event = parser.next();
                if (event == javax.xml.stream.XMLStreamConstants.END_DOCUMENT) {
                    break;
                }
                switch (event) {
                case javax.xml.stream.XMLStreamConstants.START_ELEMENT:
                    currentXMLTags.add(depth, parser.getLocalName());
                    String tagPath = currentXMLTags.toString();
                    if (tagPath.equals("[osm]")) {
                        LinkedHashMap<String, Object> properties = new LinkedHashMap<String, Object>();
                        properties.putAll(batchGraphDb.getNodeProperties(osm_dataset));
                        properties.putAll(extractProperties(parser));
                        batchGraphDb.setNodeProperties(osm_dataset, properties);
                    } else if (tagPath.equals("[osm, bounds]")) {
                        long bbox = addNode(batchGraphDb, "bbox", parser, null);
                        batchGraphDb.createRelationship(osm_dataset, bbox, OSMRelation.BBOX, null);
                    } else if (tagPath.equals("[osm, node]")) {
                        currentNode = addNode(batchGraphDb, "node", parser, "node_osm_id");
                    } else if (tagPath.equals("[osm, way]")) {
                        if (!startedWays) {
                            batchIndexService.optimize();
                            startedWays = true;
                        }
                        wayProperties = extractProperties("way", parser);
                        wayNodes.clear();
                    } else if (tagPath.equals("[osm, way, nd]")) {
                        Map<String, Object> properties = extractProperties(parser);
                        wayNodes.add(Long.parseLong(properties.get("ref").toString()));
                    } else if (tagPath.endsWith("tag]")) {
                        Map<String, Object> properties = extractProperties(parser);
                        currentNodeTags.put(properties.get("k").toString(), properties.get("v").toString());
                    } else if (tagPath.equals("[osm, relation]")) {
                        if (!startedRelations) {
                            batchIndexService.optimize();
                            startedRelations = true;
                        }
                        relationProperties = extractProperties("relation", parser);
                        relationMembers.clear();
                    } else if (tagPath.equals("[osm, relation, member]")) {
                        relationMembers.add(extractProperties(parser));
                    }
                    if (startedRelations) {
                        if (count < 10) {
                            log("Starting tag at depth " + depth + ": " + currentXMLTags.get(depth) + " - " + currentXMLTags.toString());
                            for (int i = 0; i < parser.getAttributeCount(); i++) {
                                log("\t" + currentXMLTags.toString() + ": " + parser.getAttributeLocalName(i) + "["
                                        + parser.getAttributeNamespace(i) + "," + parser.getAttributePrefix(i) + ","
                                        + parser.getAttributeType(i) + "," + "] = " + parser.getAttributeValue(i));
                            }
                        }
                        count++;
                    }
                    depth++;
                    break;
                case javax.xml.stream.XMLStreamConstants.END_ELEMENT:
                    if (currentXMLTags.toString().equals("[osm, node]")) {
                        addNodeTags(batchGraphDb, currentNode, currentNodeTags,"node");
                    } else if (currentXMLTags.toString().equals("[osm, way]")) {
                        RoadDirection direction = isOneway(currentNodeTags);
                        String name = (String)currentNodeTags.get("name");
                        String geometry = getGeomFromTags(currentNodeTags);
                        boolean isRoad = currentNodeTags.containsKey("highway");
                        if (isRoad) {
                            wayProperties.put("oneway", direction.toString());
                            wayProperties.put("highway", currentNodeTags.get("highway"));
                        }
                        if (name != null) {
                            // Copy name tag to way because this seems like a valuable location for
                            // such a property
                            wayProperties.put("name", name);
                        }
                        long way = addNode(batchGraphDb, "way", wayProperties, "way_osm_id");
                        if (prev_way < 0) {
                            batchGraphDb.createRelationship(osm_dataset, way, OSMRelation.WAYS, null);
                        } else {
                            batchGraphDb.createRelationship(prev_way, way, OSMRelation.NEXT, null);
                        }
                        prev_way = way;
                        addNodeTags(batchGraphDb, way, currentNodeTags, "way") ;
                        Envelope bbox = new Envelope();
                        long prevNode = -1;
                        long prevProxy = -1;
                        Map<String, Object> prevProps = null;
                        LinkedHashMap<String, Object> relProps = new LinkedHashMap<String, Object>();
                        HashMap<String, Object> directionProps = new HashMap<String, Object>();
                        directionProps.put("oneway", true);
                        for (long nd_ref : wayNodes) {
                            long pointNode = batchIndexService.getSingleNode("node_osm_id", nd_ref);
                            long proxyNode = batchGraphDb.createNode(null);
                            if (-1 == pointNode || prevNode == pointNode) {
                                /*
                                 * This can happen if we import not whole planet, so some referenced
                                 * nodes will be unavailable
                                 */
                                missingNode(nd_ref);
                                continue;
                            }
                            batchGraphDb.createRelationship(proxyNode, pointNode, OSMRelation.NODE, null);
                            Map<String, Object> nodeProps = batchGraphDb.getNodeProperties(pointNode);
                            double[] location = new double[] {(Double)nodeProps.get("lon"), (Double)nodeProps.get("lat")};
                            bbox.expandToInclude(location[0], location[1]);
                            if (prevProxy < 0) {
                                batchGraphDb.createRelationship(way, proxyNode, OSMRelation.FIRST_NODE, null);
                            } else {
                                relProps.clear();
                                double[] prevLoc = new double[] {(Double)prevProps.get("lon"), (Double)prevProps.get("lat")};

                                double length = distance(prevLoc[0], prevLoc[1], location[0], location[1]);
                                relProps.put("length", length);

                                // We default to bi-directional (and don't store direction in the
                                // way node), but if it is one-way we mark it as such, and define
                                // the direction using the relationship direction
                                if (direction == RoadDirection.BACKWARD) {
                                    batchGraphDb.createRelationship(proxyNode, prevProxy, OSMRelation.NEXT, relProps);
                                } else {
                                    batchGraphDb.createRelationship(prevProxy, proxyNode, OSMRelation.NEXT, relProps);
                                }
                            }
                            prevNode = pointNode;
                            prevProxy = proxyNode;
                            prevProps = nodeProps;
                        }
//                        if (prevNode > 0) {
//                            batchGraphDb.createRelationship(way, prevNode, OSMRelation.LAST_NODE, null);
//                        }
                        addNodeGeometry(batchGraphDb, way, geometry, bbox, wayNodes.size());
                    } else if (currentXMLTags.toString().equals("[osm, relation]")) {
                        String name = (String)currentNodeTags.get("name");
                        String geometry = getGeomFromTags(currentNodeTags);
                        if (name != null) {
                            // Copy name tag to way because this seems like a valuable location for
                            // such a property
                            relationProperties.put("name", name);
                        }
                        long relation = addNode(batchGraphDb, "relation", relationProperties, "relation_osm_id");
                        if (prev_relation < 0) {
                            batchGraphDb.createRelationship(osm_dataset, relation, OSMRelation.RELATIONS, null);
                        } else {
                            batchGraphDb.createRelationship(prev_relation, relation, OSMRelation.NEXT, null);
                        }
                        prev_relation = relation;
                        addNodeTags(batchGraphDb, relation, currentNodeTags, "relation");
                        Envelope bbox = new Envelope();
                        long prevMember = -1;
                        int vertices = 0;
                        LinkedHashMap<String, Object> relProps = new LinkedHashMap<String, Object>();
                        for (Map<String, Object> memberProps : relationMembers) {
                            String memberType = (String)memberProps.get("type");
                            long member_ref = Long.parseLong(memberProps.get("ref").toString());
                            if (memberType != null) {
                                long member = batchIndexService.getSingleNode(memberType + "_osm_id", member_ref);
                                if (-1 == member || prevMember == member) {
                                    /*
                                     * This can happen if we import not whole planet, so some
                                     * referenced nodes will be unavailable
                                     */
                                    missingMember(memberProps.toString());
                                    continue;
                                }
                                if (member == relation) {
                                    error("Cannot add relation to same member: relation[" + currentNodeTags + "] - member["
                                            + memberProps + "]");
                                    continue;
                                }
                                Map<String, Object> nodeProps = batchGraphDb.getNodeProperties(member);
                                if (memberType.equals("node")) {
                                    double[] location = new double[] {(Double)nodeProps.get("lon"), (Double)nodeProps.get("lat")};
                                    bbox.expandToInclude(location[0], location[1]);
                                    vertices++;
                                } else {
                                    for (SimpleRelationship rel : batchGraphDb.getRelationships(member)) {
                                        if (rel.getType().equals(OSMRelation.GEOM)) {
                                            nodeProps = batchGraphDb.getNodeProperties(rel.getEndNode());
                                            double[] sbb = (double[])nodeProps.get("bbox");
                                            bbox.expandToInclude(sbb[0], sbb[2]);
                                            bbox.expandToInclude(sbb[1], sbb[3]);
                                            vertices += (Integer)nodeProps.get("vertices");
                                        }
                                    }
                                }
                                relProps.clear();
                                relProps.put("role", (String)memberProps.get("role"));
                                batchGraphDb.createRelationship(relation, member, OSMRelation.MEMBER, relProps);
                                // members can belong to multiple relations, in multiple orders, so NEXT will clash (also with NEXT between ways in original way load)
//                                if (prevMember < 0) {
//                                    batchGraphDb.createRelationship(relation, member, OSMRelation.MEMBERS, null);
//                                } else {
//                                    batchGraphDb.createRelationship(prevMember, member, OSMRelation.NEXT, null);
//                                }
                                prevMember = member;
                            } else {
                                System.err.println("Cannot process invalid relation member: " + memberProps.toString());
                            }
                        }
                        addNodeGeometry(batchGraphDb, relation, geometry, bbox, vertices);
                    }
                    depth--;
                    currentXMLTags.remove(depth);
                    // log("Ending tag at depth "+depth+": "+currentTags.get(depth));
                    break;
                default:
                    break;
                }
            }
        } finally {
            parser.close();
            batchIndexService.shutdown();
        }
        describeMissing();

        long stopTime = System.currentTimeMillis();
        log("info | Elapsed time in seconds: " + (1.0 * (stopTime - startTime) / 1000.0));
    }

    private int missingNodeCount = 0;

    private void missingNode(long ndRef) {
        if (missingNodeCount++ < 10) {
            error("Cannot find node for osm-id " + ndRef);
        }
    }

    private void describeMissing() {
        if (missingNodeCount > 0) {
            error("When processing the ways, there were " + missingNodeCount + " missing nodes");
        }
        if (missingMemberCount > 0) {
            error("When processing the relations, there were " + missingMemberCount + " missing members");
        }
    }

    private int missingMemberCount = 0;

    private void missingMember(String description) {
        if (missingMemberCount++ < 10) {
            error("Cannot find member: " + description);
        }
    }

    private void addNodeTags(BatchInserter batchGraphDb, long currentNode, LinkedHashMap<String, Object> tags, String type) {
        if (currentNode > 0 && tags.size() > 0) {
        	addToTagStats(type, tags.keySet());
            long id = batchGraphDb.createNode(tags);
            batchGraphDb.createRelationship(currentNode, id, OSMRelation.TAGS, null);
            tags.clear();
        }
    }

    private void addNodeGeometry(BatchInserter batchGraphDb, long currentNode, String type, Envelope bbox, int vertices) {
        if (currentNode > 0 && !bbox.isNull() && vertices > 0) {
            LinkedHashMap<String, Object> properties = new LinkedHashMap<String, Object>();
            if (type == null)
                type = vertices > 1 ? "MultiPoint" : "Point";
            properties.put("geometry", type);
            properties.put("vertices", vertices);
            properties.put("bbox", new double[] {bbox.getMinX(), bbox.getMaxX(), bbox.getMinY(), bbox.getMaxY()});
            long id = batchGraphDb.createNode(properties);
            batchGraphDb.createRelationship(currentNode, id, OSMRelation.GEOM, null);
            properties.clear();
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
            if (relationship.getType().name().equals(relType.name())) {
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

    private long getOrCreateNode(BatchInserter batchInserter, String name, String type, long parent, RelationshipType relType) {
        long node = findNode(batchInserter, name, parent, relType);
        if (node < 0) {
            HashMap<String, Object> properties = new HashMap<String, Object>();
            properties.put("name", name);
            properties.put("type", type);
            node = batchInserter.createNode(properties);
            batchInserter.createRelationship(parent, node, relType, null);
        }
        return node;
    }

    public long getOrCreateOSMDataset(BatchInserter batchInserter, String name) {
        if (osm_dataset <= 0) {
            osm_root = getOrCreateNode(batchInserter, "osm_root", "osm", batchInserter.getReferenceNode(), OSMRelation.OSM);
            osm_dataset = getOrCreateNode(batchInserter, name, "osm", osm_root, OSMRelation.OSM);
        }
        return osm_dataset;
    }

    /**
     * Detects if road has the only direction
     * 
     * @param wayProperties
     * @return RoadDirection
     */
    public static RoadDirection isOneway(Map<String, Object> wayProperties) {
        String oneway = (String)wayProperties.get("oneway");
        if (null != oneway) {
            if ("-1".equals(oneway))
                return RoadDirection.BACKWARD;
            if ("1".equals(oneway) || "yes".equalsIgnoreCase(oneway) || "true".equalsIgnoreCase(oneway))
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
    public static double distance(double lonA, double latA, double lonB, double latB) {
        return WGS84.orthodromicDistance(lonA, latA, lonB, latB);
    }

    private void log(PrintStream out, String message, Exception e) {
        if (logContext != null) {
            message = logContext + "[" + contextLine + "]: " + message;
        }
        out.println(message);
        if (e != null) {
            e.printStackTrace(out);
        }
    }

    private void log(String message) {
        log(System.out, message, null);
    }

    private void error(String message) {
        log(System.err, message, null);
    }

    private void error(String message, Exception e) {
        log(System.err, message, e);
    }

    private String logContext = null;
    private int contextLine = 0;

    private void setLogContext(String context) {
        logContext = context;
        contextLine = 0;
    }

    private void incrLogContext() {
        contextLine++;
    }

}
