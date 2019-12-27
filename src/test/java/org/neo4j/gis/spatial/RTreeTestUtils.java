package org.neo4j.gis.spatial;

import org.apache.commons.collections.map.HashedMap;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Philip Stephens on 12/11/2016.
 */
public class RTreeTestUtils {
    /**
     * This class contain functions which can be used to test the integrity of the Rtree.
     *
     *
     */

    private RTreeIndex rtree;

    public RTreeTestUtils(RTreeIndex rtree) {
        this.rtree = rtree;
    }

    public static double one_d_overlap(double a1, double a2, double b1, double b2) {
        return Double.max(
                Double.min(a2, b2) - Double.max(a1,b1),
                0.0
        );
    }

    public static double compute_overlap(Envelope a, Envelope b){
        return one_d_overlap(a.getMinX(), a.getMaxX(), b.getMinX(), b.getMaxX()) *
                one_d_overlap(a.getMinY(), a.getMaxY(), b.getMinY(), b.getMaxY());
    }

    public double calculate_overlap(Node child) {

        Envelope parent = rtree.getIndexNodeEnvelope(child);
        List<Envelope> children = new ArrayList<Envelope>();

        for (Relationship r : child.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
            children.add(rtree.getIndexNodeEnvelope(r.getEndNode()));
        }
        children.sort(Comparator.comparing(Envelope::getMinX, Double::compare));
        double total_overlap = 0.0;
        List<Envelope> activeNodes = new LinkedList<>();


        for (Envelope x : children) {
            activeNodes = activeNodes
                    .stream()
                    .filter(envelope -> envelope.getMaxX() < x.getMinX())
                    .collect(Collectors.toList());
            total_overlap += activeNodes.stream().mapToDouble(envelope -> compute_overlap(x, envelope)).sum();
            activeNodes.add(x);
        }

        return total_overlap/parent.getArea();

    }

    public Map<Long, Long> get_height_map(GraphDatabaseService db, Node root){
        String id = Long.toString(root.getId());


        String cypher = "MATCH p = (root) -[:RTREE_CHILD*0..] ->(child) -[:RTREE_REFERENCE]->(leaf)\n" +
                "    WHERE id(root) = "+id+"\n" +
                "    RETURN length(p) as depth, count (*) as freq";
        Result result = db.execute(cypher);

        int i = 0;
        Map<Long, Long> map = new HashedMap();
        while (result.hasNext()) {
            Map<String, Object> r = result.next();
            map.put((Long) r.get("depth"), (Long) r.get("freq"));
            i++;
        }
        return map;
    }

    public boolean check_balance(GraphDatabaseService db, Node root) {
        String id = Long.toString(root.getId());


        String cypher = "MATCH p = (root) -[:RTREE_CHILD*0..] ->(child) -[:RTREE_REFERENCE]->(leaf)\n" +
                "    WHERE id(root) = "+id+"\n" +
                "    RETURN length(p) as depth, count (*) as freq";
        Result result = db.execute(cypher);

        int i = 0;
        while (result.hasNext()) {
            Map<String, Object> r = result.next();
            System.out.println(r.get("depth").toString() +" : " + r.get("freq"));
            i++;
        }
        return i == 1;

    }


}
