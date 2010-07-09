package org.neo4j.gis.spatial.osm;

import org.neo4j.gis.spatial.Layer;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class OSMLayer extends Layer {

    public void addWay(Node way) {
        Relationship geomRel = way.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING);
        if(geomRel !=null){
            Node geomNode = geomRel.getEndNode();
            add(geomNode);
        }
    }
}
