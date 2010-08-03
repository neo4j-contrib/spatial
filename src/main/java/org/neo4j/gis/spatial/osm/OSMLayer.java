package org.neo4j.gis.spatial.osm;

import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.NullListener;
import org.neo4j.gis.spatial.SpatialDataset;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Instances of this class represent individual layers or GeoResources of the OSM Dataset.
 * 
 * @author craig
 * @since 1.0.0
 */
public class OSMLayer extends Layer {
    private OSMDataset osmDataset;

    public SpatialDataset getDataset() {
        if(osmDataset==null){
            osmDataset = new OSMDataset(this);
        }
        return osmDataset;
    }

    protected void clear() {
        this.index.removeAll(false, new NullListener());
    }

    public void addWay(Node way) {
        Relationship geomRel = way.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING);
        if (geomRel != null) {
            Node geomNode = geomRel.getEndNode();
            add(geomNode);
        }
    }
}
