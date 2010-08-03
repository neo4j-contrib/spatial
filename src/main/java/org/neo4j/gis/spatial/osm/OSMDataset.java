package org.neo4j.gis.spatial.osm;

import java.util.Arrays;

import org.neo4j.gis.spatial.GeometryEncoder;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.SpatialDataset;
import org.neo4j.gis.spatial.SpatialRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import com.vividsolutions.jts.geom.Geometry;

public class OSMDataset implements SpatialDataset {
    private OSMLayer layer;
    private SpatialDatabaseService spatialDatabase;
    private Node datasetNode;

    /**
     * This method is used to construct the dataset on an existing node when the node id is known,
     * which is the case with OSM importers.
     * 
     * @param spatialDatabase
     * @param osmLayer
     * @param layerNode
     * @param datasetId
     */
    public OSMDataset(SpatialDatabaseService spatialDatabase, OSMLayer osmLayer, Node layerNode, long datasetId) {
        this.spatialDatabase = spatialDatabase;
        this.layer = osmLayer;
        this.datasetNode = spatialDatabase.getDatabase().getNodeById(datasetId);
        Relationship rel = layerNode.getSingleRelationship(SpatialRelationshipTypes.LAYERS, Direction.INCOMING);
        if (rel == null) {
            Transaction tx = spatialDatabase.getDatabase().beginTx();
            try {
                datasetNode.createRelationshipTo(layerNode, SpatialRelationshipTypes.LAYERS);
                tx.success();
            } finally {
                tx.finish();
            }
        } else {
            Node node = rel.getStartNode();
            if (!node.equals(datasetNode)) {
                throw new RuntimeException("Layer '" + osmLayer + "' already belongs to another dataset: " + node);
            }
        }
    }

    /**
     * This method is used to construct the dataset when only the layer node is known, and the
     * dataset node needs to be searched for.
     * 
     * @param spatialDatabase2
     * @param osmLayer
     * @param layerNode
     */
    public OSMDataset(SpatialDatabaseService spatialDatabase, OSMLayer osmLayer, Node layerNode) {
        this.spatialDatabase = spatialDatabase;
        this.layer = osmLayer;
        Relationship rel = layerNode.getSingleRelationship(SpatialRelationshipTypes.LAYERS, Direction.INCOMING);
        if (rel == null) {
            throw new RuntimeException("Layer '" + osmLayer + "' does not have an associated dataset");
        } else {
            datasetNode = rel.getStartNode();
        }
    }

    public Iterable< ? extends Geometry> getAllGeometries() {
        //@TODO: support multiple layers
        return layer.getAllGeometries();
    }

    public Iterable<Node> getAllGeometryNodes() {
        //@TODO: support multiple layers
        return layer.getAllGeometryNodes();
    }

    public GeometryEncoder getGeometryEncoder() {
        //@TODO: support multiple layers
        return layer.getGeometryEncoder();
    }

    public Iterable< ? extends Layer> getLayers() {
        return Arrays.asList(new Layer[]{layer});
    }

}
