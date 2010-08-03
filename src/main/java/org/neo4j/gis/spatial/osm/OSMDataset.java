package org.neo4j.gis.spatial.osm;

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

    public OSMDataset(SpatialDatabaseService spatialDatabaseService, OSMLayer osmLayer, Node layerNode, long datasetId) {
        this.spatialDatabase = spatialDatabaseService;
        this.layer = osmLayer;
        this.datasetNode = spatialDatabaseService.getDatabase().getNodeById(datasetId);
        Relationship rel = layerNode.getSingleRelationship(SpatialRelationshipTypes.LAYERS, Direction.INCOMING);
        if (rel == null) {
            Transaction tx = spatialDatabaseService.getDatabase().beginTx();
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

    public Iterable< ? extends Geometry> getAllGeometries() {
        return null;
    }

    public Iterable<Node> getAllGeometryNodes() {
        return null;
    }

    public GeometryEncoder getGeometryEncoder() {
        return null;
    }

    public Iterable< ? extends Layer> getLayers() {
        return null;
    }

}
