package org.neo4j.gis.spatial;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * <p>
 * The DynamicLayer class extends a Layer to be able to express itself as several Layers. Each of
 * these 'sub-layers' is defined by adding filters to the original layer. The filters are configured
 * in the LayerConfig class, on a set of nodes related to the original dynamic layer node by
 * LAYER_CONFIG relationships. One key example of where this type of capability is very valuable is
 * for example when a layer contains geometries of multiple types, but geotools can only express one
 * type in each layer. Then we can use DynamicLayer to expose each of the different geopetry types
 * as a different layer to the consuming application (desktop or web application).
 * </p>
 * 
 * @author craig
 * @since 1.0.0
 */
public class DynamicLayer extends DefaultLayer {

    /**
     * <p>
     * The LayerConfig class exposes the rules encoded in the layer config node for a custom layer
     * expressed from a single layer node. This configuration can be a set of properties to match in
     * order to be considered part of the layer, or it can be a more complex custom traverser.
     * </p>
     * 
     * @author craig
     * @since 1.0.0
     */
    public class LayerConfig {
        private Node configNode;

        public LayerConfig(Node configNode) {
            this.configNode = configNode;
        }

        public String getName() {
            return (String)configNode.getProperty("layer_name");
        }
        // TODO: Code rules for specifying layer configurations (attributes/properties to match, or
        // custom traverser rules)
    }

    public List<String> getLayerNames() {
        ArrayList<String> names = new ArrayList<String>();
        names.add(this.getName());
        for (Relationship rel : layerNode.getRelationships(SpatialRelationshipTypes.LAYER_CONFIG, Direction.OUTGOING)) {
            LayerConfig config = new LayerConfig(rel.getEndNode());
            names.add(config.getName());
        }
        return names;
    }

}
