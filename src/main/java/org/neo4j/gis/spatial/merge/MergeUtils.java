/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Spatial.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.merge;

import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.GeometryEncoder;
import org.neo4j.gis.spatial.encoders.Configurable;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;

public class MergeUtils {

    public interface Mergeable {
        long mergeFrom(Transaction tx, EditableLayer other);
    }

    private static boolean encodersIdentical(EditableLayer layer, EditableLayer mergeLayer) {
        GeometryEncoder layerEncoder = layer.getGeometryEncoder();
        GeometryEncoder mergeEncoder = mergeLayer.getGeometryEncoder();
        Class<? extends GeometryEncoder> layerGeometryClass = layerEncoder.getClass();
        Class<? extends GeometryEncoder> mergeGeometryClass = mergeEncoder.getClass();
        if (layerGeometryClass.isAssignableFrom(mergeGeometryClass)) {
            if (mergeEncoder instanceof Configurable && layerEncoder instanceof Configurable) {
                String mergeConfig = ((Configurable) mergeEncoder).getConfiguration();
                String layerConfig = ((Configurable) layerEncoder).getConfiguration();
                return mergeConfig.equals(layerConfig);
            } else {
                // If one is configurable, but not the other, they are not identical
                return !(mergeEncoder instanceof Configurable || layerEncoder instanceof Configurable);
            }
        }
        return false;
    }

    public static long mergeLayerInto(Transaction tx, EditableLayer layer, EditableLayer mergeLayer) {
        long count;
        Class<? extends EditableLayer> layerClass = layer.getClass();
        Class<? extends EditableLayer> mergeClass = mergeLayer.getClass();
        if (layer instanceof Mergeable) {
            count = ((Mergeable) layer).mergeFrom(tx, mergeLayer);
        } else if (layerClass.isAssignableFrom(mergeClass)) {
            if (encodersIdentical(layer, mergeLayer)) {
                // With identical encoders, we can simply add the node as is, but must remove it first
                ArrayList<Node> toAdd = new ArrayList<>();
                for (Node node : mergeLayer.getIndex().getAllIndexedNodes(tx)) {
                    toAdd.add(node);
                }
                for (Node node : toAdd) {
                    // Remove each from the previous index before adding to the new index, so as not to have multiple incoming RTREE_REFERENCE
                    mergeLayer.removeFromIndex(tx, node.getId());
                    layer.add(tx, node);
                }
                count = toAdd.size();
            } else {
                // With differing encoders, we must decode and re-encode each geometry, so we also create new nodes and remove and delete the actual nodes later
                ArrayList<Node> toRemove = new ArrayList<>();
                GeometryEncoder fromEncoder = mergeLayer.getGeometryEncoder();
                for (Node node : mergeLayer.getIndex().getAllIndexedNodes(tx)) {
                    toRemove.add(node);
                    Geometry geometry = fromEncoder.decodeGeometry(node);
                    layer.add(tx, geometry);
                }
                for (Node remove : toRemove) {
                    mergeLayer.removeFromIndex(tx, remove.getId());
                    remove.delete();
                }
                count = toRemove.size();
            }
        } else {
            throw new IllegalArgumentException(String.format("Cannot merge '%s' into '%s': layer classes are not compatible: '%s' cannot be caste as '%s'", mergeLayer.getName(), layer.getName(), mergeClass.getSimpleName(), layerClass.getSimpleName()));
        }
        return count;
    }
}
