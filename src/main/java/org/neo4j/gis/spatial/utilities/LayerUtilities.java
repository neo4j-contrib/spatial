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
package org.neo4j.gis.spatial.utilities;

import java.lang.reflect.InvocationTargetException;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.GeometryEncoder;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.index.LayerIndexReader;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * Utilities for creating layers from nodes.
 */
public class LayerUtilities implements Constants {

	/**
	 * Factory method to construct a layer from an existing layerNode. This will read the layer
	 * class from the layer node properties and construct the correct class from that.
	 *
	 * @return new layer instance from existing layer node
	 */
	@SuppressWarnings("unchecked")
	public static Layer makeLayerFromNode(Transaction tx, IndexManager indexManager, Node layerNode) {
		try {
			String name = (String) layerNode.getProperty(PROP_LAYER);
			if (name == null) {
				throw new IllegalArgumentException(
						"Node is not a layer node, it has no " + PROP_LAYER + " property: " + layerNode);
			}

			String className = null;
			if (layerNode.hasProperty(PROP_LAYER_CLASS)) {
				className = (String) layerNode.getProperty(PROP_LAYER_CLASS);
			}

			Class<? extends Layer> layerClass =
					className == null ? Layer.class : (Class<? extends Layer>) Class.forName(className);
			return makeLayerInstance(tx, indexManager, name, layerNode, layerClass);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Factory method to construct a layer with the specified layer class. This can be used when
	 * creating a layer for the first time. It will also construct the underlying Node in the graph.
	 *
	 * @return new Layer instance based on newly created layer Node
	 */
	public static Layer makeLayerAndNode(Transaction tx, IndexManager indexManager, String name,
			Class<? extends GeometryEncoder> geometryEncoderClass,
			Class<? extends Layer> layerClass,
			Class<? extends LayerIndexReader> indexClass) {
		try {
			if (indexClass == null) {
				indexClass = LayerRTreeIndex.class;
			}
			Node layerNode = tx.createNode();
			layerNode.addLabel(LABEL_LAYER);
			layerNode.setProperty(PROP_LAYER, name);
			layerNode.setProperty(PROP_GEOMENCODER, geometryEncoderClass.getCanonicalName());
			layerNode.setProperty(PROP_INDEX_CLASS, indexClass.getCanonicalName());
			layerNode.setProperty(PROP_LAYER_CLASS, layerClass.getCanonicalName());
			return makeLayerInstance(tx, indexManager, name, layerNode, layerClass);
		} catch (Exception e) {
			throw new SpatialDatabaseException(e);
		}
	}

	private static Layer makeLayerInstance(Transaction tx, IndexManager indexManager, String name, Node layerNode,
			Class<? extends Layer> layerClass)
			throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
		if (layerClass == null) {
			layerClass = Layer.class;
		}
		Layer layer = layerClass.getDeclaredConstructor().newInstance();
		layer.initialize(tx, indexManager, name, layerNode);
		return layer;
	}

}
