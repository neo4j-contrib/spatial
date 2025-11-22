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

import static org.neo4j.gis.spatial.Constants.LABEL_LAYER;
import static org.neo4j.gis.spatial.Constants.PROP_GEOMENCODER;
import static org.neo4j.gis.spatial.Constants.PROP_GEOMENCODER_CONFIG;
import static org.neo4j.gis.spatial.Constants.PROP_INDEX_CLASS;
import static org.neo4j.gis.spatial.Constants.PROP_INDEX_CONFIG;
import static org.neo4j.gis.spatial.Constants.PROP_INDEX_TYPE;
import static org.neo4j.gis.spatial.Constants.PROP_LAYER;
import static org.neo4j.gis.spatial.Constants.PROP_LAYER_CLASS;
import static org.neo4j.gis.spatial.Constants.PROP_LAYER_TYPE;

import javax.annotation.Nonnull;
import org.neo4j.gis.spatial.EditableLayerImpl;
import org.neo4j.gis.spatial.encoders.Configurable;
import org.neo4j.gis.spatial.encoders.WKBGeometryEncoder;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.spatial.api.encoder.GeometryEncoder;
import org.neo4j.spatial.api.index.IndexManager;
import org.neo4j.spatial.api.index.SpatialIndexWriter;
import org.neo4j.spatial.api.layer.Layer;

/**
 * Utilities for creating layers from nodes.
 */
public class LayerUtilities {

	/**
	 * Factory method to construct a layer from an existing layerNode. This will read the layer
	 * class from the layer node properties and construct the correct class from that.
	 *
	 * @return new layer instance from the existing layer node
	 */
	public static Layer makeLayerFromNode(
			@Nonnull Transaction tx,
			@Nonnull IndexManager indexManager,
			@Nonnull Node layerNode,
			boolean readOnly
	) {
		String name = (String) layerNode.getProperty(PROP_LAYER);
		if (name == null) {
			throw new IllegalArgumentException(
					"Node is not a layer node, it has no " + PROP_LAYER + " property: " + layerNode);
		}

		// init the geometry encoder
		GeometryEncoder encoder;
		if (layerNode.hasProperty(PROP_GEOMENCODER)) {
			String encoderName = (String) layerNode.getProperty(PROP_GEOMENCODER);
			encoder = GeometryEncoderRegistry.INSTANCE.constructGeometryEncoder(encoderName,
					(String) layerNode.getProperty(PROP_GEOMENCODER_CONFIG, null));
		} else {
			encoder = new WKBGeometryEncoder();
		}

		// init the layer
		Layer layer;
		if (layerNode.hasProperty(PROP_LAYER_TYPE)) {
			String layerName = (String) layerNode.getProperty(PROP_LAYER_TYPE);
			layer = LayerRegistry.INSTANCE.constructLayer(layerName);
		} else if (layerNode.hasProperty(PROP_LAYER_CLASS)) {
			String layerName = (String) layerNode.getProperty(PROP_LAYER_CLASS);
			layer = LayerRegistry.INSTANCE.constructLayer(layerName);
			if (!readOnly) {
				layerNode.setProperty(PROP_LAYER_TYPE, layer.getIdentifier());
			}
		} else {
			layer = new EditableLayerImpl();
		}

		// init the index
		SpatialIndexWriter indexWriter;
		String indexConfig = (String) layerNode.getProperty(PROP_INDEX_CONFIG, null);
		if (layerNode.hasProperty(PROP_INDEX_TYPE)) {
			String indexName = (String) layerNode.getProperty(PROP_INDEX_TYPE);
			indexWriter = IndexRegistry.INSTANCE.constructIndex(indexName, indexConfig);
		} else  if (layerNode.hasProperty(PROP_INDEX_CLASS)) {
			String indexName = (String) layerNode.getProperty(PROP_INDEX_CLASS);
			indexWriter = IndexRegistry.INSTANCE.constructIndex(indexName, indexConfig);
			if (!readOnly) {
				layerNode.setProperty(PROP_INDEX_TYPE, indexWriter.getIdentifier());
			}
		} else {
			indexWriter = new LayerRTreeIndex();
		}

		layer.initialize(tx, indexManager, name, encoder, indexWriter, layerNode, readOnly);

		return layer;
	}

	/**
	 * Factory method to construct a layer with the specified layer class. This can be used when
	 * creating a layer for the first time. It will also construct the underlying Node in the graph.
	 *
	 * @return new Layer instance based on newly created layer Node
	 */
	public static <L extends Layer> L makeLayerAndNode(
			@Nonnull Transaction tx,
			@Nonnull IndexManager indexManager,
			@Nonnull String name,
			@Nonnull Class<? extends GeometryEncoder> geometryEncoderClass,
			String encoderConfig,
			@Nonnull Class<L> layerClass,
			@Nonnull Class<? extends SpatialIndexWriter> indexClass,
			String indexConfig
	) {
		Node layerNode = tx.createNode();
		layerNode.addLabel(LABEL_LAYER);
		layerNode.setProperty(PROP_LAYER, name);

		// init the geometry encoder
		GeometryEncoder encoder = GeometryEncoderRegistry.INSTANCE.constructGeometryEncoder(
				geometryEncoderClass,
				encoderConfig);
		layerNode.setProperty(PROP_GEOMENCODER, encoder.getIdentifier());
		if (encoderConfig != null && !encoderConfig.isEmpty() && encoder instanceof Configurable) {
			layerNode.setProperty(PROP_GEOMENCODER_CONFIG, encoderConfig);
		}

		// init the layer
		L layer = LayerRegistry.INSTANCE.constructLayer(layerClass);
		layerNode.setProperty(PROP_LAYER_TYPE, layer.getIdentifier());

		// init the index
		SpatialIndexWriter index = IndexRegistry.INSTANCE.constructIndex(indexClass, indexConfig);
		layerNode.setProperty(PROP_INDEX_TYPE, index.getIdentifier());
		if (indexConfig != null && !indexConfig.isEmpty() && index instanceof Configurable) {
			layerNode.setProperty(PROP_INDEX_CONFIG, indexConfig);
		}

		layer.initialize(tx, indexManager, name, encoder, index, layerNode, false);

		return layer;

	}
}
