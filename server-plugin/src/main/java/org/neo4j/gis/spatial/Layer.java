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
package org.neo4j.gis.spatial;

import java.util.Map;
import javax.annotation.Nonnull;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.locationtech.jts.geom.GeometryFactory;
import org.neo4j.gis.spatial.attributes.PropertyMappingManager;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.index.LayerIndexReader;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;


/**
 * Instances of Layer provide the ability for developers to add/remove and edit geometries
 * associated with a single dataset (or layer). This includes support for several storage
 * mechanisms, like in-node (geometries in properties) and sub-graph (geometries describe by the
 * graph). A Layer can be associated with a dataset. In cases where the dataset contains only one
 * layer, the layer itself is the dataset. See the class DefaultLayer for the standard
 * implementation of that pattern.
 */
public interface Layer {

	/**
	 * The layer is constructed from metadata in the layer node, which requires that the layer have
	 * a no-argument constructor. The real initialization of the layer is then performed by calling
	 * this method. The layer implementation can store the passed parameters for later use
	 * satisfying the prupose of the layer API (see other Layer methods).
	 */
	void initialize(Transaction tx, IndexManager indexManager, String name, Node layerNode, boolean readOnly);

	/**
	 * Every layer using a specific implementation of the SpatialIndexReader and SpatialIndexWriter
	 * for indexing the data in that layer.
	 *
	 * @return the SpatialIndexReader used to perform searches on the data in the layer
	 */
	LayerIndexReader getIndex();

	GeometryFactory getGeometryFactory();

	/**
	 * All layers are associated with a single node in the database. This node will have properties,
	 * relationships (sub-graph) or both to describe the contents of the layer
	 */
	Node getLayerNode(Transaction tx);

	/**
	 * Every layer is defined by a unique name. Uniqueness is not enforced, but lack of uniqueness
	 * will not guarrantee the right layer returned from a search.
	 */
	String getName();

	/**
	 * Each layer can contain geometries stored in the database in a custom way. Classes that
	 * implement the layer should also provide appropriate GeometryEncoders for encoding and
	 * decoding the geometries. This can be either as properties of a geometry node, or as
	 * sub-graphs accessible from some geometry node.
	 *
	 * @return implementation of the GemoetryEncoder class enabling encoding/decoding of geometries
	 * from the graph
	 */
	GeometryEncoder getGeometryEncoder();

	/**
	 * Each layer can represent data stored in a specific coordinate refernece system, or
	 * projection.
	 */
	CoordinateReferenceSystem getCoordinateReferenceSystem(Transaction tx);

	/**
	 * Each layer contains geometries with optional attributes.
	 *
	 * @param tx the transaction
	 * @return String array of all attribute names
	 */
	@Nonnull
	Map<String, Class<?>> getExtraProperties(Transaction tx);

	/**
	 * The layer conforms with the Geotools pattern of only allowing a single geometry per layer.
	 *
	 * @return integer key for the geotools geometry type
	 */
	Integer getGeometryType(Transaction tx);

	/**
	 * Each layer is associated with a SpatialDataset. This can be a one-for-one match to the layer,
	 * or can be expressed as many layers on a single dataset.
	 *
	 * @return SpatialDataset containing the data indexed by this layer.
	 */
	SpatialDataset getDataset();

	/**
	 * Each layer can optionally provide a style to be used in rendering this
	 * layer. Return null if you wish to leave this choice to the GIS. If a
	 * Style is returned, it is used. If a File is returned, it is opened and
	 * assumed to contain SLD contents. If a String is returned, it is assumed
	 * to contain SLD contents.
	 *
	 * @return Style, String, File or null
	 */
	Object getStyle();

	PropertyMappingManager getPropertyMappingManager();

	/**
	 * For external expression of the configuration of this layer
	 *
	 * @return descriptive signature of layer, name, type and encoder
	 */
	String getSignature();

	boolean isReadOnly();

	default void checkWritable() {
		if (isReadOnly()) {
			throw new IllegalStateException("Layer is read only");
		}
	}
}
