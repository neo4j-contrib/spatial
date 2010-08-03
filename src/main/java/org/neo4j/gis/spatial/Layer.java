/*
 * Copyright (c) 2010
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial;

import org.neo4j.graphdb.Node;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Instances of Layer provide the ability for developers to add/remove and edit geometries
 * associated with a single dataset (or layer). This includes support for several storage
 * mechanisms, like in-node (geometries in properties) and sub-graph (geometries describe by the
 * graph). A Layer can be associated with a dataset. In cases where the dataset contains only one
 * layer, the layer itself is the dataset. See the class DefaultLayer for the standard
 * implementation of that pattern.
 * 
 * @author Davide Savazzi
 * @author Craig Taverner
 */
public interface Layer extends Constants {

    /**
     * The layer is constructed from metadata in the layer node, which requires that the layer have
     * a no-argument constructor. The real initialization of the layer is then performed by calling
     * this method. The layer implementation can store the passed parameters for later use
     * satisfying the prupose of the layer API (see other Layer methods).
     * 
     * @param spatialDatabase
     * @param name
     * @param layerNode
     */
    void initialize(SpatialDatabaseService spatialDatabase, String name, Node layerNode);

    /**
     * Add a new geometry to the layer. This will add the geometry to the index.
     * 
     * @param geometry
     * @return
     */
    SpatialDatabaseRecord add(Geometry geometry);

    /**
     * Delete the geometry identified by the passed node id. This might be as simple as deleting the
     * geometry node, or it might require extracting and deleting an entire sub-graph.
     * 
     * @param geoemtryNodeId
     */
    void delete(long geometryNodeId);

    /**
     * Update the geometry identified by the passed node id. This might be as simple as changing
     * node properties or it might require editing an entire sub-graph.
     * 
     * @param geoemtryNodeId
     */
    void update(long geometryNodeId, Geometry geometry);

    /**
     * Every layer using a specific implementation of the SpatialIndexReader and SpatialIndexWriter
     * for indexing the data in that layer.
     * 
     * @return the SpatialIndexReader used to perform searches on the data in the layer
     */
    SpatialIndexReader getIndex();

    GeometryFactory getGeometryFactory();

    /**
     * All layers are associated with a single node in the database. This node will have properties,
     * relationships (sub-graph) or both to describe the contents of the layer
     */
    Node getLayerNode();

    /**
     * Delete the entire layer, including the index. The specific layer implementation will decide
     * if this method should delete also the geometry nodes indexed by this layer. Some
     * implementations have data that only has meaning within a layer, and so will be deleted.
     * Others are simply views onto other more complex data models and deleting the geometry nodes
     * might imply damage to the model. Keep this in mind when coding implementations of the Layer.
     */
    void delete(Listener monitor);

    /**
     * Every layer is defined by a unique name. Uniqueness is not enforced, but lack of uniqueness
     * will not guarrantee the right layer returned from a search.
     * 
     * @return
     */
    String getName();

    /**
     * Each layer can contain geometries stored in the database in a custom way. Classes that
     * implement the layer should also provide appropriate GeometryEncoders for encoding and
     * decoding the geometries. This can be either as properties of a geometry node, or as
     * sub-graphs accessible from some geometry node.
     * 
     * @return implementation of the GemoetryEncoder class enabling encoding/decoding of geometries
     *         from the graph
     */
    GeometryEncoder getGeometryEncoder();

    /**
     * Each layer can represent data stored in a specific coordinate refernece system, or
     * projection.
     * 
     * @return
     */
    CoordinateReferenceSystem getCoordinateReferenceSystem();

    /**
     * Each layer contains geometries with optional attributes.
     * 
     * @return String array of all attribute names
     */
    String[] getExtraPropertyNames();

    /**
     * The layer conforms with the Geotools pattern of only allowing a single geometry per layer.
     * 
     * @return integer key for the geotools geometry type
     */
    Integer getGeometryType();

    /**
     * Since the layer is a key object passed around to most code, it is important to be able to
     * access the generic spatial API from this object.
     * 
     * @return instance of the SpatialDatabaseService object for general access to the spatial
     *         database features
     */
    SpatialDatabaseService getSpatialDatabase();

    /**
     * Each layer is associated with a SpatialDataset. This can be a one-for-one match to the layer,
     * or can be expressed as many layers on a single dataset.
     * 
     * @return SpatialDataset containing the data indexed by this layer.
     */
    SpatialDataset getDataset();

}