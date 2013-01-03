/**
 * Copyright (c) 2010-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
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

import org.neo4j.collections.rtree.EnvelopeDecoder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;

import com.vividsolutions.jts.geom.Geometry;


/**
 * Implementations of this interface define custom approaches to storing geometries in the database
 * graph. There are two primary approaches:
 * <ul>
 * <dt>In-node</dt>
 * <dd>This approach makes use of properties of an individual node to store the geometry. The
 * built-in WKT and WKB encoders use this approach, but a custom encoder that simply stored a
 * float[] of coordinates for a LineString would also be classed here.</dd>
 * <dt>Sub-graph</dt>
 * <dd>This approach makes use of a graph of nodes and relationships to describe a single geometry.
 * This could be as simple as a chain of nodes representing a LineString or a complex nested graph
 * like the OSM approach to MultiPolygons.</dd>
 * </ul>
 * Classes that implement this interface must have a public constructor taking no parameters, and
 * should be able to interface to the Layer class using the init(Layer) method. When a new Layer is
 * created, it is a single node that has a property containing the class name of the
 * GeometryEncoder, and if the Layer needs to be read later, that node will be read, and a Layer
 * created from it, and the Layer will create an instance of the required GeometryEncoder, which in
 * turn should be capable of reading and writing all Geometries supported by that Layer.
 * 
 * @author Davide Savazzi
 */
public interface GeometryEncoder extends EnvelopeDecoder {

    /**
     * When accessing an existing layer, the Layer is constructed from a single node in the graph
     * that represents a layer. This node is expected to have a property containing the class name
     * of the GeometryEncoder for that layer, and it will be constructed and passed the layer using
     * this method, allowing the Layer and the GeometryEncoder to interact.
     * 
     * @param layer recently created Layer class
     */
    void init(Layer layer);

    /**
     * This method is called to store a geometry object to the database. It should write it to the
     * container supplied. If the container is a node, it can be the root of an entire sub-graph.
     * 
     * @param geometry
     * @param container
     */
    void encodeGeometry(Geometry geometry, PropertyContainer container);

    /**
     * This method is called on an individual container when we need to extract the geometry. If the
     * container is a node, this could be the root of a sub-graph containing the geometry.
     * 
     * @param container
     * @return
     */
    Geometry decodeGeometry(PropertyContainer container);

	/**
	 * Each geometry might have a set of associated attributes, or properties.
	 * These are seen as a map of String to Object types, where the Objects
	 * should be primitives or Strings. This can be encoded as properties of the
	 * geometry node itself (default behaviour), or stored in the graph in some
	 * other way.
	 * 
	 * @param geomNode
	 * @param attribute to test
	 * @return
	 */
	boolean hasAttribute(Node geomNode, String name);

	/**
	 * Each geometry might have a set of associated attributes, or properties.
	 * These are seen as a map of String to Object types, where the Objects
	 * should be primitives or Strings. This can be encoded as properties of the
	 * geometry node itself (default behaviour), or stored in the graph in some
	 * other way.
	 * 
	 * @param geomNode
	 * @param attribute to test
	 * @return
	 */
	Object getAttribute(Node geomNode, String name);

}