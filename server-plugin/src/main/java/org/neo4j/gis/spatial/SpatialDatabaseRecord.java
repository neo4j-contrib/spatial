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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.attributes.PropertyMapper;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class SpatialDatabaseRecord implements Constants, SpatialRecord {

	private Node geomNode;
	private Geometry geometry;
	private final Layer layer;

	public SpatialDatabaseRecord(Layer layer, Node geomNode) {
		this(layer, geomNode, null);
	}

	@Override
	public String getId() {
		return geomNode.getElementId();
	}

	public String getNodeId() {
		return getId();
	}

	@Override
	public Node getGeomNode() {
		return geomNode;
	}

	/**
	 * If the geomNode is to be used in a different transaction than the one in which it was created, we must call this
	 * first
	 */
	public void refreshGeomNode(Transaction tx) {
		geomNode = tx.getNodeByElementId(geomNode.getElementId());
	}

	/**
	 * This method returns a simple integer representation of the geometry. Some
	 * geometry encoders store this directly as a property of the geometry node,
	 * while others might store this information elsewhere in the graph, or
	 * deduce it from other factors of the data model. See the GeometryEncoder
	 * for information about mapping from the data model to the geometry.
	 *
	 * @return integer representation of a geometry
	 * @deprecated This method is of questionable value, since it is better to
	 * query the geometry object directly, outside the result
	 */
	@Deprecated
	public int getType() {
		//TODO: Get the type from the geometryEncoder
		return SpatialDatabaseService.convertJtsClassToGeometryType(getGeometry().getClass());
	}

	@Override
	public Geometry getGeometry() {
		if (geometry == null) {
			geometry = layer.getGeometryEncoder().decodeGeometry(geomNode);
		}
		return geometry;
	}

	public CoordinateReferenceSystem getCoordinateReferenceSystem(Transaction tx) {
		return layer.getCoordinateReferenceSystem(tx);
	}

	public String getLayerName() {
		return layer.getName();
	}

	/**
	 * Not all geometry records have the same attribute set, so we should test
	 * for each specific record if it contains that property.
	 */
	@Override
	public boolean hasProperty(Transaction tx, String name) {
		PropertyMapper mapper = layer.getPropertyMappingManager().getPropertyMapper(tx, name);
		return mapper == null ? hasGeometryProperty(name) : hasGeometryProperty(mapper.from());
	}

	private boolean hasGeometryProperty(String name) {
		return layer.getGeometryEncoder().hasAttribute(geomNode, name);
	}

	@Override
	public String[] getPropertyNames(Transaction tx) {
		return layer.getExtraPropertyNames(tx);
	}

	public Object[] getPropertyValues(Transaction tx) {
		String[] names = getPropertyNames(tx);
		if (names == null) {
			return null;
		}
		Object[] values = new Object[names.length];
		for (int i = 0; i < names.length; i++) {
			values[i] = getProperty(tx, names[i]);
		}
		return values;
	}

	@Override
	public Map<String, Object> getProperties(Transaction tx) {
		Map<String, Object> result = new HashMap<>();

		String[] names = getPropertyNames(tx);
		for (String name : names) {
			result.put(name, getProperty(tx, name));
		}

		return result;
	}

	@Override
	public Object getProperty(Transaction tx, String name) {
		PropertyMapper mapper = layer.getPropertyMappingManager().getPropertyMapper(tx, name);
		return mapper == null ? getGeometryProperty(name) : mapper.map(getGeometryProperty(mapper.from()));
	}

	private Object getGeometryProperty(String name) {
		return layer.getGeometryEncoder().getAttribute(geomNode, name);
	}

	public void setProperty(String name, Object value) {
		checkIsNotReservedProperty(name);
		geomNode.setProperty(name, value);
	}

	@Override
	public int hashCode() {
		return geomNode.getElementId().hashCode();
	}

	@Override
	public boolean equals(Object anotherObject) {
		if (!(anotherObject instanceof SpatialDatabaseRecord anotherRecord)) {
			return false;
		}

		return Objects.equals(getNodeId(), anotherRecord.getNodeId());
	}

	@Override
	public String toString() {
		return "SpatialDatabaseRecord[" + getNodeId() + "]: type='" + getType() + "', props[" + getPropString() + "]";
	}

	// Protected Constructors

	protected SpatialDatabaseRecord(Layer layer, Node geomNode, Geometry geometry) {
		this.layer = layer;
		this.geomNode = geomNode;
		this.geometry = geometry;
	}

	// Private methods

	private static void checkIsNotReservedProperty(String name) {
		for (String property : RESERVED_PROPS) {
			if (property.equals(name)) {
				throw new SpatialDatabaseException("Updating not allowed for Reserved Property: " + name);
			}
		}
	}

	private String getPropString() {
		StringBuilder text = new StringBuilder();
		for (String key : geomNode.getPropertyKeys()) {
			if (!text.isEmpty()) {
				text.append(", ");
			}
			text.append(key).append(": ").append(geomNode.getProperty(key).toString());
		}
		return text.toString();
	}
}
