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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

/**
 * <p>
 * The DynamicLayer class extends a Layer to be able to express itself as
 * several Layers. Each of these 'sub-layers' is defined by adding filters to
 * the original layer. The filters are configured in the LayerConfig class, on a
 * set of nodes related to the original dynamic layer node by LAYER_CONFIG
 * relationships. One key example of where this type of capability is very
 * valuable is for example when a layer contains geometries of multiple types,
 * but geotools can only express one type in each layer. Then we can use
 * DynamicLayer to expose each of the different geometry types as a different
 * layer to the consuming application (desktop or web application).
 * </p>
 * <p>
 * DynamicLayer extends EdiableLayerImpl, and is therefore editable. Not that
 * this support is dependant on the correct working of the appropriate
 * GeometryEncoder, and also does not extend to the sub-layers provided. Those
 * are read-only views.
 * </p>
 */
public class DynamicLayer extends EditableLayerImpl {

	private LinkedHashMap<String, Layer> layers;

	private synchronized Map<String, Layer> getLayerMap(Transaction tx) {
		if (layers == null) {
			layers = new LinkedHashMap<>();
			layers.put(getName(), this);
			try (var relationships = getLayerNode(tx).getRelationships(Direction.OUTGOING,
					SpatialRelationshipTypes.LAYER_CONFIG)) {
				for (Relationship rel : relationships) {
					DynamicLayerConfig config = new DynamicLayerConfig(this, rel.getEndNode());
					layers.put(config.getName(), config);
				}
			}
		}
		return layers;
	}

	protected boolean removeLayerConfig(Transaction tx, String name) {
		Layer layer = getLayerMap(tx).get(name);
		if (layer instanceof DynamicLayerConfig) {
			synchronized (this) {
				DynamicLayerConfig config = (DynamicLayerConfig) layer;
				layers = null; // force recalculation of layers cache
				Node configNode = config.configNode(tx);
				configNode.getSingleRelationship(SpatialRelationshipTypes.LAYER_CONFIG, Direction.INCOMING).delete();
				configNode.delete();
				return true;
			}
		}
		if (layer == null) {
			System.out.println("Dynamic layer not found: " + name);
			return false;
		}
		System.out.println("Layer is not dynamic and cannot be deleted: " + name);
		return false;
	}

	private static String makeGeometryName(int gtype) {
		return SpatialDatabaseService.convertGeometryTypeToName(gtype);
	}

	private static String makeGeometryCQL(int gtype) {
		return "geometryType(the_geom) = '" + makeGeometryName(gtype) + "'";
	}

	public DynamicLayerConfig addCQLDynamicLayerOnGeometryType(Transaction tx, int gtype) {
		return addLayerConfig(tx, "CQL:" + makeGeometryName(gtype), gtype, makeGeometryCQL(gtype));
	}

	public DynamicLayerConfig addCQLDynamicLayerOnAttribute(Transaction tx, String key, String value, int gtype) {
		if (value == null) {
			return addLayerConfig(tx, "CQL:" + key, gtype, key + " IS NOT NULL AND " + makeGeometryCQL(gtype));
		}
		// TODO: Better escaping here
		//return addLayerConfig("CQL:" + key + "-" + value, gtype, key + " = '" + value + "' AND " + makeGeometryCQL(gtype));
		return addCQLDynamicLayerOnAttributes(tx, new String[]{key, value}, gtype);
	}

	public DynamicLayerConfig addCQLDynamicLayerOnAttributes(Transaction tx, String[] attributes, int gtype) {
		if (attributes == null) {
			return addCQLDynamicLayerOnGeometryType(tx, gtype);
		}
		StringBuilder name = new StringBuilder();
		StringBuilder query = new StringBuilder();
		if (gtype != GTYPE_GEOMETRY) {
			query.append(makeGeometryCQL(gtype));
		}
		for (int i = 0; i < attributes.length; i += 2) {
			String key = attributes[i];
			if (!name.isEmpty()) {
				name.append("-");
			}
			if (!query.isEmpty()) {
				query.append(" AND ");
			}
			if (attributes.length > i + 1) {
				String value = attributes[i + 1];
				name.append(key).append("-").append(value);
				query.append(key).append(" = '").append(value).append("'");
			} else {
				name.append(key);
				query.append(key).append(" IS NOT NULL");
			}
		}
		return addLayerConfig(tx, "CQL:" + name, gtype, query.toString());
	}

	public DynamicLayerConfig addLayerConfig(Transaction tx, String name, int type, String query) {
		// Not a JSON query, must be CQL, so check the syntax
		if (!query.startsWith("{")) {
			try {
				ECQL.toFilter(query);
			} catch (CQLException e) {
				throw new SpatialDatabaseException("DynamicLayer query is not JSON and not valid CQL: " + query, e);
			}
		}

		Layer layer = getLayerMap(tx).get(name);
		if (layer != null) {
			if (layer instanceof DynamicLayerConfig config) {
				if (config.getGeometryType(tx) != type || !config.getQuery().equals(query)) {
					System.err.println("Existing LayerConfig with different geometry type or query: " + config);
					return null;
				}
				return config;
			}
			System.err.println("Existing Layer has same name as requested LayerConfig: " + layer.getName());
			return null;
		}
		synchronized (this) {
			DynamicLayerConfig config = new DynamicLayerConfig(tx, this, name, type, query);
			layers = null;    // force recalculation of layers cache
			return config;
		}
	}

	/**
	 * Restrict specified layers attributes to the specified set. This will simply
	 * save the quest to the LayerConfig node, so that future queries will only return
	 * attributes that are within the named list. If you want to have it perform
	 * and automatic search, pass null for the names list, but be warned, this can
	 * take a long time on large datasets.
	 *
	 * @param name  of layer to restrict
	 * @param names to use for attributes
	 */
	public DynamicLayerConfig restrictLayerProperties(Transaction tx, String name, String[] names) {
		Layer layer = getLayerMap(tx).get(name);
		if (layer != null) {
			if (layer instanceof DynamicLayerConfig config) {
				if (names == null) {
					config.restrictLayerProperties(tx);
				} else {
					config.setExtraPropertyNames(tx, names);
				}
				return config;
			}
			System.err.println("Existing Layer has same name as requested LayerConfig: " + layer.getName());
			return null;
		}
		System.err.println("No such layer: " + name);
		return null;
	}

	/**
	 * Restrict specified layers attributes to only those that are actually
	 * found to be used. This does an exhaustive search and can be time-consuming. For large layers, consider
	 * manually setting the properties instead.
	 */
	public DynamicLayerConfig restrictLayerProperties(Transaction tx, String name) {
		return restrictLayerProperties(tx, name, null);
	}

	public List<String> getLayerNames(Transaction tx) {
		return new ArrayList<>(getLayerMap(tx).keySet());
	}

	public List<Layer> getLayers(Transaction tx) {
		return new ArrayList<>(getLayerMap(tx).values());
	}

	public Layer getLayer(Transaction tx, String name) {
		return getLayerMap(tx).get(name);
	}
}
