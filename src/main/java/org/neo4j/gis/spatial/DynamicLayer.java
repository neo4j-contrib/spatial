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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.neo4j.graphdb.Direction;
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
 * 
 * @author craig
 * @since 1.0.0
 */
public class DynamicLayer extends EditableLayerImpl {

	private LinkedHashMap<String, Layer> layers;

	private synchronized Map<String, Layer> getLayerMap() {
		if (layers == null) {
			layers = new LinkedHashMap<String, Layer>();
			layers.put(getName(), this);
			for (Relationship rel : layerNode.getRelationships(SpatialRelationshipTypes.LAYER_CONFIG, Direction.OUTGOING)) {
				DynamicLayerConfig config = new DynamicLayerConfig(this, rel.getEndNode());
				layers.put(config.getName(), config);
			}
		}
		return layers;
	}
	
	protected boolean removeLayerConfig(String name) {
		Layer layer = getLayerMap().get(name);
		if (layer != null && layer instanceof DynamicLayerConfig) {
			synchronized (this) {
				DynamicLayerConfig config = (DynamicLayerConfig) layer;
				layers = null; // force recalculation of layers cache
				Transaction tx = config.configNode.getGraphDatabase().beginTx();
				try {
					config.configNode.getSingleRelationship(SpatialRelationshipTypes.LAYER_CONFIG, Direction.INCOMING).delete();
					config.configNode.delete();
					tx.success();
				} finally {
					tx.finish();
				}
				return true;
			}
		} else if (layer == null) {
			System.out.println("Dynamic layer not found: " + name);
			return false;
		} else {
			System.out.println("Layer is not dynamic and cannot be deleted: " + name);
			return false;
		}
	}

	private static String makeGeometryName(int gtype) {
		return SpatialDatabaseService.convertGeometryTypeToName(gtype);
	}

	private static String makeGeometryCQL(int gtype) {
		return "geometryType(the_geom) = '" + makeGeometryName(gtype) + "'";
	}

	public DynamicLayerConfig addCQLDynamicLayerOnGeometryType(int gtype) {
		return addLayerConfig("CQL:" + makeGeometryName(gtype), gtype, makeGeometryCQL(gtype));
	}

	public DynamicLayerConfig addCQLDynamicLayerOnAttribute(String key, String value, int gtype) {
		if (value == null) {
			return addLayerConfig("CQL:" + key, gtype, key + " IS NOT NULL AND " + makeGeometryCQL(gtype));
		} else {
			// TODO: Better escaping here
			//return addLayerConfig("CQL:" + key + "-" + value, gtype, key + " = '" + value + "' AND " + makeGeometryCQL(gtype));
			return addCQLDynamicLayerOnAttributes(new String[] { key, value }, gtype);
		}
	}

	public DynamicLayerConfig addCQLDynamicLayerOnAttributes(String[] attributes, int gtype) {
		if (attributes == null) {
			return addCQLDynamicLayerOnGeometryType(gtype);
		} else {
			StringBuffer name = new StringBuffer();
			StringBuffer query = new StringBuffer();
			if (gtype != GTYPE_GEOMETRY) {
				query.append(makeGeometryCQL(gtype));
			}
			for (int i = 0; i < attributes.length; i += 2) {
				String key = attributes[i];
				if (name.length() > 0) {
					name.append("-");
				}
				if (query.length() > 0) {
					query.append(" AND ");
				}
				if (attributes.length > i) {
					String value = attributes[i + 1];
					name.append(key).append("-").append(value);
					query.append(key).append(" = '").append(value).append("'");
				} else {
					name.append(key);
					query.append(key).append(" IS NOT NULL");
				}
			}
			return addLayerConfig("CQL:" + name.toString(), gtype, query.toString());
		}
	}
	
	public DynamicLayerConfig addLayerConfig(String name, int type, String query) {
		if (!query.startsWith("{")) {
			// Not a JSON query, must be CQL, so check the syntax
	        try {
	            ECQL.toFilter(query);
	        } catch (CQLException e) {
	            throw new SpatialDatabaseException("DynamicLayer query is not JSON and not valid CQL: " + query, e);
	        }
		}

		Layer layer = getLayerMap().get(name);
		if (layer != null) {
			if (layer instanceof DynamicLayerConfig) {
				DynamicLayerConfig config = (DynamicLayerConfig) layer;
				if (config.getGeometryType() != type || !config.getQuery().equals(query)) {
					System.err.println("Existing LayerConfig with different geometry type or query: " + config);
					return null;
				} else {
					return config;
				}
			} else {
				System.err.println("Existing Layer has same name as requested LayerConfig: " + layer.getName());
				return null;
			}
		} else synchronized (this) {
			DynamicLayerConfig config = new DynamicLayerConfig(this, name, type, query);
			layers = null;	// force recalculation of layers cache
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
	 * @param name of layer to restrict
	 * @param names to use for attributes
	 * @return
	 */
	public DynamicLayerConfig restrictLayerProperties(String name, String[] names) {
		Layer layer = getLayerMap().get(name);
		if (layer != null) {
			if (layer instanceof DynamicLayerConfig) {
				DynamicLayerConfig config = (DynamicLayerConfig) layer;
				if (names == null) {
					config.restrictLayerProperties();
				} else {
					config.setExtraPropertyNames(names);
				}
				return config;
			} else {
				System.err.println("Existing Layer has same name as requested LayerConfig: " + layer.getName());
				return null;
			}
		} else {
			System.err.println("No such layer: " + name);
			return null;
		}
	}

	/**
	 * Restrict specified layers attributes to only those that are actually
	 * found to be used. This does an exhaustive search and can be time
	 * consuming. For large layers, consider manually setting the properties
	 * instead.
	 * 
	 * @param name
	 * @return
	 */
	public DynamicLayerConfig restrictLayerProperties(String name) {
		return restrictLayerProperties(name, null);
	}
	
	public List<String> getLayerNames() {
		return new ArrayList<String>(getLayerMap().keySet());
	}

	public List<Layer> getLayers() {
		return new ArrayList<Layer>(getLayerMap().values());
	}

	public Layer getLayer(String name) {
		return getLayerMap().get(name);
	}
}
