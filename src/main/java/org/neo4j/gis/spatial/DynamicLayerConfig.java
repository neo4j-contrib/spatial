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

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.filter.text.cql2.CQLException;
import org.locationtech.jts.geom.GeometryFactory;
import org.neo4j.gis.spatial.attributes.PropertyMappingManager;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.index.LayerIndexReader;
import org.neo4j.gis.spatial.index.LayerTreeIndexReader;
import org.neo4j.gis.spatial.indexfilter.CQLIndexReader;
import org.neo4j.gis.spatial.indexfilter.DynamicIndexReader;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class DynamicLayerConfig implements Layer, Constants {

	private final DynamicLayer parent;
	private final String name;
	private final int geometryType;
	private final String query;
	protected final String configNodeId;
	private String[] propertyNames;

	/**
	 * Construct the layer config instance on existing config information in the database.
	 */
	public DynamicLayerConfig(DynamicLayer parent, Node configNode) {
		this.parent = parent;
		this.name = (String) configNode.getProperty(PROP_LAYER);
		this.geometryType = (Integer) configNode.getProperty(PROP_TYPE);
		this.query = (String) configNode.getProperty(PROP_QUERY);
		this.configNodeId = configNode.getElementId();
		this.propertyNames = (String[]) configNode.getProperty("propertyNames", null);
	}

	/**
	 * Construct a new layer config by building the database structure to support the necessary configuration
	 *
	 * @param tx           the Transaction in which this config is created
	 * @param parent       the DynamicLayer containing this config
	 * @param name         of the new dynamic layer
	 * @param geometryType the geometry this layer supports
	 * @param query        formatted query string for this dynamic layer
	 */
	public DynamicLayerConfig(Transaction tx, DynamicLayer parent, String name, int geometryType, String query) {
		this.parent = parent;
		Node node = tx.createNode();
		node.setProperty(PROP_LAYER, name);
		node.setProperty(PROP_TYPE, geometryType);
		node.setProperty(PROP_QUERY, query);
		parent.getLayerNode(tx).createRelationshipTo(node, SpatialRelationshipTypes.LAYER_CONFIG);
		this.name = name;
		this.geometryType = geometryType;
		this.query = query;
		configNodeId = node.getElementId();
	}

	@Override
	public String getName() {
		return name;
	}

	public String getQuery() {
		return query;
	}

	@Override
	public SpatialDatabaseRecord add(Transaction tx, Node geomNode) {
		throw new SpatialDatabaseException(
				"Cannot add nodes to dynamic layers, add the node to the base layer instead");
	}

	@Override
	public int addAll(Transaction tx, List<Node> geomNodes) {
		throw new SpatialDatabaseException(
				"Cannot add nodes to dynamic layers, add the node to the base layer instead");
	}

	@Override
	public void delete(Transaction tx, Listener monitor) {
		throw new SpatialDatabaseException("Cannot delete dynamic layers, delete the base layer instead");
	}

	@Override
	public CoordinateReferenceSystem getCoordinateReferenceSystem(Transaction tx) {
		return parent.getCoordinateReferenceSystem(tx);
	}

	@Override
	public SpatialDataset getDataset() {
		return parent.getDataset();
	}

	@Override
	public String[] getExtraPropertyNames(Transaction tx) {
		if (propertyNames != null && propertyNames.length > 0) {
			return propertyNames;
		}
		return parent.getExtraPropertyNames(tx);
	}

	private static class PropertyUsageSearch implements SearchFilter {

		private final Layer layer;
		private final LinkedHashMap<String, Integer> names = new LinkedHashMap<>();
		private int nodeCount = 0;
		private final int MAX_COUNT = 10000;

		public PropertyUsageSearch(Layer layer) {
			this.layer = layer;
		}

		@Override
		public boolean needsToVisit(Envelope indexNodeEnvelope) {
			return nodeCount < MAX_COUNT;
		}

		@Override
		public boolean geometryMatches(Transaction tx, Node geomNode) {
			if (nodeCount++ < MAX_COUNT) {
				SpatialDatabaseRecord record = new SpatialDatabaseRecord(layer, geomNode);
				for (String name : record.getPropertyNames(tx)) {
					Object value = record.getProperty(tx, name);
					if (value != null) {
						Integer count = names.get(name);
						if (count == null) {
							count = 0;
						}
						names.put(name, count + 1);
					}
				}
			}

			// no need to collect nodes
			return false;
		}

		public String[] getNames() {
			return names.keySet().toArray(new String[]{});
		}

		public int getNodeCount() {
			return nodeCount;
		}
	}

	/**
	 * This method will scan the layer for property names that are actually
	 * used, and restrict the layer properties to those
	 */
	public void restrictLayerProperties(Transaction tx) {
		if (propertyNames != null && propertyNames.length > 0) {
			System.out.println("Restricted property names already exists - will be overwritten");
		}
		System.out.println(
				"Before property scan we have " + getExtraPropertyNames(tx).length + " known attributes for layer "
						+ getName());

		PropertyUsageSearch search = new PropertyUsageSearch(this);
		getIndex().searchIndex(tx, search).count();
		setExtraPropertyNames(tx, search.getNames());

		System.out.println(
				"After property scan of " + search.getNodeCount() + " nodes, we have " + getExtraPropertyNames(
						tx).length + " known attributes for layer " + getName());
	}

	public Node configNode(Transaction tx) {
		return tx.getNodeByElementId(configNodeId);
	}

	public void setExtraPropertyNames(Transaction tx, String[] names) {
		configNode(tx).setProperty("propertyNames", names);
		propertyNames = names;
	}

	@Override
	public GeometryEncoder getGeometryEncoder() {
		return parent.getGeometryEncoder();
	}

	@Override
	public GeometryFactory getGeometryFactory() {
		return parent.getGeometryFactory();
	}

	@Override
	public Integer getGeometryType(Transaction tx) {
		return (Integer) configNode(tx).getProperty(PROP_TYPE);
	}

	@Override
	public LayerIndexReader getIndex() {
		if (parent.indexReader instanceof LayerTreeIndexReader) {
			String query = getQuery();
			if (query.startsWith("{")) {
				// Make a standard JSON based dynamic layer
				return new DynamicIndexReader((LayerTreeIndexReader) parent.indexReader, query);
			}
			try {
				// Make a CQL based dynamic layer
				return new CQLIndexReader((LayerTreeIndexReader) parent.indexReader, this, query);
			} catch (CQLException e) {
				throw new SpatialDatabaseException("Error while creating CQL based DynamicLayer", e);
			}
		}
		throw new SpatialDatabaseException("Cannot make a DynamicLayer from a non-LayerTreeIndexReader Layer");
	}

	@Override
	public Node getLayerNode(Transaction tx) {
		// TODO: Make sure that the mismatch between the name on the dynamic
		// layer node and the dynamic layer translates into the correct
		// object being returned
		return parent.getLayerNode(tx);
	}

	@Override
	public void initialize(Transaction tx, IndexManager indexManager, String name, Node layerNode) {
		throw new SpatialDatabaseException(
				"Cannot initialize the layer config, initialize only the dynamic layer node");
	}

	@Override
	public Object getStyle() {
		Object style = parent.getStyle();
		if (style instanceof File) {
			File parent = ((File) style).getParentFile();
			File newStyle = new File(parent, getName() + ".sld");
			if (newStyle.canRead()) {
				style = newStyle;
			}
		}
		return style;
	}

	public Layer getParent() {
		return parent;
	}

	@Override
	public String toString() {
		return getName();
	}

	private PropertyMappingManager propertyMappingManager;

	@Override
	public PropertyMappingManager getPropertyMappingManager() {
		if (propertyMappingManager == null) {
			propertyMappingManager = new PropertyMappingManager(this);
		}
		return propertyMappingManager;
	}

	protected Map<String, String> getConfig() {
		Map<String, String> config = new LinkedHashMap<>();
		config.put("layer", this.name);
		config.put("type", String.valueOf(this.geometryType));
		config.put("query", this.query);
		return config;
	}

	@Override
	public String getSignature() {
		Map<String, String> config = getConfig();
		return "DynamicLayer(name='" + getName() + "', config={layer='" + config.get("layer") + "', query=\""
				+ config.get("query") + "\"})";
	}
}
