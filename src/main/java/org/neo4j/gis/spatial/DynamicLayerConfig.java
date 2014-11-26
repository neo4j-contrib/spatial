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

import java.io.File;
import java.io.PrintStream;
import java.util.LinkedHashMap;

import org.geotools.filter.text.cql2.CQLException;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.attributes.PropertyMappingManager;
import org.neo4j.gis.spatial.indexfilter.CQLIndexReader;
import org.neo4j.gis.spatial.indexfilter.DynamicIndexReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.GeometryFactory;

public class DynamicLayerConfig implements Layer, Constants {

	private DynamicLayer parent;
	protected Node configNode;
	private String[] propertyNames;

	/**
	 * Construct the layer config instance on existing config information in
	 * the database.
	 * 
	 * @param configNode
	 */
	public DynamicLayerConfig(DynamicLayer parent, Node configNode) {
		this.parent = parent;
		this.configNode = configNode;
		this.propertyNames = (String[]) configNode.getProperty("propertyNames", null);
	}

	/**
	 * Construct a new layer config by building the database structure to
	 * support the necessary configuration
	 * 
	 * @param name
	 *            of the new dynamic layer
	 * @param geometryType
	 *            the geometry this layer supports
	 * @param query
	 *            formated query string for this dynamic layer
	 */
	public DynamicLayerConfig(DynamicLayer parent, String name, int geometryType, String query) {
		this.parent = parent;
		
		GraphDatabaseService database = parent.getSpatialDatabase().getDatabase();
		Transaction tx = database.beginTx();
		try {
			Node node = database.createNode();
			node.setProperty(PROP_LAYER, name);
			node.setProperty(PROP_TYPE, geometryType);
			node.setProperty(PROP_QUERY, query);
			parent.getLayerNode().createRelationshipTo(node, SpatialRelationshipTypes.LAYER_CONFIG);
			tx.success();
			configNode = node;
		} finally {
			tx.close();
		}
	}

	public String getName() {
    	try (Transaction tx = configNode.getGraphDatabase().beginTx()) {
    		String name = (String) configNode.getProperty(PROP_LAYER);
    		tx.success();
    		return name;
    	}
	}

	public String getQuery() {
    	try (Transaction tx = configNode.getGraphDatabase().beginTx()) {
    		String name = (String) configNode.getProperty(PROP_QUERY);
    		tx.success();
    		return name;
    	}
	}

	public SpatialDatabaseRecord add(Node geomNode) {
		throw new SpatialDatabaseException("Cannot add nodes to dynamic layers, add the node to the base layer instead");
	}

	public void delete(Listener monitor) {
		throw new SpatialDatabaseException("Cannot delete dynamic layers, delete the base layer instead");
	}

	public CoordinateReferenceSystem getCoordinateReferenceSystem() {
		return parent.getCoordinateReferenceSystem();
	}

	public SpatialDataset getDataset() {
		return parent.getDataset();
	}

	public String[] getExtraPropertyNames() {
		if (propertyNames != null && propertyNames.length > 0) {
			return propertyNames;
		} else {
			return parent.getExtraPropertyNames();
		}
	}

	private class PropertyUsageSearch implements SearchFilter {
		
		private Layer layer;
		private LinkedHashMap<String, Integer> names = new LinkedHashMap<String, Integer>();
		private int nodeCount = 0;
		private int MAX_COUNT = 10000;

		public PropertyUsageSearch(Layer layer) {
			this.layer = layer;
		}
		
		@Override
		public boolean needsToVisit(Envelope indexNodeEnvelope) {
			return nodeCount < MAX_COUNT;
		}

		@Override
		public boolean geometryMatches(Node geomNode) {
			if (nodeCount++ < MAX_COUNT) {
				SpatialDatabaseRecord record = new SpatialDatabaseRecord(layer, geomNode);
				for (String name : record.getPropertyNames()) {
					Object value = record.getProperty(name);
					if (value != null) {
						Integer count = names.get(name);
						if (count == null)
							count = 0;
						names.put(name, count + 1);
					}
				}
			}
			
			// no need to collect nodes
			return false;
		}
		
		public String[] getNames() {
			return names.keySet().toArray(new String[] {});				
		}
		
		public int getNodeCount() {
			return nodeCount;
		}
		
		public void describeUsage(PrintStream out) {
			for (String name : names.keySet()) {
				System.out.println(name + "\t" + names.get(name));
			}
		}
	}

	/**
	 * This method will scan the layer for property names that are actually
	 * used, and restrict the layer properties to those
	 */
	public void restrictLayerProperties() {
		if (propertyNames != null && propertyNames.length > 0) {
			System.out.println("Restricted property names already exists - will be overwritten");
		}
		
		System.out.println("Before property scan we have " + getExtraPropertyNames().length + " known attributes for layer "
				+ getName());
		
		PropertyUsageSearch search = new PropertyUsageSearch(this);
		getIndex().searchIndex(search).count();
		setExtraPropertyNames(search.getNames());
		
		System.out.println("After property scan of " + search.getNodeCount() + " nodes, we have "
				+ getExtraPropertyNames().length + " known attributes for layer " + getName());
		// search.describeUsage(System.out);
	}
	
	public void setExtraPropertyNames(String[] names) {
		try (Transaction tx = configNode.getGraphDatabase().beginTx()) {
			configNode.setProperty("propertyNames", names);
			propertyNames = names;
			tx.success();
		}
	}		
	
	public GeometryEncoder getGeometryEncoder() {
		return parent.getGeometryEncoder();
	}

	public GeometryFactory getGeometryFactory() {
		return parent.getGeometryFactory();
	}

	public Integer getGeometryType() {
		try (Transaction tx = configNode.getGraphDatabase().beginTx()) {
			Integer geometryType = (Integer) configNode.getProperty(PROP_TYPE);
			tx.success();
			return geometryType;
		}
	}

	public LayerIndexReader getIndex() {
		if (parent.index instanceof LayerTreeIndexReader) {
			String query = getQuery();
			if (query.startsWith("{")) {
				// Make a standard JSON based dynamic layer
				return new DynamicIndexReader((LayerTreeIndexReader) parent.index, query);
			} else {
				// Make a CQL based dynamic layer
				try {
					return new CQLIndexReader((LayerTreeIndexReader) parent.index, this, query);
				} catch (CQLException e) {
					throw new SpatialDatabaseException("Error while creating CQL based DynamicLayer", e);
				}
			}
		} else {
			throw new SpatialDatabaseException("Cannot make a DynamicLayer from a non-LayerTreeIndexReader Layer");
		}
	}

	public Node getLayerNode() {
		// TODO: Make sure that the mismatch between the name on the dynamic
		// layer node and the dynamic layer translates into the correct
		// object being returned
		return parent.getLayerNode();
	}

	public SpatialDatabaseService getSpatialDatabase() {
		return parent.getSpatialDatabase();
	}

	public void initialize(SpatialDatabaseService spatialDatabase, String name, Node layerNode) {
		throw new SpatialDatabaseException("Cannot initialize the layer config, initialize only the dynamic layer node");
	}

	public Object getStyle() {
		Object style = parent.getStyle();
		if (style != null && style instanceof File) {
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
}