/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.neo4j.collections.rtree.Envelope;
import org.neo4j.collections.rtree.EnvelopeDecoder;
import org.neo4j.collections.rtree.Listener;
import org.neo4j.collections.rtree.SpatialIndexRecordCounter;
import org.neo4j.collections.rtree.filter.SearchFilter;
import org.neo4j.collections.rtree.filter.SearchResults;
import org.neo4j.collections.rtree.search.Search;
import org.neo4j.gis.spatial.attributes.PropertyMapper;
import org.neo4j.gis.spatial.attributes.PropertyMappingManager;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.geotools.data.Neo4jFeatureBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.gis.spatial.pipes.GeoFilter;
import org.neo4j.gis.spatial.pipes.GeoProcessingPipeline;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.GeometryFactory;


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

	/**
	 * This class wraps a SpatialIndexReader instance, passing through all calls
	 * transparently. This is a class that should not be used as it, as it
	 * provides not additional functionality. However extending this class
	 * allows for wrapping an existing index and modifying its behaviour through
	 * overriding only specific methods. For example, override the
	 * excecuteSearch method with a modification to the search parameter.
	 * 
	 * @author craig
	 */
	public static class SpatialIndexReaderWrapper implements LayerIndexReader {
		
		protected LayerTreeIndexReader index;

		public SpatialIndexReaderWrapper(LayerTreeIndexReader index) {
			this.index = index;
		}
		
		@Override
		public Layer getLayer() {
			return index.getLayer();
		}
		
		@Override		
		public EnvelopeDecoder getEnvelopeDecoder() {
			return index.getEnvelopeDecoder();
		}

		@Override		
		public int count() {
			return index.count();
		}

		@Override		
		public void executeSearch(Search search) {
			index.executeSearch(search);
		}

		@Override		
		public boolean isNodeIndexed(Long nodeId) {
			return index.isNodeIndexed(nodeId);
		}
		
		@Override		
		public SpatialDatabaseRecord get(Long geomNodeId) {
			return index.get(geomNodeId);
		}

		@Override		
		public List<SpatialDatabaseRecord> get(Set<Long> geomNodeIds) {
			return index.get(geomNodeIds);
		}
		
		@Override		
		public Envelope getBoundingBox() {
			return index.getBoundingBox();
		}

		@Override		
		public boolean isEmpty() {
			return index.isEmpty();
		}

		@Override		
		public Iterable<Node> getAllIndexedNodes() {
			return index.getAllIndexedNodes();
		}

		@Override
		public SearchResults searchIndex(SearchFilter filter) {
			return index.searchIndex(filter);
		}

		@Override
		public SearchRecords search(SearchFilter filter) {
			return index.search(filter);
		}
	}

	/**
	 * This class enables support for CQL based dynamic layers. This means the
	 * filtering on the result set is based on matches to a CQL query. Some key
	 * differences between CQL queries and JSON queries are:
	 * <ul>
	 * <li>CQL will operate on the geometry itself, performing spatial
	 * operations, but also requiring the geometry to be created from the graph.
	 * This makes it slower than the JSON approach, but richer from a GIS
	 * perspective</li>
	 * <li>JSON will operate on the graph itself, and so it is more specific to
	 * the data model, and not at all specific to the GIS meaning of the data.
	 * This makes it faster, but more complex to develop to. You really need to
	 * know your graph structure well to write a complex JSON query. For simple
	 * single-node property matches, this is the easiest solution.</li>
	 * </ul>
	 * 
	 * @author dwins
	 */
    public class CQLIndexReader extends SpatialIndexReaderWrapper {
        private final Filter filter;
        private final Neo4jFeatureBuilder builder;
        private final Layer layer;

        public CQLIndexReader(LayerTreeIndexReader index, Layer layer, String query) throws CQLException {
            super(index);
            this.filter = ECQL.toFilter(query);
            this.builder = new Neo4jFeatureBuilder(layer);
            this.layer = layer;
        }

        private class Counter extends SpatialIndexRecordCounter {
            public boolean needsToVisit(Envelope indexNodeEnvelope) {
                return queryIndexNode(indexNodeEnvelope);
            }

            public void onIndexReference(Node geomNode) {
                if (queryLeafNode(geomNode)) {
                    super.onIndexReference(geomNode);
                }
            }
        }

        private class FilteredSearch implements Search {
        	
            private Search delegate;
            
            public FilteredSearch(Search delegate) {
                this.delegate = delegate;
            }

			@Override            
            public List<Node> getResults() {
                return delegate.getResults();
            }

			@Override            
            public boolean needsToVisit(Envelope indexNodeEnvelope) {
                return delegate.needsToVisit(indexNodeEnvelope);
            }

			@Override            
            public void onIndexReference(Node geomNode) {
                if (queryLeafNode(geomNode)) {
                    delegate.onIndexReference(geomNode);
                }
            }
        }        
        
        private class FilteredLayerSearch implements LayerSearch {
        	
            private LayerSearch delegate;
            
            public FilteredLayerSearch(LayerSearch delegate) {
                this.delegate = delegate;
            }

			@Override            
            public List<Node> getResults() {
                return delegate.getResults();
            }

			@Override            
            public boolean needsToVisit(Envelope indexNodeEnvelope) {
                return delegate.needsToVisit(indexNodeEnvelope);
            }

			@Override            
            public void onIndexReference(Node geomNode) {
                if (queryLeafNode(geomNode)) {
                    delegate.onIndexReference(geomNode);
                }
            }

			@Override
			public void setLayer(Layer layer) {
				delegate.setLayer(layer);
			}

			@Override
			public List<SpatialDatabaseRecord> getExtendedResults() {
				return delegate.getExtendedResults();
			}
        }

        private boolean queryIndexNode(Envelope indexNodeEnvelope) {
            return true;
        }

		private boolean queryLeafNode(Node indexNode) {
			SpatialDatabaseRecord dbRecord = new SpatialDatabaseRecord(layer, indexNode);
			SimpleFeature feature = builder.buildFeature(dbRecord);
			return filter.evaluate(feature);
		}

   		public int count() {
			Counter counter = new Counter();
			index.visit(counter, index.getIndexRoot());
			return counter.getResult();
		}

		public void executeSearch(final Search search) {
			if (LayerSearch.class.isAssignableFrom(search.getClass())) {
				index.executeSearch(new FilteredLayerSearch((LayerSearch) search));
			} else {
				index.executeSearch(new FilteredSearch(search));
			}
		}
    }

	/**
	 * The standard DynamicIndexReader allows for graph traversal and property
	 * match queries written in JSON. The JSON code is expected to be a match
	 * for a sub-graph and it's properties. It only supports tree structures,
	 * since JSON is a tree format. The root of the JSON is the geometry node,
	 * which means that queries for properties on nodes further away require
	 * traversals in the JSON. The following example demonstrates a query for
	 * an OSM geometry layer, with a test of the geometry type on the geometry
	 * node itself, followed by a two step traversal to the ways tag node, and
	 * then a query on the tags.
	 * 
	 * <pre>
	 * { "properties": {"type": "geometry"},
	 *   "step": {"type": "GEOM", "direction": "INCOMING"
	 *     "step": {"type": "TAGS", "direction": "OUTGOING"
	 *       "properties": {"highway": "residential"}
	 *     }
	 *   }
	 * }
	 * </pre>
	 * 
	 * This will work with OSM datasets, traversing from the geometry node to
	 * the way node and then to the tags node to test if the way is a
	 * residential street.
	 * 
	 * @author craig
	 * 
	 */
	public class DynamicIndexReader extends SpatialIndexReaderWrapper {
		private JSONObject query;

		private class DynamicRecordCounter extends SpatialIndexRecordCounter {
			public boolean needsToVisit(Envelope indexNodeEnvelope) {
				return queryIndexNode(indexNodeEnvelope);
			}

			public void onIndexReference(Node geomNode) {
				if (queryLeafNode(geomNode)) {
					super.onIndexReference(geomNode);
				}
			}
		}

		public DynamicIndexReader(LayerTreeIndexReader index, String query) {
			super(index);
			this.query = (JSONObject)JSONValue.parse(query);
		}

		private boolean queryIndexNode(Envelope indexNodeEnvelope) {
			// TODO: Support making the query on each index node for performance
			return true;
		}

		/**
		 * This method is there the real querying is done. It first tests for
		 * properties on the geometry node, and then steps though the tree
		 * structure of the JSON, and a matching structure in the graph,
		 * querying recursively each nodes properties on the way, as along as
		 * the JSON contains to have properties to test, and traversal steps to
		 * take.
		 * 
		 * @param geomNode
		 * @return true if the node matches the query string, or the query
		 *         string is empty
		 */
		private boolean queryLeafNode(Node geomNode) {
			// TODO: Extend support for more complex queries
			JSONObject properties = (JSONObject)query.get("properties");
			JSONObject step = (JSONObject)query.get("step");
			return queryNodeProperties(geomNode,properties) && stepAndQuery(geomNode,step);
		}
		
		private boolean stepAndQuery(Node source, JSONObject step) {
			if (step != null) {
				JSONObject properties = (JSONObject) step.get("properties");
				Relationship rel = source.getSingleRelationship(DynamicRelationshipType.withName(step.get("type").toString()), Direction
				        .valueOf(step.get("direction").toString()));
				if (rel != null) {
					Node node = rel.getOtherNode(source);
					step = (JSONObject) step.get("step");
					return queryNodeProperties(node, properties) && stepAndQuery(node, step);
				} else {
					return false;
				}
			} else {
				return true;
			}
		}

		private boolean queryNodeProperties(Node node, JSONObject properties) {
			if (properties != null) {
				if(properties.containsKey("geometry")){
					System.out.println("Unexpected 'geometry' in query string");
					properties.remove("geometry");
				}
				for (Object key : properties.keySet()) {
					Object value = node.getProperty(key.toString(), null);
					Object match = properties.get(key);
					//TODO: Find a better way to solve minor type mismatches (Long!=Integer) than the string conversion below
					if (value == null || (match != null && !value.equals(match) && !value.toString().equals(match.toString()))) {
						return false;
					}
				}
			}
			return true;
		}

		public int count() {
			DynamicRecordCounter counter = new DynamicRecordCounter();
			index.visit(counter, index.getIndexRoot());
			return counter.getResult();
		}

		public void executeSearch(final Search search) {
			if (LayerSearch.class.isAssignableFrom(LayerSearch.class)) {
				((LayerSearch) search).setLayer(DynamicLayer.this);
			}
			
			index.executeSearch(new Search() {

				public List<Node> getResults() {
					return search.getResults();
				}

				public boolean needsToVisit(Envelope indexNodeEnvelope) {
					return search.needsToVisit(indexNodeEnvelope);
				}

				public void onIndexReference(Node geomNode) {
					if (queryLeafNode(geomNode)) {
						search.onIndexReference(geomNode);
					}
				}
			});
		}

	}

	/**
	 * <p>
	 * The LayerConfig class exposes the rules encoded in the layer config node
	 * for a custom layer expressed from a single layer node. This configuration
	 * can be a set of properties to match in order to be considered part of the
	 * layer, or it can be a more complex custom traverser.
	 * </p>
	 * 
	 * @author craig
	 * @since 1.0.0
	 */
	public class LayerConfig implements Layer {
		private Node configNode;
		private String[] propertyNames;

		/**
		 * Construct the layer config instance on existing config information in
		 * the database.
		 * 
		 * @param configNode
		 */
		public LayerConfig(Node configNode) {
			this.configNode = configNode;
			this.propertyNames = (String[])configNode.getProperty("propertyNames", null);
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
		public LayerConfig(String name, int geometryType, String query) {
			GraphDatabaseService database = DynamicLayer.this.getSpatialDatabase().getDatabase();
			Transaction tx = database.beginTx();
			try {
				Node node = database.createNode();
				node.setProperty(PROP_LAYER, name);
				node.setProperty(PROP_TYPE, geometryType);
				node.setProperty(PROP_QUERY, query);
				DynamicLayer.this.getLayerNode().createRelationshipTo(node, SpatialRelationshipTypes.LAYER_CONFIG);
				tx.success();
				configNode = node;
			} finally {
				tx.finish();
			}
		}

		public String getName() {
			return (String) configNode.getProperty(PROP_LAYER);
		}

		public String getQuery() {
			return (String) configNode.getProperty(PROP_QUERY);
		}

		public SpatialDatabaseRecord add(Node geomNode) {
			throw new SpatialDatabaseException("Cannot add nodes to dynamic layers, add the node to the base layer instead");
		}

		public void delete(Listener monitor) {
			throw new SpatialDatabaseException("Cannot delete dynamic layers, delete the base layer instead");
		}

		public CoordinateReferenceSystem getCoordinateReferenceSystem() {
			return DynamicLayer.this.getCoordinateReferenceSystem();
		}

		public SpatialDataset getDataset() {
			return DynamicLayer.this.getDataset();
		}

		public String[] getExtraPropertyNames() {
			if (propertyNames != null && propertyNames.length > 0) {
				return propertyNames;
			} else {
				return DynamicLayer.this.getExtraPropertyNames();
			}
		}

		private class PropertyUsageSearch extends AbstractLayerSearch {
			private LinkedHashMap<String, Integer> names = new LinkedHashMap<String, Integer>();
			private int nodeCount = 0;
			private int MAX_COUNT = 10000;

			@Override
			public boolean needsToVisit(Envelope indexNodeEnvelope) {
				return nodeCount < MAX_COUNT;
			}

			@Override
			public void onIndexReference(Node geomNode) {
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
			PropertyUsageSearch search = new PropertyUsageSearch();
			getIndex().executeSearch(search);
			setExtraPropertyNames(search.getNames());
			System.out.println("After property scan of " + search.getNodeCount() + " nodes, we have "
					+ getExtraPropertyNames().length + " known attributes for layer " + getName());
			//search.describeUsage(System.out);
		}
		
		public void setExtraPropertyNames(String[] names) {
			Transaction tx = configNode.getGraphDatabase().beginTx();
			try {
				configNode.setProperty("propertyNames", names);
				propertyNames = names;
				tx.success();
			} finally {
				tx.finish();
			}
		}

		public GeometryEncoder getGeometryEncoder() {
			return DynamicLayer.this.getGeometryEncoder();
		}

		public GeometryFactory getGeometryFactory() {
			return DynamicLayer.this.getGeometryFactory();
		}

		public Integer getGeometryType() {
			return (Integer) configNode.getProperty(PROP_TYPE);
		}

		public LayerIndexReader getIndex() {
			if (index instanceof LayerTreeIndexReader) {
				String query = getQuery();
				if (query.startsWith("{")) {
					// Make a standard JSON based dynamic layer
					return new DynamicIndexReader((LayerTreeIndexReader) index, getQuery());
				} else {
					// Make a CQL based dynamic layer
					try {
						return new CQLIndexReader((LayerTreeIndexReader) index, this, getQuery());
					} catch (CQLException e) {
						throw new SpatialDatabaseException("Error while creating CQL based DynamicLayer", e);
					}
				}
			} else {
				throw new SpatialDatabaseException("Cannot make a DynamicLayer from a non-SpatialTreeIndex Layer");
			}
		}

		public Node getLayerNode() {
			// TODO: Make sure that the mismatch between the name on the dynamic
			// layer node and the dynamic layer translates into the correct
			// object being returned
			return DynamicLayer.this.getLayerNode();
		}

		public SpatialDatabaseService getSpatialDatabase() {
			return DynamicLayer.this.getSpatialDatabase();
		}

		public void initialize(SpatialDatabaseService spatialDatabase, String name, Node layerNode) {
			throw new SpatialDatabaseException("Cannot initialize the layer config, initialize only the dynamic layer node");
		}

		public Object getStyle() {
			Object style = DynamicLayer.this.getStyle();
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
			return DynamicLayer.this;
		}
		
		public String toString() {
			return getName();
		}

		@Override
		public GeoFilter filter() {
			return new GeoFilter(this);
		}

		@Override
		public GeoProcessingPipeline<SpatialDatabaseRecord, SpatialDatabaseRecord> process() {
			return new GeoProcessingPipeline<SpatialDatabaseRecord, SpatialDatabaseRecord>(this);
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

	private synchronized Map<String, Layer> getLayerMap() {
		if (layers == null) {
			layers = new LinkedHashMap<String, Layer>();
			layers.put(getName(), this);
			for (Relationship rel : layerNode.getRelationships(SpatialRelationshipTypes.LAYER_CONFIG, Direction.OUTGOING)) {
				LayerConfig config = new LayerConfig(rel.getEndNode());
				layers.put(config.getName(), config);
			}
		}
		return layers;
	}
	
	protected boolean removeLayerConfig(String name) {
		Layer layer = getLayerMap().get(name);
		if (layer != null && layer instanceof LayerConfig) {
			synchronized (this) {
				LayerConfig config = (LayerConfig)layer;
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

	public LayerConfig addCQLDynamicLayerOnGeometryType(int gtype) {
		return addLayerConfig("CQL:" + makeGeometryName(gtype), gtype, makeGeometryCQL(gtype));
	}

	public LayerConfig addCQLDynamicLayerOnAttribute(String key, String value, int gtype) {
		if (value == null) {
			return addLayerConfig("CQL:" + key, gtype, key + " IS NOT NULL AND " + makeGeometryCQL(gtype));
		} else {
			// TODO: Better escaping here
			//return addLayerConfig("CQL:" + key + "-" + value, gtype, key + " = '" + value + "' AND " + makeGeometryCQL(gtype));
			return addCQLDynamicLayerOnAttributes(new String[] { key, value }, gtype);
		}
	}

	public LayerConfig addCQLDynamicLayerOnAttributes(String[] attributes, int gtype) {
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
	
	public LayerConfig addLayerConfig(String name, int type, String query) {
		if(!query.startsWith("{")) {
			// Not a JSON query, must be CQL, so check the syntax
	        try {
	            ECQL.toFilter(query);
	        } catch (CQLException e) {
	            throw new SpatialDatabaseException("DynamicLayer query is not JSON and not valid CQL: " + query, e);
	        }
		}

		Layer layer = getLayerMap().get(name);
		if (layer != null) {
			if (layer instanceof LayerConfig) {
				LayerConfig config = (LayerConfig) layer;
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
			LayerConfig config = new LayerConfig(name, type, query);
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
	public LayerConfig restrictLayerProperties(String name, String[] names) {
		Layer layer = getLayerMap().get(name);
		if (layer != null) {
			if (layer instanceof LayerConfig) {
				LayerConfig config = (LayerConfig) layer;
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
	public LayerConfig restrictLayerProperties(String name) {
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
