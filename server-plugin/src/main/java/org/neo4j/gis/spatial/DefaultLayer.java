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

import static org.neo4j.gis.spatial.Constants.PROP_CRS;
import static org.neo4j.gis.spatial.Constants.PROP_LAYERNODEEXTRAPROPS;
import static org.neo4j.gis.spatial.Constants.PROP_PREFIX_EXTRA_PROP_V2;
import static org.neo4j.gis.spatial.Constants.PROP_TYPE;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.neo4j.gis.spatial.attributes.PropertyMappingManager;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.spatial.api.Envelope;
import org.neo4j.spatial.api.SearchFilter;
import org.neo4j.spatial.api.SpatialDataset;
import org.neo4j.spatial.api.encoder.GeometryEncoder;
import org.neo4j.spatial.api.index.IndexManager;
import org.neo4j.spatial.api.index.SpatialIndexReader;
import org.neo4j.spatial.api.index.SpatialIndexWriter;
import org.neo4j.spatial.api.layer.Layer;
import org.neo4j.spatial.geotools.common.utilities.GeotoolsAdapter;

/**
 * Instances of Layer provide the ability for developers to query geometries associated with a single dataset (or
 * layer).
 * A Layer can be associated with a dataset. In cases where the dataset contains only one layer, the layer
 * itself is the dataset.
 * <p>
 * You should not construct the DefaultLayer directly but use the factory methods
 * on the SpatialDatabaseService for correct initialization.
 */
public abstract class DefaultLayer implements Layer, InternalLayer, SpatialDataset {

	protected String layerNodeId = null;

	// Public methods
	protected SpatialIndexReader indexReader;
	private PropertyMappingManager propertyMappingManager;
	//private SpatialDatabaseService spatialDatabase;
	private String name;
	private GeometryEncoder geometryEncoder;
	private GeometryFactory geometryFactory;
	private boolean readOnly;

	/**
	 * The constructor is protected because we should not construct this class
	 * directly but use the factory methods to create Layers based on
	 * configurations
	 * directly but use the factory methods to create Layers based on configurations
	 */
	protected DefaultLayer() {
	}

	@Override
	public void initialize(
			Transaction tx,
			IndexManager indexManager,
			String name,
			GeometryEncoder geometryEncoder,
			SpatialIndexWriter index,
			Node layerNode,
			boolean readOnly
	) {
		this.name = name;
		this.layerNodeId = layerNode.getElementId();
		this.readOnly = readOnly;

		this.geometryFactory = new GeometryFactory();
		this.geometryEncoder = geometryEncoder;
		CoordinateReferenceSystem crs = getCoordinateReferenceSystem(tx);
		if (crs != null) {
			// TODO: Verify this code works for general cases to read SRID from layer properties and use them to construct GeometryFactory
			Integer code = GeotoolsAdapter.getEPSGCode(crs);
			if (code != null) {
				this.geometryFactory = new GeometryFactory(new PrecisionModel(), code);
			}
		}
		this.geometryEncoder.init(this);
		this.indexReader = index;
		this.indexReader.init(tx, indexManager, this, readOnly);
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public SpatialIndexReader getIndex() {
		return indexReader;
	}

	@Override
	public String getSignature() {
		return "Layer(name='" + getName() + "', encoder=" + getGeometryEncoder().getSignature() + ")";
	}

	@Override
	public boolean isReadOnly() {
		return readOnly;
	}

	@Override
	public GeometryFactory getGeometryFactory() {
		return geometryFactory;
	}

	@Override
	public CoordinateReferenceSystem getCoordinateReferenceSystem(Transaction tx) {
		Node layerNode = getLayerNode(tx);
		if (layerNode.hasProperty(PROP_CRS)) {
			return GeotoolsAdapter.getCRS((String) layerNode.getProperty(PROP_CRS));
		}
		return null;
	}

	@Override
	public Integer getGeometryType(Transaction tx) {
		Node layerNode = getLayerNode(tx);
		if (layerNode.hasProperty(PROP_TYPE)) {
			return (Integer) layerNode.getProperty(PROP_TYPE);
		}
		GuessGeometryTypeSearch geomTypeSearch = new GuessGeometryTypeSearch();
		indexReader.searchIndex(tx, geomTypeSearch).count();

		// returns null for an empty layer!
		return geomTypeSearch.firstFoundType;
	}

	@Nonnull
	@Override
	public Map<String, Class<?>> getExtraProperties(Transaction tx) {
		Node layerNode = getLayerNode(tx);
		var extraProps = new TreeMap<String, Class<?>>();
		layerNode.getAllProperties().forEach((name, o) -> {
			if (!name.startsWith(PROP_PREFIX_EXTRA_PROP_V2)) {
				return;
			}
			Class<?> clazz = String.class;
			if (o instanceof String className) {
				try {
					clazz = Class.forName(className);
				} catch (ClassNotFoundException ignore) {
				}
			}

			var key = name.substring(PROP_PREFIX_EXTRA_PROP_V2.length());
			extraProps.put(key, clazz);
		});
		if (layerNode.hasProperty(PROP_LAYERNODEEXTRAPROPS)) {
			Object legacyProps = layerNode.getProperty(PROP_LAYERNODEEXTRAPROPS);
			if (legacyProps instanceof String[] props) {
				for (String s : props) {
					extraProps.putIfAbsent(s, String.class);
				}
			}
		}
		return extraProps;
	}

	/**
	 * All layers are associated with a single node in the database. This node will have properties,
	 * relationships (sub-graph) or both to describe the contents of the layer
	 */
	@Override
	public Node getLayerNode(Transaction tx) {
		return tx.getNodeByElementId(layerNodeId);
	}

	@Override
	public SpatialDataset getDataset() {
		return this;
	}

	@Override
	public Iterable<Node> getAllGeometryNodes(Transaction tx) {
		return indexReader.getAllIndexedNodes(tx);
	}

	@Override
	public boolean containsGeometryNode(Transaction tx, Node geomNode) {
		return indexReader.isNodeIndexed(tx, geomNode.getElementId());
	}

	/**
	 * Provides a method for iterating over all geometries in this dataset. This is similar to the
	 * getAllGeometryNodes() method but internally converts the Node to a Geometry.
	 *
	 * @return iterable over geometries in the dataset
	 */
	@Override
	public Iterable<? extends Geometry> getAllGeometries(Transaction tx) {
		return new NodeToGeometryIterable(getAllGeometryNodes(tx));
	}

	/**
	 * Return the geometry encoder used by this SpatialDataset to convert individual geometries to
	 * and from the database structure.
	 *
	 * @return GeometryEncoder for this dataset
	 */
	@Override
	public GeometryEncoder getGeometryEncoder() {
		return geometryEncoder;
	}

	/**
	 * This dataset contains only one layer, itself.
	 *
	 * @return iterable over all Layers that can be viewed from this dataset
	 */
	@Override
	public Iterable<? extends Layer> getLayers() {
		return Collections.singletonList(this);
	}

	@Override
	public PropertyMappingManager getPropertyMappingManager() {
		if (propertyMappingManager == null) {
			propertyMappingManager = new PropertyMappingManager(this);
		}
		return propertyMappingManager;
	}

	private static class GuessGeometryTypeSearch implements SearchFilter {

		Integer firstFoundType;

		@Override
		public boolean needsToVisit(Envelope indexNodeEnvelope) {
			return firstFoundType == null;
		}

		@Override
		public boolean geometryMatches(Transaction tx, Node geomNode) {
			if (firstFoundType == null) {
				firstFoundType = (Integer) geomNode.getProperty(PROP_TYPE);
			}

			return false;
		}
	}

	/**
	 * In order to wrap one iterable or iterator in another that converts the objects from one type
	 * to another without loading all into memory, we need to use this ugly java-magic. Man, I miss
	 * Ruby right now!
	 */
	private class NodeToGeometryIterable implements Iterable<Geometry> {

		private final Iterator<Node> allGeometryNodeIterator;

		NodeToGeometryIterable(Iterable<Node> allGeometryNodes) {
			this.allGeometryNodeIterator = allGeometryNodes.iterator();
		}

		@Override
		@Nonnull
		public Iterator<Geometry> iterator() {
			return new GeometryIterator();
		}

		private class GeometryIterator implements Iterator<Geometry> {

			@Override
			public boolean hasNext() {
				return NodeToGeometryIterable.this.allGeometryNodeIterator.hasNext();
			}

			@Override
			public Geometry next() {
				return geometryEncoder.decodeGeometry(NodeToGeometryIterable.this.allGeometryNodeIterator.next());
			}

			@Override
			public void remove() {
			}

		}

	}

}
