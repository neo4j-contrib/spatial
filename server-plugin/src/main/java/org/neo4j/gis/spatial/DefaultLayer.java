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
import static org.neo4j.gis.spatial.Constants.PROP_GEOMENCODER;
import static org.neo4j.gis.spatial.Constants.PROP_GEOMENCODER_CONFIG;
import static org.neo4j.gis.spatial.Constants.PROP_INDEX_CLASS;
import static org.neo4j.gis.spatial.Constants.PROP_INDEX_CONFIG;
import static org.neo4j.gis.spatial.Constants.PROP_LAYERNODEEXTRAPROPS;
import static org.neo4j.gis.spatial.Constants.PROP_TYPE;

import java.util.Collections;
import java.util.Iterator;
import javax.annotation.Nonnull;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.neo4j.gis.spatial.attributes.PropertyMappingManager;
import org.neo4j.gis.spatial.encoders.Configurable;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.gis.spatial.index.LayerIndexReader;
import org.neo4j.gis.spatial.index.LayerRTreeIndex;
import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.utilities.GeotoolsAdapter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * Instances of Layer provide the ability for developers to query geometries associated with a single dataset (or
 * layer).
 * A Layer can be associated with a dataset. In cases where the dataset contains only one layer, the layer
 * itself is the dataset.
 * <p>
 * You should not construct the DefaultLayer directly but use the factory methods
 * on the SpatialDatabaseService for correct initialization.
 */
public class DefaultLayer implements Layer, SpatialDataset {

	protected String layerNodeId = null;

	// Public methods
	protected LayerIndexReader indexReader;
	private PropertyMappingManager propertyMappingManager;
	//private SpatialDatabaseService spatialDatabase;
	private String name;
	private GeometryEncoder geometryEncoder;
	private GeometryFactory geometryFactory;
	private boolean readOnly;

	/**
	 * The constructor is protected because we should not construct this class
	 * directly but use the factory methods to create Layers based on configurations
	 */
	protected DefaultLayer() {
	}

	@Override
	public void initialize(Transaction tx, IndexManager indexManager, String name, Node layerNode, boolean readOnly) {
		//this.spatialDatabase = spatialDatabase;
		this.name = name;
		this.layerNodeId = layerNode.getElementId();
		this.readOnly = readOnly;

		this.geometryFactory = new GeometryFactory();
		CoordinateReferenceSystem crs = getCoordinateReferenceSystem(tx);
		if (crs != null) {
			// TODO: Verify this code works for general cases to read SRID from layer properties and use them to construct GeometryFactory
			Integer code = GeotoolsAdapter.getEPSGCode(crs);
			if (code != null) {
				this.geometryFactory = new GeometryFactory(new PrecisionModel(), code);
			}
		}

		if (layerNode.hasProperty(PROP_GEOMENCODER)) {
			String encoderClassName = (String) layerNode.getProperty(PROP_GEOMENCODER);
			try {
				this.geometryEncoder = (GeometryEncoder) Class.forName(encoderClassName).getDeclaredConstructor()
						.newInstance();
			} catch (Exception e) {
				throw new SpatialDatabaseException(e);
			}
			if (this.geometryEncoder instanceof Configurable) {
				if (layerNode.hasProperty(PROP_GEOMENCODER_CONFIG)) {
					((Configurable) this.geometryEncoder).setConfiguration(
							(String) layerNode.getProperty(PROP_GEOMENCODER_CONFIG));
				}
			}
		} else {
			this.geometryEncoder = new WKBGeometryEncoder();
		}
		this.geometryEncoder.init(this);

		// index must be created *after* geometryEncoder
		if (layerNode.hasProperty(PROP_INDEX_CLASS)) {
			String indexClass = (String) layerNode.getProperty(PROP_INDEX_CLASS);
			try {
				Object index = Class.forName(indexClass).getDeclaredConstructor().newInstance();
				this.indexReader = (LayerIndexReader) index;
			} catch (Exception e) {
				throw new SpatialDatabaseException(e);
			}
			if (this.indexReader instanceof Configurable) {
				if (layerNode.hasProperty(PROP_INDEX_CONFIG)) {
					((Configurable) this.indexReader).setConfiguration(
							(String) layerNode.getProperty(PROP_INDEX_CONFIG));
				}
			}
		} else {
			this.indexReader = new LayerRTreeIndex();
		}
		this.indexReader.init(tx, indexManager, this, readOnly);
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public LayerIndexReader getIndex() {
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

	@Override
	public String[] getExtraPropertyNames(Transaction tx) {
		Node layerNode = getLayerNode(tx);
		String[] extraPropertyNames;
		if (layerNode.hasProperty(PROP_LAYERNODEEXTRAPROPS)) {
			extraPropertyNames = (String[]) layerNode.getProperty(PROP_LAYERNODEEXTRAPROPS);
		} else {
			extraPropertyNames = new String[]{};
		}
		return extraPropertyNames;
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

	/**
	 * Override this method to provide a style if your layer wishes to control
	 * its own rendering in the GIS. If a Style is returned, it is used. If a
	 * File is returned, it is opened and assumed to contain SLD contents. If a
	 * String is returned, it is assumed to contain SLD contents.
	 *
	 * @return null
	 */
	@Override
	public Object getStyle() {
		return null;
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
