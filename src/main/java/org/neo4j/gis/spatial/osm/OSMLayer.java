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
package org.neo4j.gis.spatial.osm;

import java.io.File;
import java.util.HashMap;

import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.json.simple.JSONObject;
import org.neo4j.collections.rtree.NullListener;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.DynamicLayer;
import org.neo4j.gis.spatial.DynamicLayerConfig;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.SpatialDataset;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * Instances of this class represent the primary layer of the OSM Dataset. It
 * extends the DynamicLayer class becauase the OSM dataset can have many layers.
 * Only one is primary, the layer containing all ways. Other layers are dynamic.
 * 
 * @author craig
 * @since 1.0.0
 */
public class OSMLayer extends DynamicLayer {
	private OSMDataset osmDataset;

	public SpatialDataset getDataset() {
		if (osmDataset == null) {
			osmDataset = new OSMDataset(getSpatialDatabase(), this, layerNode);
		}
		return osmDataset;
	}

	/**
	 * This method is used to find or construct the necessary dataset object on
	 * an existing dataset node and layer. This will create the relationships
	 * between the two if it is missing.
	 */
	public OSMDataset getDataset(long datasetId) {
		if (osmDataset == null) {
			osmDataset = new OSMDataset(this.getSpatialDatabase(), this, layerNode, datasetId);
		}
		return osmDataset;
	}

	public Integer getGeometryType() {
		// The core layer in OSM is based on the Ways, and we return all of them
		// as LINESTRING and POLYGON, so we use the parent GEOMETRY
		return GTYPE_GEOMETRY;
	}

	/**
	 * OSM always uses WGS84 CRS; so we return that.
	 */
	public CoordinateReferenceSystem getCoordinateReferenceSystem() {
		try {
			return DefaultGeographicCRS.WGS84;
		} catch (Exception e) {
			System.err.println("Failed to decode WGS84 CRS: " + e.getMessage());
			e.printStackTrace(System.err);
			return null;
		}
	}

	protected void clear() {
		index.clear(new NullListener());
	}

	public Node addWay(Node way) {
		return addWay(way,false);
	}

	public Node addWay(Node way, boolean verifyGeom) {
		Relationship geomRel = way.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING);
		if (geomRel != null) {
			Node geomNode = geomRel.getEndNode();
			try {
				// This is a test of the validity of the geometry, throws exception on error
				if (verifyGeom)
					getGeometryEncoder().decodeGeometry(geomNode);
				index.add(geomNode);
			} catch (Exception e) {
				System.err.println("Failed geometry test on node " + geomNode.getProperty("name", geomNode.toString()) + ": "
				        + e.getMessage());
				for (String key : geomNode.getPropertyKeys()) {
					System.err.println("\t" + key + ": " + geomNode.getProperty(key));
				}
				System.err.println("For way node " + way);
				for (String key : way.getPropertyKeys()) {
					System.err.println("\t" + key + ": " + way.getProperty(key));
				}
				// e.printStackTrace(System.err);
			}
			return geomNode;
		} else {
			return null;
		}
	}

	/**
     * Provides a method for iterating over all nodes that represent geometries in this layer.
     * This is similar to the getAllNodes() methods from GraphDatabaseService but will only return
     * nodes that this dataset considers its own, and can be passed to the GeometryEncoder to
     * generate a Geometry. There is no restricting on a node belonging to multiple datasets, or
     * multiple layers within the same dataset.
     * 
     * @return iterable over geometry nodes in the dataset
     */
    public Iterable<Node> getAllGeometryNodes() {
        return index.getAllIndexedNodes();
    }

    public boolean removeDynamicLayer(String name) {
    	return removeLayerConfig(name);
    }

    @SuppressWarnings("unchecked")
	/**
	 * <pre>
	 * { "step": {"type": "GEOM", "direction": "INCOMING"
	 *     "step": {"type": "TAGS", "direction": "OUTGOING"
	 *       "properties": {"highway": "residential"}
	 *     }
	 *   }
	 * }
	 * </pre>
	 * 
	 * This will work with OSM datasets, traversing from the geometry node
	 * to the way node and then to the tags node to test if the way is a
	 * residential street.
	 */
    public DynamicLayerConfig addDynamicLayerOnWayTags(String name, int type, HashMap<?,?> tags) {
		JSONObject query = new JSONObject();
		if (tags != null && !tags.isEmpty()) {
			JSONObject step2tags = new JSONObject();
			JSONObject step2way = new JSONObject();
			JSONObject properties = new JSONObject();
			for (Object key : tags.keySet()) {
				Object value = tags.get(key);
				if (value != null && (value.toString().length() < 1 || value.equals("*")))
					value = null;
				properties.put(key.toString(), value);
			}

			step2tags.put("properties", properties);
			step2tags.put("type", "TAGS");
			step2tags.put("direction", "OUTGOING");

			step2way.put("step", step2tags);
			step2way.put("type", "GEOM");
			step2way.put("direction", "INCOMING");

			query.put("step", step2way);
		}
		if (type > 0) {
			JSONObject properties = new JSONObject();
			properties.put(PROP_TYPE, type);
			query.put("properties", properties);
		}
		System.out.println("Created dynamic layer query: "+query.toJSONString());
		return addLayerConfig(name, type, query.toJSONString());
    }

	/**
	 * Add a rule for a pure way based search, with a single property key/value
	 * match on the way tags. All ways with the specified tag property will be
	 * returned. This convenience method will automatically name the layer based
	 * on the key/value passed, namely 'key-value'. If you want more control
	 * over the naming, revert to the addDynamicLayerOnWayTags method.
	 * The geometry is assumed to be LineString, the most common type for ways.
	 * 
	 * @param key
	 * @param value
	 */
	public DynamicLayerConfig addSimpleDynamicLayer(String key, String value) {
		return addSimpleDynamicLayer(key, value, Constants.GTYPE_LINESTRING);
	}

	/**
	 * Add a rule for a pure way based search, with a single property key/value
	 * match on the way tags. All ways with the specified tag property will be
	 * returned. This convenience method will automatically name the layer based
	 * on the key/value passed, namely 'key-value'. If you want more control
	 * over the naming, revert to the addDynamicLayerOnWayTags method.
	 * 
	 * @param key
	 * @param value
	 * @param geometry type as defined in Constants.
	 */
	public DynamicLayerConfig addSimpleDynamicLayer(String key, String value, int gtype) {
		HashMap<String, String> tags = new HashMap<String, String>();
		tags.put(key, value);
		return addDynamicLayerOnWayTags(value==null ? key : key + "-" + value, gtype, tags);
	}

	/**
	 * Add a rule for a pure way based search, with multiple property key/value
	 * match on the way tags. All ways with the specified tag properties will be
	 * returned. This convenience method will automatically name the layer based
	 * on the key/value pairs passed, namely 'key-value-key-value-...'. If you
	 * want more control over the naming, revert to the addDynamicLayerOnWayTags
	 * method.
	 * 
	 * @param geometry
	 *            type as defined in Constants.
	 * @param query
	 *            String of ',' separated key=value tags to match
	 */
	public DynamicLayerConfig addSimpleDynamicLayer(int gtype, String tagsQuery) {
		HashMap<String, String> tags = new HashMap<String, String>();
		StringBuffer name = new StringBuffer();
		for (String query : tagsQuery.split("\\s*\\,\\s*")) {
			String[] fields = query.split("\\s*\\=+\\s*");
			String key = fields[0];
			String value = fields.length > 1 ? fields[1] : "*";
			tags.put(key, value);
			if (name.length() > 0)
				name.append("-");
			name.append(key);
			name.append("-");
			if (!value.equals("*"))
				name.append(value);
		}
		return addDynamicLayerOnWayTags(name.toString(), gtype, tags);
	}

	/**
	 * Add a rule for a pure way based search, with multiple property key/value
	 * match on the way tags. All ways with the specified tag properties will be
	 * returned. This convenience method will automatically name the layer based
	 * on the key/value pairs passed, namely 'key-value-key-value-...'. If you
	 * want more control over the naming, revert to the addDynamicLayerOnWayTags
	 * method. The geometry type will be assumed to be LineString.
	 * 
	 * @param query
	 *            String of ',' separated key=value tags to match
	 */
	public DynamicLayerConfig addSimpleDynamicLayer(String tagsQuery) {
		return addSimpleDynamicLayer(GTYPE_LINESTRING, tagsQuery);
	}

	/**
	 * Add a rule for a pure way based search, with a check on geometry type only.
	 * 
	 * @param geometry type as defined in Constants.
	 */
	public DynamicLayerConfig addSimpleDynamicLayer(int gtype) {
		return addDynamicLayerOnWayTags(SpatialDatabaseService.convertGeometryTypeToName(gtype), gtype, null);
	}

	/**
	 * The OSM dataset has a number of possible stylesOverride this method to provide a style if your layer wishes to control
	 * its own rendering in the GIS.
	 * 
	 * @return Style or null
	 */
	public File getStyle() {
		return new File("dev/neo4j/neo4j-spatial/src/main/resources/sld/osm/osm.sld");
	}

}
