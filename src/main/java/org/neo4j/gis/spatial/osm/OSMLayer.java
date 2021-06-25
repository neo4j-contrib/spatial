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
package org.neo4j.gis.spatial.osm;

import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.json.simple.JSONObject;
import org.neo4j.gis.spatial.*;
import org.neo4j.gis.spatial.merge.MergeUtils;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.rtree.NullListener;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.File;
import java.util.HashMap;

/**
 * Instances of this class represent the primary layer of the OSM Dataset. It
 * extends the DynamicLayer class because the OSM dataset can have many layers.
 * Only one is primary, the layer containing all ways. Other layers are dynamic.
 */
public class OSMLayer extends DynamicLayer implements MergeUtils.Mergeable {
    private OSMDataset osmDataset;

    @Override
    public SpatialDataset getDataset() {
        return osmDataset;
    }

    public void setDataset(OSMDataset osmDataset) {
        this.osmDataset = osmDataset;
    }

    public Integer getGeometryType() {
        // The core layer in OSM is based on the Ways, and we return all of them
        // as LINESTRING and POLYGON, so we use the parent GEOMETRY
        return GTYPE_GEOMETRY;
    }

    /**
     * OSM always uses WGS84 CRS; so we return that.
     */
    public CoordinateReferenceSystem getCoordinateReferenceSystem(Transaction tx) {
        return DefaultGeographicCRS.WGS84;
    }

    protected void clear(Transaction tx) {
        indexWriter.clear(tx, new NullListener());
    }

    @Override
    public void delete(Transaction tx, Listener monitor) {
        Relationship datasetRel = this.getLayerNode(tx).getSingleRelationship(SpatialRelationshipTypes.LAYERS, Direction.INCOMING);
        if (datasetRel != null) {
            Node datasetNode = datasetRel.getStartNode();
            datasetRel.delete();
            datasetNode.delete();
        }
        super.delete(tx, monitor);
    }

    public Node addWay(Transaction tx, Node way, boolean verifyGeom) {
        Relationship geomRel = way.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING);
        if (geomRel != null) {
            return addGeomNode(tx, geomRel.getEndNode(), verifyGeom);
        } else {
            return null;
        }
    }

    public Node addGeomNode(Transaction tx, Node geomNode, boolean verifyGeom) {
        try {
            // This is a test of the validity of the geometry, throws exception on error
            if (verifyGeom)
                getGeometryEncoder().decodeGeometry(geomNode);
            indexWriter.add(tx, geomNode);
        } catch (Exception e) {
            System.err.printf("Failed geometry test on node '%s': %s%n", geomNode.getProperty("name", geomNode.toString()), e.getMessage());
            for (String key : geomNode.getPropertyKeys()) {
                System.err.println("\t" + key + ": " + geomNode.getProperty(key));
            }
            Relationship geomRel = geomNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING);
            if (geomRel != null) {
                Node way = geomRel.getStartNode();
                System.err.println("For way node " + way);
                for (String key : way.getPropertyKeys()) {
                    System.err.println("\t" + key + ": " + way.getProperty(key));
                }
            } else {
                System.err.printf("Geometry node %d has no connected OSM model node%n", geomNode.getId());
            }
        }
        return geomNode;
    }

    /**
     * Provides a method for iterating over all nodes that represent geometries in this layer.
     * This is similar to the getAllNodes() methods from GraphDatabaseService but will only return
     * nodes that this dataset considers its own, and can be passed to the GeometryEncoder to
     * generate a Geometry. There is no restriction on a node belonging to multiple datasets, or
     * multiple layers within the same dataset.
     *
     * @return iterable over geometry nodes in the dataset
     */
    public Iterable<Node> getAllGeometryNodes(Transaction tx) {
        return indexReader.getAllIndexedNodes(tx);
    }

    public boolean removeDynamicLayer(Transaction tx, String name) {
        return removeLayerConfig(tx, name);
    }

    /**
     * <pre>
     * { "step": {"type": "GEOM", "direction": "INCOMING"
     *     "step": {"type": "TAGS", "direction": "OUTGOING"
     *       "properties": {"highway": "residential"}
     *     }
     *   }
     * }
     * </pre>
     * <p>
     * This will work with OSM datasets, traversing from the geometry node
     * to the way node and then to the tags node to test if the way is a
     * residential street.
     */
    public DynamicLayerConfig addDynamicLayerOnWayTags(Transaction tx, String name, int type, HashMap<?, ?> tags) {
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
        System.out.println("Created dynamic layer query: " + query.toJSONString());
        return addLayerConfig(tx, name, type, query.toJSONString());
    }

    /**
     * Add a rule for a pure way based search, with a single property key/value
     * match on the way tags. All ways with the specified tag property will be
     * returned. This convenience method will automatically name the layer based
     * on the key/value passed, namely 'key-value'. If you want more control
     * over the naming, revert to the addDynamicLayerOnWayTags method.
     * The geometry is assumed to be LineString, the most common type for ways.
     *
     * @param key   key to match on way tags
     * @param value value to match on way tags
     */
    public DynamicLayerConfig addSimpleDynamicLayer(Transaction tx, String key, String value) {
        return addSimpleDynamicLayer(tx, key, value, Constants.GTYPE_LINESTRING);
    }

    /**
     * Add a rule for a pure way based search, with a single property key/value
     * match on the way tags. All ways with the specified tag property will be
     * returned. This convenience method will automatically name the layer based
     * on the key/value passed, namely 'key-value'. If you want more control
     * over the naming, revert to the addDynamicLayerOnWayTags method.
     *
     * @param key   key to match on way tags
     * @param value value to match on way tags
     * @param gtype type as defined in Constants.
     */
    public DynamicLayerConfig addSimpleDynamicLayer(Transaction tx, String key, String value, int gtype) {
        HashMap<String, String> tags = new HashMap<>();
        tags.put(key, value);
        return addDynamicLayerOnWayTags(tx, value == null ? key : key + "-" + value, gtype, tags);
    }

    /**
     * Add a rule for a pure way based search, with multiple property key/value
     * match on the way tags. All ways with the specified tag properties will be
     * returned. This convenience method will automatically name the layer based
     * on the key/value pairs passed, namely 'key-value-key-value-...'. If you
     * want more control over the naming, revert to the addDynamicLayerOnWayTags
     * method.
     *
     * @param gtype     type as defined in Constants.
     * @param tagsQuery String of ',' separated key=value tags to match
     */
    public DynamicLayerConfig addSimpleDynamicLayer(Transaction tx, int gtype, String tagsQuery) {
        HashMap<String, String> tags = new HashMap<>();
        StringBuilder name = new StringBuilder();
        for (String query : tagsQuery.split("\\s*,\\s*")) {
            String[] fields = query.split("\\s*=+\\s*");
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
        return addDynamicLayerOnWayTags(tx, name.toString(), gtype, tags);
    }

    /**
     * Add a rule for a pure way based search, with multiple property key/value
     * match on the way tags. All ways with the specified tag properties will be
     * returned. This convenience method will automatically name the layer based
     * on the key/value pairs passed, namely 'key-value-key-value-...'. If you
     * want more control over the naming, revert to the addDynamicLayerOnWayTags
     * method. The geometry type will be assumed to be LineString.
     *
     * @param tagsQuery String of ',' separated key=value tags to match
     */
    public DynamicLayerConfig addSimpleDynamicLayer(Transaction tx, String tagsQuery) {
        return addSimpleDynamicLayer(tx, GTYPE_LINESTRING, tagsQuery);
    }

    /**
     * Add a rule for a pure way based search, with a check on geometry type only.
     *
     * @param gtype type as defined in Constants.
     */
    public DynamicLayerConfig addSimpleDynamicLayer(Transaction tx, int gtype) {
        return addDynamicLayerOnWayTags(tx, SpatialDatabaseService.convertGeometryTypeToName(gtype), gtype, null);
    }

    /**
     * The OSM dataset has a number of possible stylesOverride this method to provide a style if your layer wishes to control
     * its own rendering in the GIS.
     *
     * @return Style or null
     */
    public File getStyle() {
        // TODO: Replace with a proper resource lookup, since this will be in the JAR
        return new File("dev/neo4j/neo4j-spatial/src/main/resources/sld/osm/osm.sld");
    }

    @Override
    public long mergeFrom(Transaction tx, EditableLayer other) {
        if (other instanceof OSMLayer) {
            try {
                OSMMerger merger = new OSMMerger(this);
                return merger.merge(tx, (OSMLayer) other);
            } catch (Exception e) {
                throw new SpatialDatabaseException("Failed to merge OSM layer " + other.getName() + " into " + this.getName() + ": " + e.getMessage(), e);
            }
        } else {
            throw new IllegalArgumentException("Cannot merge non-OSM layer into OSM layer: '" + other.getName() + "' is not OSM");
        }
    }
}
