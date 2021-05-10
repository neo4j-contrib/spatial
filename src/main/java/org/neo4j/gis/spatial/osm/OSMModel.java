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

import org.neo4j.graphdb.Label;

public class OSMModel {
    public static Label LABEL_DATASET = Label.label("OSMDataset");
    public static Label LABEL_BBOX = Label.label("OSMBBox");
    public static Label LABEL_CHANGESET = Label.label("OSMChangeset");
    public static Label LABEL_USER = Label.label("OSMUser");
    public static Label LABEL_TAGS = Label.label("OSMTags");
    public static Label LABEL_NODE = Label.label("OSMNode");
    public static Label LABEL_GEOM = Label.label("OSMGeometry");
    public static Label LABEL_WAY = Label.label("OSMWay");
    public static Label LABEL_WAY_NODE = Label.label("OSMWayNode");
    public static Label LABEL_RELATION = Label.label("OSMRelation");
    public static String PROP_BBOX = "bbox";
    public static String PROP_TIMESTAMP = "timestamp";
    public static String PROP_CHANGESET = "changeset";
    public static String PROP_USER_NAME = "user";
    public static String PROP_USER_ID = "uid";
    public static String PROP_NODE_ID = "node_osm_id";
    public static String PROP_NODE_LON = "lon";
    public static String PROP_NODE_LAT = "lat";
    public static String PROP_WAY_ID = "way_osm_id";
    public static String PROP_RELATION_ID = "relation_osm_id";
}
