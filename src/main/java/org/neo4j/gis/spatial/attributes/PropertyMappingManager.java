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
package org.neo4j.gis.spatial.attributes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class PropertyMappingManager {
	private Layer layer;
	private LinkedHashMap<String, PropertyMapper> propertyMappers;

	public PropertyMappingManager(Layer layer) {
		this.layer = layer;
	}

	public LinkedHashMap<String, PropertyMapper> getPropertyMappers() {
		if (propertyMappers == null) {
			propertyMappers = new LinkedHashMap<String, PropertyMapper>();
			for (PropertyMapper mapper : loadMappers().values()) {
				addPropertyMapper(mapper);
			}
		}
		return propertyMappers;
	}

	private Map<Node, PropertyMapper> loadMappers() {
		HashMap<Node, PropertyMapper> mappers = new HashMap<Node, PropertyMapper>();
		for (Relationship rel : layer.getLayerNode()
				.getRelationships(SpatialRelationshipTypes.PROPERTY_MAPPING, Direction.OUTGOING)) {
			Node node = rel.getEndNode();
			mappers.put(node, PropertyMapper.fromNode(node));
		}
		return mappers;
	}

	public void save() {
		ArrayList<PropertyMapper> toSave = new ArrayList<PropertyMapper>(getPropertyMappers().values());
		ArrayList<Node> toDelete = new ArrayList<Node>();
		for (Map.Entry<Node, PropertyMapper> entry : loadMappers().entrySet()) {
			if (!toSave.remove(entry.getValue())) {
				toDelete.add(entry.getKey());
			}
		}
		GraphDatabaseService db = layer.getLayerNode().getGraphDatabase();
		Transaction tx = db.beginTx();
		try {
			for (Node node : toDelete) {
				for (Relationship rel : node.getRelationships()) {
					rel.delete();
				}
				node.delete();
			}
			for (PropertyMapper mapper : toSave) {
				Node node = db.createNode();
				mapper.save(node);
				layer.getLayerNode().createRelationshipTo(node, SpatialRelationshipTypes.PROPERTY_MAPPING);
			}
			tx.success();
		} finally {
			tx.finish();
		}
	}

	private void addPropertyMapper(PropertyMapper mapper) {
		getPropertyMappers().put(mapper.to(), mapper);
		save();
	}

	public PropertyMapper removePropertyMapper(String to) {
		PropertyMapper mapper = getPropertyMappers().remove(to);
		if (mapper != null)
			save();
		return mapper;
	}

	public PropertyMapper getPropertyMapper(String to) {
		return getPropertyMappers().get(to);
	}

	public void addPropertyMapper(String from, String to, String type, String params) {
		addPropertyMapper(PropertyMapper.fromParams(from, to, type, params));
	}

}
