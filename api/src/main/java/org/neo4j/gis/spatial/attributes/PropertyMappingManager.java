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
package org.neo4j.gis.spatial.attributes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class PropertyMappingManager {

	private final Layer layer;
	private LinkedHashMap<String, PropertyMapper> propertyMappers;

	public PropertyMappingManager(Layer layer) {
		this.layer = layer;
	}

	private LinkedHashMap<String, PropertyMapper> getPropertyMappers(Transaction tx) {
		if (propertyMappers == null) {
			propertyMappers = new LinkedHashMap<>();
			for (PropertyMapper mapper : loadMappers(tx).values()) {
				addPropertyMapper(tx, mapper);
			}
		}
		return propertyMappers;
	}

	private Map<Node, PropertyMapper> loadMappers(Transaction tx) {
		HashMap<Node, PropertyMapper> mappers = new HashMap<>();
		try (var relationships = layer.getLayerNode(tx)
				.getRelationships(Direction.OUTGOING, SpatialRelationshipTypes.PROPERTY_MAPPING)) {
			for (Relationship rel : relationships) {
				Node node = rel.getEndNode();
				mappers.put(node, PropertyMapper.fromNode(node));
			}
		}
		return mappers;
	}

	private void save(Transaction tx) {
		ArrayList<PropertyMapper> toSave = new ArrayList<>(getPropertyMappers(tx).values());
		ArrayList<Node> toDelete = new ArrayList<>();
		for (Map.Entry<Node, PropertyMapper> entry : loadMappers(tx).entrySet()) {
			if (!toSave.remove(entry.getValue())) {
				toDelete.add(entry.getKey());
			}
		}
		for (Node node : toDelete) {
			try (var relationships = node.getRelationships()) {
				for (Relationship rel : relationships) {
					rel.delete();
				}
			}
			node.delete();
		}
		for (PropertyMapper mapper : toSave) {
			Node node = tx.createNode();
			mapper.save(node);
			layer.getLayerNode(tx).createRelationshipTo(node, SpatialRelationshipTypes.PROPERTY_MAPPING);
		}
	}

	private void addPropertyMapper(Transaction tx, PropertyMapper mapper) {
		getPropertyMappers(tx).put(mapper.to(), mapper);
		save(tx);
	}

	public PropertyMapper getPropertyMapper(Transaction tx, String to) {
		return getPropertyMappers(tx).get(to);
	}

	public void addPropertyMapper(Transaction tx, String from, String to, String type, String params) {
		addPropertyMapper(tx, PropertyMapper.fromParams(from, to, type, params));
	}

}
