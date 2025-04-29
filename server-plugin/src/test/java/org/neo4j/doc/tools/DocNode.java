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

package org.neo4j.doc.tools;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.impl.core.AbstractNodeEntity;
import org.neo4j.kernel.impl.core.NodeEntity;

/**
 * A {@link Node} implementation that represents the labal and properties of a {@link NodeEntity}.
 */
public class DocNode extends AbstractNodeEntity {

	private final long nodeId;
	private final String elementId;
	private final Map<String, Object> propertyMap;
	private final List<Label> labels;

	public DocNode(NodeEntity node) {
		this.nodeId = node.getId();
		this.elementId = node.getElementId();
		this.propertyMap = new TreeMap<>(node.getAllProperties());
		this.labels = StreamSupport.stream(node.getLabels().spliterator(), false).collect(Collectors.toList());
	}

	@SuppressWarnings("removal")
	@Override
	@Deprecated
	public long getId() {
		return nodeId;
	}

	@Override
	public String getElementId() {
		return elementId;
	}

	@Override
	public boolean hasProperty(String key) {
		return propertyMap.containsKey(key);
	}

	@Override
	public Object getProperty(String key) {
		return propertyMap.get(key);
	}

	@Override
	public Object getProperty(String key, Object defaultValue) {
		return propertyMap.getOrDefault(key, defaultValue);
	}

	@Override
	public void setProperty(String key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object removeProperty(String key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<String> getPropertyKeys() {
		return propertyMap.keySet();
	}

	@Override
	public Map<String, Object> getProperties(String... keys) {
		var result = new HashMap<String, Object>();
		for (String key : keys) {
			result.put(key, propertyMap.get(key));
		}
		return result;
	}

	@Override
	public Map<String, Object> getAllProperties() {
		return propertyMap;
	}

	@Override
	public void delete() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ResourceIterable<Relationship> getRelationships() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasRelationship() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ResourceIterable<Relationship> getRelationships(RelationshipType... types) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ResourceIterable<Relationship> getRelationships(Direction direction, RelationshipType... types) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasRelationship(RelationshipType... types) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void singleNode(NodeCursor nodes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasRelationship(Direction direction, RelationshipType... types) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ResourceIterable<Relationship> getRelationships(Direction dir) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasRelationship(Direction dir) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<RelationshipType> getRelationshipTypes() {
		return null;
	}

	@Override
	public int getDegree() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getDegree(RelationshipType type) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getDegree(Direction direction) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getDegree(RelationshipType type, Direction direction) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addLabel(Label label) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void removeLabel(Label label) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasLabel(Label label) {
		return labels.contains(label);
	}

	@Override
	public Iterable<Label> getLabels() {
		return labels;
	}

	@Override
	public boolean equals(Object o) {
		return o instanceof Node && this.getElementId().equals(((Node) o).getElementId());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getElementId());
	}

	@Override
	protected TokenRead tokenRead() {
		throw new UnsupportedOperationException();
	}
}
