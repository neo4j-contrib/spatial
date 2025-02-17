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

import java.util.HashMap;
import org.neo4j.graphdb.Node;

public abstract class PropertyMapper {

	private final String from;
	private final String to;
	protected final String type;
	private final String params;

	public PropertyMapper(String from, String to, String type, String params) {
		this.from = from;
		this.to = to;
		this.type = type;
		this.params = params;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PropertyMapper other) {
			return this.key().equals(other.key());
		}
		return false;
	}

	protected void save(Node node) {
		node.setProperty("from", this.from);
		node.setProperty("to", this.to);
		node.setProperty("type", this.type);
		node.setProperty("params", this.params);
	}

	public abstract Object map(Object value);

	public String to() {
		return to;
	}

	public String from() {
		return from;
	}

	public String key() {
		return from + "-" + to + "-" + type + "-"
				+ params;
	}

	public static PropertyMapper fromNode(Node node) {
		String from = (String) node.getProperty("from", null);
		String to = (String) node.getProperty("to", null);
		String type = (String) node.getProperty("type", null);
		String params = (String) node.getProperty("params", null);
		return fromParams(from, to, type, params);
	}

	public static PropertyMapper fromParams(String from, String to, String type, String params) {
		return switch (type) {
			case "DeltaLong" -> new DeltaLongMapper(from, to, type, Long.parseLong(params));
			case "Days" -> new DaysPropertyMapper(from, to, type, Long.parseLong(params));
			case "Map" -> new MapMapper(from, to, type, params);
			default -> new IdentityMapper(from, to);
		};
	}

	private static class IdentityMapper extends PropertyMapper {

		public IdentityMapper(String from, String to) {
			super(from, to, "Identity", "");
		}

		@Override
		public Object map(Object value) {
			return value;
		}

	}

	private static class DeltaLongMapper extends PropertyMapper {

		protected final long reference;

		public DeltaLongMapper(String from, String to, String type, long reference) {
			super(from, to, type, Long.toString(reference));
			this.reference = reference;
		}

		@Override
		public Object map(Object value) {
			return (Long) value - reference;
		}

	}

	private static class DaysPropertyMapper extends DeltaLongMapper {

		public DaysPropertyMapper(String from, String to, String type, long referenceTimestamp) {
			super(from, to, type, referenceTimestamp);
		}

		@Override
		public Object map(Object value) {
			long msPerDay = 24 * 3600 * 1000;
			return ((Long) super.map(value)) / msPerDay;
		}

	}

	private static class MapMapper extends PropertyMapper {

		private final HashMap<String, String> map = new HashMap<>();

		public MapMapper(String from, String to, String type, String params) {
			super(from, to, type, params);
			for (String param : params.split(",")) {
				String[] fields = param.split(":");
				map.put(fields[0], fields[1]);
			}
		}

		@Override
		public Object map(Object value) {
			return value == null ? null : map.get(value.toString());
		}

	}
}
