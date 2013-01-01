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

import java.util.HashMap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public abstract class PropertyMapper {
	private String from;
	private String to;
	protected String type;
	private String params;

	public PropertyMapper(String from, String to, String type, String params) {
		this.from = from;
		this.to = to;
		this.type = type;
		this.params = params;
	}

	public boolean equals(Object obj) {
		if (obj instanceof PropertyMapper) {
			PropertyMapper other = (PropertyMapper) obj;
			return this.key().equals(other.key());
		} else {
			return false;
		}
	}

	public void save(Node node) {
		Transaction tx = node.getGraphDatabase().beginTx();
		try {
			node.setProperty("from", this.from);
			node.setProperty("to", this.to);
			node.setProperty("type", this.type);
			node.setProperty("params", this.params);
			tx.success();
		} finally {
			tx.finish();
		}
	}

	public abstract Object map(Object value);

	public String to() {
		return to;
	}

	public String from() {
		return from;
	}

	public String key() {
		return new StringBuffer().append(from).append("-").append(to).append("-").append(type).append("-").append(params)
				.toString();
	}

	public static PropertyMapper fromNode(Node node) {
		String from = (String) node.getProperty("from", null);
		String to = (String) node.getProperty("to", null);
		String type = (String) node.getProperty("type", null);
		String params = (String) node.getProperty("params", null);
		return fromParams(from, to, type, params);
	}

	public static PropertyMapper fromParams(String from, String to, String type, String params) {
		if (type == null) {
			return new IdentityMapper(from, to);
		} else if (type.equals("DeltaLong")) {
			return new DeltaLongMapper(from, to, type, Long.parseLong(params));
		} else if (type.equals("Days")) {
			return new DaysPropertyMapper(from, to, type, Long.parseLong(params));
		} else if (type.equals("Map")) {
			return new MapMapper(from, to, type, params);
		} else {
			return new IdentityMapper(from, to);
		}
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
		protected long reference;

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

		private long msPerDay = 24 * 3600 * 1000;

		public DaysPropertyMapper(String from, String to, String type, long referenceTimestamp) {
			super(from, to, type, referenceTimestamp);
		}

		@Override
		public Object map(Object value) {
			return ((Long) super.map(value)) / msPerDay;
		}

	}

	private static class MapMapper extends PropertyMapper {

		private HashMap<String,String> map = new HashMap<String,String>();

		public MapMapper(String from, String to, String type, String params) {
			super(from, to, type, params);
			for(String param:params.split(",")){
				String[] fields = param.split(":");
				map.put(fields[0],fields[1]);
			}
		}

		@Override
		public Object map(Object value) {
			return value == null ? null : map.get(value.toString());
		}

	}
}
