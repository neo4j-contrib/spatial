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
package org.neo4j.gis.spatial.pipes.processing;

import org.neo4j.gis.spatial.pipes.AbstractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.graphdb.Transaction;

/**
 * Copies item Node properties to item properties.<br>
 * Since extracting properties from database Nodes can be expensive it
 * must be done explicitly using this pipe.<br>
 * This pipe is useful if you want to use pipes that filter by property value or
 * pipes that process property values.
 */
public class CopyDatabaseRecordProperties extends AbstractGeoPipe {

	private final String[] keys;
	private final Transaction tx;

	public CopyDatabaseRecordProperties(Transaction tx) {
		this.tx = tx;
		this.keys = null;
	}

	public CopyDatabaseRecordProperties(Transaction tx, String key) {
		this.tx = tx;
		this.keys = new String[]{key};
	}

	public CopyDatabaseRecordProperties(Transaction tx, String[] keys) {
		this.tx = tx;
		this.keys = keys;
	}

	@Override
	protected GeoPipeFlow process(GeoPipeFlow flow) {
		String[] names = keys != null ? keys : flow.getRecord().getPropertyNames(tx);
		for (String name : names) {
			flow.getProperties().put(name, flow.getRecord().getProperty(tx, name));
		}

		return flow;
	}

}
