/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.gis.spatial.utilities;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.IteratorUtil;
import static org.neo4j.helpers.collection.MapUtil.map;

public class ReferenceNodes {

    private static GraphDatabaseService dbRef;

    public static Node getReferenceNode(GraphDatabaseService db) {
        return getReferenceNode(db, "rtree");
    }

    public static Node getReferenceNode(GraphDatabaseService db, String name) {

        if (db != dbRef) {
            ReferenceNodes.dbRef = db;
        }

	Result result = db.execute("MERGE (ref:ReferenceNode {name:{name}}) RETURN ref", map("name", name));
        return IteratorUtil.single(result.<Node>columnAs("ref"));
    }
}
