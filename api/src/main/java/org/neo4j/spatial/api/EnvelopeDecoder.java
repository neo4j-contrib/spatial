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
package org.neo4j.spatial.api;

import org.neo4j.graphdb.Entity;


/**
 * Interface for decoding spatial envelope information from Neo4j entities.
 * <p>
 * An envelope decoder is responsible for extracting spatial bounding box information
 * from Neo4j database entities (nodes or relationships) and converting it into
 * an {@link Envelope} object that can be used for spatial indexing and querying.
 * </p>
 * <p>
 * This interface is typically implemented by classes that understand how spatial
 * data is stored within specific Neo4j entities and can extract the minimum and
 * maximum coordinate values to form a bounding envelope.
 * </p>
 *
 * @see Envelope
 * @see Entity
 */
public interface EnvelopeDecoder {

	/**
	 * Decodes and extracts the spatial envelope (bounding box) from the given Neo4j entity.
	 * <p>
	 * This method analyzes the provided entity and extracts spatial coordinate information
	 * to construct an envelope that represents the spatial bounds of the data contained
	 * within the entity.
	 * </p>
	 *
	 * @param container the Neo4j entity (node or relationship) containing spatial data
	 *                  from which to extract envelope information
	 * @return the decoded {@link Envelope} representing the spatial bounds of the entity's data,
	 *         or null if no spatial envelope can be determined from the entity
	 * @throws IllegalArgumentException if the container entity is null or does not contain
	 *                                  the expected spatial data format
	 */
	Envelope decodeEnvelope(Entity container);

}
