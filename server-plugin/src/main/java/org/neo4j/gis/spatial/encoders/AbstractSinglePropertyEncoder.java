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

package org.neo4j.gis.spatial.encoders;

import java.util.Set;
import org.neo4j.gis.spatial.AbstractGeometryEncoder;

public abstract class AbstractSinglePropertyEncoder extends AbstractGeometryEncoder implements Configurable {

	protected String geomProperty = PROP_GEOM;

	@Override
	public void setConfiguration(String configuration) {
		if (configuration != null && !configuration.trim().isEmpty()) {
			String[] fields = configuration.split(":");
			if (fields.length > 0) {
				geomProperty = fields[0];
			}
			if (fields.length > 1) {
				bboxProperty = fields[1];
			}
		}
	}

	@Override
	public String getConfiguration() {
		return geomProperty + ":" + bboxProperty;
	}

	@Override
	public String getSignature() {
		return "GeometryEncoder(geom='" + geomProperty + "', bbox='" + bboxProperty + "')";
	}


	@Override
	public Set<String> getEncoderProperties() {
		return Set.of(bboxProperty, geomProperty, PROP_TYPE);
	}
}
