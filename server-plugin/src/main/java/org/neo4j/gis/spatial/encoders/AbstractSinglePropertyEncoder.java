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

import static org.neo4j.gis.spatial.Constants.PROP_GEOM;
import static org.neo4j.gis.spatial.Constants.PROP_TYPE;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.Set;
import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.encoders.AbstractSinglePropertyEncoder.Config;

public abstract class AbstractSinglePropertyEncoder extends AbstractGeometryEncoder<Config> {

	public static class Config extends BaseGeometryEncoderConfig {

		@JsonPropertyDescription("The name of the property containing the geometry")
		public String geomProperty = PROP_GEOM;

		public Config() {
		}

		public Config(String geomProperty) {
			if (geomProperty != null) {
				this.geomProperty = geomProperty;
			}
		}
	}


	public AbstractSinglePropertyEncoder() {
		super(new Config());
	}

	@Override
	public Class<Config> getConfigType() {
		return Config.class;
	}


	@Override
	public Config migrateLegacyConfiguration(String configuration) {
		Config config = new Config();
		if (configuration != null && !configuration.trim().isEmpty()) {
			String[] fields = configuration.split(":");
			if (fields.length > 0) {
				config.geomProperty = fields[0];
			}
			if (fields.length > 1) {
				config.bboxProperty = fields[1];
			}
		}
		return config;
	}

	@Override
	public String getSignature() {
		return "GeometryEncoder(geom='" + configuration.geomProperty + "', bbox='" + configuration.bboxProperty + "')";
	}


	@Override
	public Set<String> getEncoderProperties() {
		return Set.of(configuration.bboxProperty, configuration.geomProperty, PROP_TYPE);
	}
}
