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

import static org.neo4j.gis.spatial.Constants.PROP_TYPE;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.List;
import java.util.Set;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder.Config;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Transaction;

/**
 * Simple encoder that stores point geometries as two x/y properties.
 */
public class SimplePointEncoder extends AbstractGeometryEncoder<Config> {

	/**
	 * @deprecated use {@link Config#DEFAULT_X}
	 */
	@Deprecated
	public static final String DEFAULT_X = Config.DEFAULT_X;

	/**
	 * @deprecated use {@link Config#DEFAULT_Y}
	 */
	@Deprecated
	public static final String DEFAULT_Y = Config.DEFAULT_Y;

	public static class Config extends BaseGeometryEncoderConfig {

		public static final String DEFAULT_X = "longitude";
		public static final String DEFAULT_Y = "latitude";

		@JsonPropertyDescription("The name of the property containing the x-coordinate")
		public String xProperty = DEFAULT_X;

		@JsonPropertyDescription("The name of the property containing the y-coordinate")
		public String yProperty = DEFAULT_Y;

		public Config() {
		}

		public Config(String xProperty, String yProperty) {
			this.xProperty = xProperty == null ? DEFAULT_X : xProperty;
			this.yProperty = yProperty == null ? DEFAULT_Y : yProperty;
		}

		public Config(String xProperty, String yProperty, String bboxProperty) {
			this.xProperty = xProperty == null ? DEFAULT_X : xProperty;
			this.yProperty = yProperty == null ? DEFAULT_Y : yProperty;
			if (bboxProperty != null) {
				this.bboxProperty = bboxProperty;
			}
		}

		public static Config migrateLegacyConfiguration(String configuration) {
			Config config = new Config();
			if (configuration != null && !configuration.trim().isEmpty()) {
				String[] fields = configuration.split(":");
				if (fields.length > 0) {
					config.xProperty = fields[0];
				}
				if (fields.length > 1) {
					config.yProperty = fields[1];
				}
				if (fields.length > 2) {
					config.bboxProperty = fields[2];
				}
			}
			return config;
		}
	}

	public SimplePointEncoder() {
		super(new Config());
	}

	@Override
	public Class<Config> getConfigType() {
		return Config.class;
	}

	@Override
	public List<String> getIdentifiers() {
		return List.of("SimplePointEncoder", "org.neo4j.gis.spatial.encoders.SimplePointEncoder");
	}

	@Override
	protected void encodeGeometryShape(Transaction tx, Geometry geometry, Entity container) {
		container.setProperty(
				PROP_TYPE,
				SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass()));
		Coordinate[] coords = geometry.getCoordinates();
		container.setProperty(configuration.xProperty, coords[0].x);
		container.setProperty(configuration.yProperty, coords[0].y);
	}

	@Override
	public Geometry decodeGeometry(Entity container) {
		double x = ((Number) container.getProperty(configuration.xProperty)).doubleValue();
		double y = ((Number) container.getProperty(configuration.yProperty)).doubleValue();
		Coordinate coordinate = new Coordinate(x, y);
		return getGeometryFactory().createPoint(coordinate);
	}

	@Override
	public Config migrateLegacyConfiguration(String configuration) {
		return Config.migrateLegacyConfiguration(configuration);
	}

	@Override
	public String getSignature() {
		return "SimplePointEncoder(x='" + configuration.xProperty + "', y='" + configuration.yProperty + "', bbox='"
				+ configuration.bboxProperty + "')";
	}

	@Override
	public Set<String> getEncoderProperties() {
		return Set.of(configuration.bboxProperty, configuration.xProperty, configuration.yProperty, PROP_TYPE);
	}
}
