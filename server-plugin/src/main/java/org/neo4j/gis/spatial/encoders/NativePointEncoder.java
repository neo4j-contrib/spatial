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

import static org.neo4j.gis.spatial.Constants.DOC_CRS;
import static org.neo4j.gis.spatial.Constants.GTYPE_POINT;
import static org.neo4j.gis.spatial.Constants.PROP_TYPE;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.List;
import java.util.Set;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.encoders.NativePointEncoder.Config;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jCRS;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jPoint;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Transaction;

/**
 * Simple encoder that stores point geometries as one Neo4j Point property.
 */
public class NativePointEncoder extends AbstractGeometryEncoder<Config> {

	public static class Config extends BaseGeometryEncoderConfig {

		private static final String DEFAULT_GEOM = "location";

		@JsonPropertyDescription("The name of the property containing the geometry")
		public String locationProperty = DEFAULT_GEOM;

		@JsonPropertyDescription(DOC_CRS)
		public String crs = Constants.WGS84_CRS_NAME;

		public Config() {
		}

		public Config(String locationProperty) {
			this(locationProperty, null, null);
		}

		public Config(String locationProperty, String bboxProperty) {
			this(locationProperty, bboxProperty, null);
		}

		public Config(String locationProperty, String bboxProperty, String crs) {
			if (locationProperty != null) {
				this.locationProperty = locationProperty;
			}
			if (bboxProperty != null) {
				this.bboxProperty = bboxProperty;
			}
			if (crs != null) {
				this.crs = crs;
			}
		}


		public static Config migrateLegacyConfiguration(String configuration) {
			Config config = new Config();
			if (configuration != null && !configuration.trim().isEmpty()) {
				String[] fields = configuration.split(":");
				if (fields.length > 0) {
					config.locationProperty = fields[0];
				}
				if (fields.length > 1) {
					config.bboxProperty = fields[1];
				}
				if (fields.length > 2) {
					config.crs = fields[2];
				}
			}
			return config;
		}
	}

	public NativePointEncoder() {
		super(new Config());
	}

	@Override
	public Class<Config> getConfigType() {
		return Config.class;
	}

	// derived on access: setConfiguration runs from the super constructor, before field initializers
	private Neo4jCRS crs() {
		return Neo4jCRS.findCRS(configuration.crs);
	}

	@Override
	public List<String> getIdentifiers() {
		return List.of("NativePointEncoder", "org.neo4j.gis.spatial.encoders.NativePointEncoder");
	}

	@Override
	protected void encodeGeometryShape(Transaction tx, Geometry geometry, Entity container) {
		int gtype = SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass());
		if (gtype == GTYPE_POINT) {
			container.setProperty(PROP_TYPE, gtype);
			Neo4jPoint neo4jPoint = new Neo4jPoint((Point) geometry, crs());
			container.setProperty(configuration.locationProperty, neo4jPoint);
		} else {
			throw new IllegalArgumentException("Cannot store non-Point types as Native Neo4j properties: "
					+ SpatialDatabaseService.convertGeometryTypeToName(gtype));
		}

	}

	@Override
	public Geometry decodeGeometry(Entity container) {
		org.neo4j.graphdb.spatial.Point point = ((org.neo4j.graphdb.spatial.Point) container.getProperty(
				configuration.locationProperty));
		Neo4jCRS crs = crs();
		if (point.getCRS().getCode() != crs.getCode()) {
			throw new IllegalStateException("Trying to decode geometry with wrong CRS: layer configured to crs=" + crs
					+ ", but geometry has crs=" + point.getCRS().getCode());
		}
		double[] coordinate = point.getCoordinate().getCoordinate();
		if (crs.dimensions() == 3) {
			return getGeometryFactory().createPoint(new Coordinate(coordinate[0], coordinate[1], coordinate[2]));
		}
		return getGeometryFactory().createPoint(new Coordinate(coordinate[0], coordinate[1]));
	}

	@Override
	public Config migrateLegacyConfiguration(String configuration) {
		return Config.migrateLegacyConfiguration(configuration);
	}

	@Override
	public void setConfiguration(Config configuration) {
		Neo4jCRS.findCRS(configuration.crs); // fail fast on unsupported CRS
		super.setConfiguration(configuration);
	}

	@Override
	public String getSignature() {
		return "NativePointEncoder(geometry='" + configuration.locationProperty + "', bbox='"
				+ configuration.bboxProperty + "', crs=" + crs().getCode() + ")";
	}

	@Override
	public Set<String> getEncoderProperties() {
		return Set.of(configuration.bboxProperty, configuration.locationProperty, PROP_TYPE);
	}
}
