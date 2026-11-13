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
import static org.neo4j.gis.spatial.Constants.PROP_GEOM;
import static org.neo4j.gis.spatial.Constants.PROP_TYPE;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.encoders.NativePointsEncoder.Config;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jCRS;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jPoint;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Transaction;

/**
 * Simple encoder that stores line strings as an array of points.
 */
public class NativePointsEncoder extends AbstractGeometryEncoder<Config> {

	public static class Config extends BaseGeometryEncoderConfig {

		@JsonPropertyDescription("The name of the property containing the geometry")
		public String geomProperty = PROP_GEOM;

		@JsonPropertyDescription(DOC_CRS)
		public String crs = Constants.WGS84_CRS_NAME;

		public Config() {
		}

		public Config(String geomProperty) {
			this(geomProperty, null);
		}

		public Config(String geomProperty, String crs) {
			if (geomProperty != null) {
				this.geomProperty = geomProperty;
			}
			if (crs != null) {
				this.crs = crs;
			}
		}
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
		return List.of("NativePointsEncoder", "org.neo4j.gis.spatial.encoders.NativePointsEncoder");
	}

	@Override
	protected void encodeGeometryShape(Transaction tx, Geometry geometry, Entity container) {
		var gtype = SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass());
		if (geometry instanceof LineString lineString) {
			Neo4jCRS crs = crs();
			var neo4jPoints = Arrays.stream(lineString.getCoordinates())
					.map(coordinate -> new Neo4jPoint(coordinate, crs))
					.toArray(org.neo4j.graphdb.spatial.Point[]::new);
			container.setProperty(PROP_TYPE, gtype);
			container.setProperty(configuration.geomProperty, neo4jPoints);
		} else {
			throw new IllegalArgumentException(
					"Can only store point-arrays as linestring: " + SpatialDatabaseService.convertGeometryTypeToName(
							gtype));
		}

	}

	public NativePointsEncoder() {
		super(new Config());
	}

	@Override
	public Geometry decodeGeometry(Entity container) {
		var points = ((org.neo4j.graphdb.spatial.Point[]) container.getProperty(configuration.geomProperty));
		var factory = getGeometryFactory();
		Neo4jCRS crs = crs();
		var coordinates = Arrays.stream(points)
				.map(point -> {
					if (point.getCRS().getCode() != crs.getCode()) {
						throw new IllegalStateException(
								"Trying to decode geometry with wrong CRS: layer configured to crs=" + crs.getCode()
										+ ", but geometry has crs=" + point.getCRS().getCode());
					}
					double[] coordinate = point.getCoordinate().getCoordinate();
					if (crs.dimensions() == 3) {
						return new Coordinate(coordinate[0], coordinate[1], coordinate[2]);
					} else {
						return new Coordinate(coordinate[0], coordinate[1]);
					}
				})
				.toArray(Coordinate[]::new);
		return factory.createLineString(coordinates);
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
			if (fields.length > 2) {
				config.crs = fields[2];
			}
		}
		return config;
	}

	@Override
	public void setConfiguration(Config configuration) {
		Neo4jCRS.findCRS(configuration.crs); // fail fast on unsupported CRS
		super.setConfiguration(configuration);
	}

	@Override
	public String getSignature() {
		return "NativePointEncoder(geometry='" + configuration.geomProperty + "', bbox='" + configuration.bboxProperty
				+ "', crs=" + crs().getCode()
				+ ")";
	}

	@Override
	public Set<String> getEncoderProperties() {
		return Set.of(configuration.bboxProperty, configuration.geomProperty, PROP_TYPE);
	}
}
