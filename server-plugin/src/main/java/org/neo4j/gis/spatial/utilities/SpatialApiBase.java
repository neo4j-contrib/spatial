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

package org.neo4j.gis.spatial.utilities;

import static org.neo4j.gis.spatial.encoders.neo4j.Neo4jCRS.findCRS;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.geotools.api.referencing.ReferenceIdentifier;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jCRS;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jGeometry;
import org.neo4j.gis.spatial.encoders.neo4j.Neo4jPoint;
import org.neo4j.gis.spatial.index.IndexManager;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;

public class SpatialApiBase {

	@Context
	public Transaction tx;

	@Context
	public GraphDatabaseAPI api;

	@Context
	public KernelTransaction ktx;

	protected SpatialDatabaseService spatial() {
		return new SpatialDatabaseService(new IndexManager(api, ktx.securityContext()));
	}

	protected org.neo4j.graphdb.spatial.Geometry toNeo4jGeometry(Layer layer, Object value) {
		if (value instanceof org.neo4j.graphdb.spatial.Geometry) {
			return (org.neo4j.graphdb.spatial.Geometry) value;
		}
		Neo4jCRS crs = findCRS("Cartesian");
		if (layer != null) {
			CoordinateReferenceSystem layerCRS = layer.getCoordinateReferenceSystem(tx);
			if (layerCRS != null) {
				ReferenceIdentifier crsRef = layer.getCoordinateReferenceSystem(tx).getName();
				crs = findCRS(crsRef.toString());
			}
		}
		if (value instanceof Point point) {
			return new Neo4jPoint(point, crs);
		}
		if (value instanceof Geometry geometry) {
			return new Neo4jGeometry(geometry.getGeometryType(), toNeo4jCoordinates(geometry.getCoordinates()), crs);
		}
		if (value instanceof String && layer != null) {
			GeometryFactory factory = layer.getGeometryFactory();
			WKTReader reader = new WKTReader(factory);
			try {
				Geometry geometry = reader.read((String) value);
				return new Neo4jGeometry(geometry.getGeometryType(), toNeo4jCoordinates(geometry.getCoordinates()),
						crs);
			} catch (ParseException e) {
				throw new IllegalArgumentException("Invalid WKT: " + e.getMessage());
			}
		}
		Map<String, Object> latLon = null;
		if (value instanceof Entity) {
			latLon = ((Entity) value).getProperties("latitude", "longitude", "lat", "lon");
		}
		if (value instanceof Map) {
			//noinspection unchecked
			latLon = (Map<String, Object>) value;
		}
		Coordinate coord = toCoordinate(latLon);
		if (coord != null) {
			return new Neo4jPoint(coord, crs);
		}
		throw new RuntimeException("Can't convert " + value + " to a geometry");
	}

	private static List<org.neo4j.graphdb.spatial.Coordinate> toNeo4jCoordinates(Coordinate[] coordinates) {
		ArrayList<org.neo4j.graphdb.spatial.Coordinate> converted = new ArrayList<>();
		for (Coordinate coordinate : coordinates) {
			converted.add(toNeo4jCoordinate(coordinate));
		}
		return converted;
	}

	private static org.neo4j.graphdb.spatial.Coordinate toNeo4jCoordinate(Coordinate coordinate) {
		if (coordinate.z == Coordinate.NULL_ORDINATE) {
			return new org.neo4j.graphdb.spatial.Coordinate(coordinate.x, coordinate.y);
		}
		return new org.neo4j.graphdb.spatial.Coordinate(coordinate.x, coordinate.y, coordinate.z);
	}

	private static Coordinate toCoordinate(org.neo4j.graphdb.spatial.Coordinate point) {
		double[] coordinate = point.getCoordinate();
		return new Coordinate(coordinate[0], coordinate[1]);
	}

	private static Coordinate toCoordinate(Map<?, ?> map) {
		if (map == null) {
			return null;
		}
		Coordinate coord = toCoordinate(map, "longitude", "latitude");
		if (coord == null) {
			return toCoordinate(map, "lon", "lat");
		}
		return coord;
	}

	private static Coordinate toCoordinate(Map<?, ?> map, String xName, String yName) {
		if (map.containsKey(xName) && map.containsKey(yName)) {
			return new Coordinate(((Number) map.get(xName)).doubleValue(), ((Number) map.get(yName)).doubleValue());
		}
		return null;
	}


	protected static Coordinate toCoordinate(Object value) {
		if (value instanceof Coordinate) {
			return (Coordinate) value;
		}
		if (value instanceof org.neo4j.graphdb.spatial.Coordinate) {
			return toCoordinate((org.neo4j.graphdb.spatial.Coordinate) value);
		}
		if (value instanceof org.neo4j.graphdb.spatial.Point) {
			return toCoordinate(((org.neo4j.graphdb.spatial.Point) value).getCoordinate());
		}
		if (value instanceof Entity) {
			return toCoordinate(((Entity) value).getProperties("latitude", "longitude", "lat", "lon"));
		}
		if (value instanceof Map<?, ?>) {
			return toCoordinate((Map<?, ?>) value);
		}
		throw new RuntimeException("Can't convert " + value + " to a coordinate");
	}

	private static double[][] toCoordinateArrayFromCoordinates(List<org.neo4j.graphdb.spatial.Coordinate> coords) {
		List<double[]> coordinates = new ArrayList<>(coords.size());
		for (org.neo4j.graphdb.spatial.Coordinate coord : coords) {
			coordinates.add(coord.getCoordinate());
		}
		return toCoordinateArray(coordinates);
	}

	private static double[][] toCoordinateArray(List<double[]> coords) {
		double[][] coordinates = new double[coords.size()][];
		for (int i = 0; i < coordinates.length; i++) {
			coordinates[i] = coords.get(i);
		}
		return coordinates;
	}

	protected static Map<String, Object> toMap(org.neo4j.graphdb.spatial.Geometry geometry) {
		if (geometry instanceof org.neo4j.graphdb.spatial.Point point) {
			return Map.of("type", geometry.getGeometryType(), "coordinate", point.getCoordinate().getCoordinate());
		}
		return Map.of("type", geometry.getGeometryType(), "coordinates",
				toCoordinateArrayFromCoordinates(geometry.getCoordinates()));
	}

	protected Map<String, Object> toGeometryMap(Object geometry) {
		return toMap(toNeo4jGeometry(null, geometry));
	}

	protected static Layer getLayerOrThrow(Transaction tx, SpatialDatabaseService spatial, String name) {
		EditableLayer layer = (EditableLayer) spatial.getLayer(tx, name);
		if (layer != null) {
			return layer;
		}
		throw new IllegalArgumentException("No such layer '" + name + "'");
	}
}
