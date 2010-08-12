package org.neo4j.gis.spatial;

import org.neo4j.graphdb.PropertyContainer;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Simple encoder that stores geometries as an array of float values.
 * Only supports LineString geometries.
 * 
 * @TODO: Consider generalizing this code and making a float[] type
 *        geometry store available in the library
 * @TODO: Consider switching from Float to Double according to Davide Savazzi
 * @author craig
 */
public class SimplePropertyEncoder extends AbstractGeometryEncoder {
	private GeometryFactory geometryFactory;

	private GeometryFactory getGeometryFactory() {
		if(geometryFactory==null) geometryFactory = new GeometryFactory();
		return geometryFactory;
	}

	@Override
	protected void encodeGeometryShape(Geometry geometry, PropertyContainer container) {
		container.setProperty("gtype", SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass()));
		Coordinate[] coords = geometry.getCoordinates();
		float[] data = new float[coords.length * 2];
		for (int i = 0; i < coords.length; i++) {
			data[i * 2 + 0] = (float) coords[i].x;
			data[i * 2 + 1] = (float) coords[i].y;
		}
		container.setProperty("data", data);
	}

	public Geometry decodeGeometry(PropertyContainer container) {
		float[] data = (float[]) container.getProperty("data");
		Coordinate[] coordinates = new Coordinate[data.length / 2];
		for (int i = 0; i < data.length / 2; i++) {
			coordinates[i] = new Coordinate(data[2 * i + 0], data[2 * i + 1]);
		}
		return getGeometryFactory().createLineString(coordinates);
	}
}