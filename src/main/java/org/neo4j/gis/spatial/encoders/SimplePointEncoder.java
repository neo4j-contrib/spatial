package org.neo4j.gis.spatial.encoders;

import org.neo4j.gis.spatial.AbstractGeometryEncoder;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.PropertyContainer;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Simple encoder that stores point geometries as two x/y properties.
 * 
 * @author craig
 */
public class SimplePointEncoder extends AbstractGeometryEncoder implements Configurable {
	protected GeometryFactory geometryFactory;
	protected String xProperty = "longitude";
	protected String yProperty = "latitude";

	protected GeometryFactory getGeometryFactory() {
		if (geometryFactory == null)
			geometryFactory = new GeometryFactory();
		return geometryFactory;
	}

	@Override
	protected void encodeGeometryShape(Geometry geometry, PropertyContainer container) {
		container.setProperty("gtype", SpatialDatabaseService.convertJtsClassToGeometryType(geometry.getClass()));
		Coordinate[] coords = geometry.getCoordinates();
		container.setProperty(xProperty, coords[0].x);
		container.setProperty(yProperty, coords[0].y);
	}

	public Geometry decodeGeometry(PropertyContainer container) {
		double x = (Double) container.getProperty(xProperty);
		double y = (Double) container.getProperty(yProperty);
		Coordinate coordinate = new Coordinate(x, y);
		return getGeometryFactory().createPoint(coordinate);
	}

	public String getConfiguration() {
		return xProperty + ":" + yProperty;
	}

	public void setConfiguration(String configuration) {
		if (configuration != null) {
			String[] fields = configuration.split(":");
			if (fields.length > 0)
				xProperty = fields[0];
			if (fields.length > 1)
				yProperty = fields[1];
		}
	}
}