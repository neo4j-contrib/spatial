package org.neo4j.gis.spatial.encoders;

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
}
