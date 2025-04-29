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
package org.neo4j.gis.spatial.pipes.processing;

import java.io.IOException;
import org.geotools.kml.KML;
import org.geotools.kml.KMLConfiguration;
import org.geotools.xsd.Encoder;
import org.neo4j.gis.spatial.pipes.AbstractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;

/**
 * Encodes item geometry to Keyhole Markup Language (KML).
 */
public class KeyholeMarkupLanguage extends AbstractGeoPipe {

	public KeyholeMarkupLanguage() {
	}

	/**
	 * @param resultPropertyName property name to use for geometry output
	 */
	public KeyholeMarkupLanguage(String resultPropertyName) {
		super(resultPropertyName);
	}

	@Override
	protected GeoPipeFlow process(GeoPipeFlow flow) {
		Encoder encoder = new Encoder(new KMLConfiguration());
		encoder.setIndenting(true);
		try {
			setProperty(flow, encoder.encodeAsString(flow.getGeometry(), KML.Geometry));
		} catch (IOException e) {
			setProperty(flow, e.getMessage());
		}
		return flow;
	}

}
