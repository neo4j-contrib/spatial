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
package org.neo4j.gis.spatial.process;

import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.factory.StaticMethodsProcessFactory;
import org.geotools.text.Text;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.OctagonalEnvelope;

public class SpatialProcess extends StaticMethodsProcessFactory<SpatialProcess> {

	public SpatialProcess() {
		super(Text.text("Spatial"), "spatial", SpatialProcess.class);
	}

	@DescribeProcess(title = "Octagonal Envelope", description = "Get the octagonal envelope of this Geometry.")
	@DescribeResult(description = "octagonal of geom")
	static public Geometry octagonalEnvelope(@DescribeParameter(name = "geom") Geometry geom) {
		return new OctagonalEnvelope(geom).toGeometry(geom.getFactory());
	}
}
