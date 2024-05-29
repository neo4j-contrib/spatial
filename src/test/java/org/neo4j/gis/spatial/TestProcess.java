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
package org.neo4j.gis.spatial;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.geotools.api.data.Parameter;
import org.geotools.api.feature.type.Name;
import org.geotools.feature.NameImpl;
import org.geotools.process.ProcessExecutor;
import org.geotools.process.Processors;
import org.geotools.process.Progress;
import org.geotools.util.KVP;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class TestProcess extends Neo4jTestCase {

	@Test
	public void testProcess() throws ParseException, InterruptedException, ExecutionException {
		WKTReader wktReader = new WKTReader(new GeometryFactory());
		Geometry geom = wktReader.read("MULTIPOINT (1 1, 5 4, 7 9, 5 5, 2 2)");

		Name name = new NameImpl("spatial", "octagonalEnvelope");
		org.geotools.process.Process process = Processors.createProcess(name);
		System.out.println("Executing process: " + name);
		for (Map.Entry<String, Parameter<?>> entry : Processors.getParameterInfo(name).entrySet()) {
			System.out.println("\t" + entry.getKey() + ":\t" + entry.getValue());
		}

		ProcessExecutor engine = Processors.newProcessExecutor(2);

		// quick map of inputs
		Map<String, Object> input = new KVP("geom", geom);
		Progress working = engine.submit(process, input);

		// you could do other stuff whle working is doing its thing
		if (working.isCancelled()) {
			return;
		}

		Map<String, Object> result = working.get(); // get is BLOCKING
		Geometry octo = (Geometry) result.get("result");

		System.out.println(octo);
	}
}
