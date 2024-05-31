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

import static org.geotools.referencing.crs.DefaultEngineeringCRS.GENERIC_2D;
import static org.geotools.referencing.crs.DefaultGeographicCRS.WGS84;

import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.SpatialDatabaseException;

/**
 * This class provides some basic wrappers around geotools calls.
 * It came into existence as a workaround for some issues with geotools and versions of Java newer than Java 1.8.
 * Once we have ported to a newer Geotools that supports Java 11, we could either remove this class or re-purpose it.
 */
public class GeotoolsAdapter {

	public static CoordinateReferenceSystem getCRS(String crsText) {
		// TODO: upgrade geotools to get around bug with java11 support
		try {
			if (crsText.startsWith("GEOGCS[\"WGS84(DD)\"")) {
				return WGS84;
			}
			if (crsText.startsWith("LOCAL_CS[\"Generic cartesian 2D\"")) {
				return GENERIC_2D;
			}
			System.out.println("Attempting to use geotools to lookup CRS - might fail with Java11: " + crsText);
			return ReferencingFactoryFinder.getCRSFactory(null).createFromWKT(crsText);
		} catch (FactoryException e) {
			throw new SpatialDatabaseException(e);
		}
	}

	public static Integer getEPSGCode(CoordinateReferenceSystem crs) {
		try {
			return (crs == WGS84) ? Integer.valueOf(Constants.SRID_COORDINATES_2D)
					: (crs == GENERIC_2D) ? null : CRS.lookupEpsgCode(crs, true);
		} catch (FactoryException e) {
			System.err.println("Failed to lookup CRS: " + e.getMessage());
			return null;
		}
	}
}
