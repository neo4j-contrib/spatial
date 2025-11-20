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

package org.neo4j.spatial.geotools.common.utilities;

import static org.geotools.referencing.crs.DefaultEngineeringCRS.GENERIC_2D;
import static org.geotools.referencing.crs.DefaultGeographicCRS.WGS84;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;

/**
 * This class provides some basic wrappers around geotools calls.
 * It came into existence as a workaround for some issues with geotools and versions of Java newer than Java 1.8.
 * Once we have ported to a newer Geotools that supports Java 11, we could either remove this class or re-purpose it.
 */
public class GeotoolsAdapter {

	private static final Logger LOGGER = Logger.getLogger(GeotoolsAdapter.class.getName());
	public static int SRID_COORDINATES_2D = 4326;
	public static int SRID_COORDINATES_3D = 4979;

	public static CoordinateReferenceSystem getCRS(String crsText) {
		// TODO: upgrade geotools to get around bug with java11 support
		try {
			if (crsText.equals("WGS84(DD)") || crsText.startsWith("GEOGCS[\"WGS84(DD)\"")) {
				return WGS84;
			}
			if (crsText.startsWith("LOCAL_CS[\"Generic cartesian 2D\"")) {
				return GENERIC_2D;
			}
			LOGGER.log(Level.INFO, "Attempting to use geotools to lookup CRS - might fail with Java11: {0}", crsText);
			return ReferencingFactoryFinder.getCRSFactory(null).createFromWKT(crsText);
		} catch (FactoryException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public static Integer getEPSGCode(CoordinateReferenceSystem crs) {
		try {
			return (crs == DefaultGeographicCRS.WGS84) ? Integer.valueOf(SRID_COORDINATES_2D)
					: (crs == DefaultEngineeringCRS.GENERIC_2D) ? null : CRS.lookupEpsgCode(crs, true);
		} catch (FactoryException e) {
			LOGGER.log(Level.WARNING, "Failed to lookup CRS: {0}", e.getMessage());
			return null;
		}
	}
}
