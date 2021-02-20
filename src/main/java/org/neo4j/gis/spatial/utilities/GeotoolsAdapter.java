package org.neo4j.gis.spatial.utilities;

import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import static org.geotools.referencing.crs.DefaultEngineeringCRS.GENERIC_2D;
import static org.geotools.referencing.crs.DefaultGeographicCRS.WGS84;

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
            } else if (crsText.startsWith("LOCAL_CS[\"Generic cartesian 2D\"")) {
                return GENERIC_2D;
            } else {
                System.out.println("Attempting to use geotools to lookup CRS - might fail with Java11: " + crsText);
                return ReferencingFactoryFinder.getCRSFactory(null).createFromWKT(crsText);
            }
        } catch (FactoryException e) {
            throw new SpatialDatabaseException(e);
        }
    }

    public static Integer getEPSGCode(CoordinateReferenceSystem crs) {
        try {
            // TODO: upgrade geotools to avoid Java11 failures on CRS.lookupEpsgCode
            return (crs == WGS84) ? Integer.valueOf(4326) : (crs == GENERIC_2D) ? null : CRS.lookupEpsgCode(crs, true);
        } catch (FactoryException e) {
            System.err.println("Failed to lookup CRS: " + e.getMessage());
            return null;
        }
    }
}
