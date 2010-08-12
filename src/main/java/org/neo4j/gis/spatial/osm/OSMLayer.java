package org.neo4j.gis.spatial.osm;

import org.geotools.factory.FactoryRegistryException;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.geotools.resources.CRSUtilities;
import org.neo4j.gis.spatial.DynamicLayer;
import org.neo4j.gis.spatial.NullListener;
import org.neo4j.gis.spatial.SpatialDatabaseException;
import org.neo4j.gis.spatial.SpatialDataset;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * Instances of this class represent individual layers or GeoResources of the OSM Dataset.
 * 
 * @author craig
 * @since 1.0.0
 */
public class OSMLayer extends DynamicLayer {
    private OSMDataset osmDataset;

    public SpatialDataset getDataset() {
        if(osmDataset==null) {
            osmDataset = new OSMDataset(getSpatialDatabase(), this, layerNode);
        }
        return osmDataset;
    }

    public OSMDataset getDataset(long datasetId) {
        if(osmDataset==null){
            osmDataset = new OSMDataset(this.getSpatialDatabase(),this, layerNode, datasetId);
        }
        return osmDataset;
    }

    public Integer getGeometryType() {
    	// The core layer in OSM is based on the Ways, and we return all of them as LINESTRING
    	return GTYPE_LINESTRING;
    }

    /**
     * OSM always uses WGS84 CRS; so we return that.
     */
    public CoordinateReferenceSystem getCoordinateReferenceSystem() {
    	try {
	        return CRS.decode("EPSG:4326");
        } catch (Exception e) {
        	System.err.println("Failed to decode WGS84 CRS: "+e.getMessage());
	        e.printStackTrace(System.err);
	        return null;
        }
    }

    protected void clear() {
        index.clear(new NullListener());
    }

    public void addWay(Node way) {
        Relationship geomRel = way.getSingleRelationship(OSMRelation.GEOM, Direction.OUTGOING);
        if (geomRel != null) {
            Node geomNode = geomRel.getEndNode();
            add(geomNode);
        }
    }

}
