package org.neo4j.gis.spatial.osm;

import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.neo4j.gis.spatial.DynamicLayer;
import org.neo4j.gis.spatial.NullListener;
import org.neo4j.gis.spatial.SpatialDataset;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * Instances of this class represent the primary layer of the OSM Dataset. It
 * extends the DynamicLayer class becauase the OSM dataset can have many layers.
 * Only one is primary, the layer containing all ways. Other layers are dynamic.
 * 
 * @author craig
 * @since 1.0.0
 */
public class OSMLayer extends DynamicLayer {
	private OSMDataset osmDataset;

	public SpatialDataset getDataset() {
		if (osmDataset == null) {
			osmDataset = new OSMDataset(getSpatialDatabase(), this, layerNode);
		}
		return osmDataset;
	}

	public OSMDataset getDataset(long datasetId) {
		if (osmDataset == null) {
			osmDataset = new OSMDataset(this.getSpatialDatabase(), this, layerNode, datasetId);
		}
		return osmDataset;
	}

	public Integer getGeometryType() {
		// The core layer in OSM is based on the Ways, and we return all of them
		// as LINESTRING
		return GTYPE_LINESTRING;
	}

	/**
	 * OSM always uses WGS84 CRS; so we return that.
	 */
	public CoordinateReferenceSystem getCoordinateReferenceSystem() {
		try {
			return DefaultGeographicCRS.WGS84;
		} catch (Exception e) {
			System.err.println("Failed to decode WGS84 CRS: " + e.getMessage());
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

    /**
     * Provides a method for iterating over all nodes that represent geometries in this layer.
     * This is similar to the getAllNodes() methods from GraphDatabaseService but will only return
     * nodes that this dataset considers its own, and can be passed to the GeometryEncoder to
     * generate a Geometry. There is no restricting on a node belonging to multiple datasets, or
     * multiple layers within the same dataset.
     * 
     * @return iterable over geometry nodes in the dataset
     */
    public Iterable<Node> getAllGeometryNodes() {
        return index.getAllGeometryNodes();
    }

}
