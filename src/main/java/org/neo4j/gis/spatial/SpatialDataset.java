package org.neo4j.gis.spatial;

import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Geometry;

/**
 * <p>
 * Classes that implement this interface should represent a single dataset. That is a collection of
 * data that is considered to belong together. This dataset will contain one or more Layers, each of
 * which can be loaded independently onto a map, but may be based on common data in the dataset.
 * When layers have no relationship to one another, it is more common to view them as separate
 * datasets. For this reason the Layer class implements the SpatialDataset interface.
 * </p>
 * <p>
 * All datasets are expected to conform to a single mechanism for storing spatial information, as
 * defined by the GeometryEncoder, which can be retrieved from the getGeometryEncoder() method.
 * </p>
 * 
 * @author craig
 * @since 1.0.0
 */
public interface SpatialDataset {

    /**
     * Provides a method for iterating over all nodes that represent geometries in this dataset.
     * This is similar to the getAllNodes() methods from GraphDatabaseService but will only return
     * nodes that this dataset considers its own, and can be passed to the GeometryEncoder to
     * generate a Geometry. There is no restricting on a node belonging to multiple datasets, or
     * multiple layers within the same dataset.
     * 
     * @return iterable over geometry nodes in the dataset
     */
    public Iterable<Node> getAllGeometryNodes();

    /**
     * Provides a method for iterating over all geometries in this dataset. This is similar to the
     * getAllGeometryNodes() method but internally converts the Node to a Geometry.
     * 
     * @return iterable over geometries in the dataset
     */
    public Iterable< ? extends Geometry> getAllGeometries();

    /**
     * Return the geometry encoder used by this SpatialDataset to convert individual geometries to
     * and from the database structure.
     * 
     * @return GeometryEncoder for this dataset
     */
    public GeometryEncoder getGeometryEncoder();

    /**
     * Each dataset can have one or more layers. This methods provides a way to iterate over all
     * layers.
     * 
     * @return iterable over all Layers that can be viewed from this dataset
     */
    public Iterable< ? extends Layer> getLayers();
}
