package org.neo4j.gis.spatial;

import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Geometry;

public class EditableLayerImpl extends DefaultLayer implements EditableLayer {
    private Node previousGeomNode;

    /**
     *  Add a geometry to this layer.
     */ 
    public SpatialDatabaseRecord add(Geometry geometry) {
        return add(geometry, null, null);
    }
    
    /**
     *  Add a geometry to this layer, including properties.
     */
    public SpatialDatabaseRecord add(Geometry geometry, String[] fieldsName, Object[] fields) {
        Node geomNode = addGeomNode(geometry, fieldsName, fields);
        index.add(geomNode);
        return new SpatialDatabaseRecord(getName(), getGeometryEncoder(), getCoordinateReferenceSystem(), getExtraPropertyNames(), geomNode, geometry);
    }   
    
    public void update(long geomNodeId, Geometry geometry) {
        index.remove(geomNodeId, false);
        
        Node geomNode = getDatabase().getNodeById(geomNodeId);
        getGeometryEncoder().encodeGeometry(geometry, geomNode);
        index.add(geomNode);
    }
    
    public void delete(long geomNodeId) {
        index.remove(geomNodeId, true);
    }

    private Node addGeomNode(Geometry geom, String[] fieldsName, Object[] fields) {
        Node geomNode = getDatabase().createNode();
        if(previousGeomNode!=null) {
            previousGeomNode.createRelationshipTo(geomNode, SpatialRelationshipTypes.NEXT_GEOM);
        }else{
            layerNode.createRelationshipTo(geomNode, SpatialRelationshipTypes.GEOMETRIES);
        }
        previousGeomNode = geomNode;
        getGeometryEncoder().encodeGeometry(geom, geomNode);
        
        // other properties
        if (fieldsName != null) {
            for (int i = 0; i < fieldsName.length; i++) {
                geomNode.setProperty(fieldsName[i], fields[i]);
            }
        }
        
        return geomNode;
    }
    
}
