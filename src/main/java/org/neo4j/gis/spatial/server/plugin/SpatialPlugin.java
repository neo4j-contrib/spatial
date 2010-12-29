package org.neo4j.gis.spatial.server.plugin;

import java.util.ArrayList;

import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

@Description( "a set of extensions that perform operations using the neo4j-spatial component" )
public class SpatialPlugin extends ServerPlugin
{
    
    @PluginTarget( GraphDatabaseService.class )
    @Description( "add a node to a layer" )
    public Iterable<Node> addPointToLayer(
            @Source GraphDatabaseService db,
            @Description( "The node to add to the layer" ) @Parameter( name = "node" ) Node node,
            @Description( "The layer to add the node to. Will be automatically created if not existing." ) @Parameter( name = "layer" ) String layer,
            @Description( "The node property that contains the latitude. Default is 'lat'" ) @Parameter( name = "lat", optional=true ) String lat,
            @Description( "The node property that contains the longitude. Default is 'lon'" ) @Parameter( name = "lon", optional=true  ) String lon )
    {
        System.out.println("Hello Spatial");
        SpatialDatabaseService spatialService = new SpatialDatabaseService( db );

        EditableLayer spatialLayer = (EditableLayer) spatialService.getOrCreateEditableLayer( layer );

        ArrayList<Node> result = new ArrayList<Node>();
        result.add(node);
        return result;
    }

}
