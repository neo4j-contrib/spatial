/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
    public Iterable<Node> addLayer(
            @Source GraphDatabaseService db,
            @Description( "The layer to add the node to. Will be automatically created if not existing." ) @Parameter( name = "layer" ) String layer,
            @Description( "The node property that contains the latitude. Default is 'lat'" ) @Parameter( name = "lat", optional=true ) String lat,
            @Description( "The node property that contains the longitude. Default is 'lon'" ) @Parameter( name = "lon", optional=true  ) String lon )
    {
        System.out.println("Creating new layer '"+layer+"' unless it already exists");
        SpatialDatabaseService spatialService = new SpatialDatabaseService( db );

        EditableLayer spatialLayer = (EditableLayer) spatialService.getOrCreatePointLayer( layer, lon, lat );

        ArrayList<Node> result = new ArrayList<Node>();
        result.add(spatialLayer.getLayerNode());
        return result;
    }

    @PluginTarget( GraphDatabaseService.class )
    @Description( "add a node to a layer" )
    public Iterable<Node> addPointToLayer(
            @Source GraphDatabaseService db,
            @Description( "The node to add to the layer" ) @Parameter( name = "node" ) Node node,
            @Description( "The layer to add the node to. Will be automatically created if not existing." ) @Parameter( name = "layer" ) String layer )
    {
        System.out.println("Hello Spatial");
        SpatialDatabaseService spatialService = new SpatialDatabaseService( db );

        EditableLayer spatialLayer = (EditableLayer) spatialService.getLayer( layer );
        spatialLayer.add(node);

        ArrayList<Node> result = new ArrayList<Node>();
        result.add(node);
        return result;
    }

}
