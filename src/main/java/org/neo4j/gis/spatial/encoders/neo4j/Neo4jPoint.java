package org.neo4j.gis.spatial.encoders.neo4j;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;

public class Neo4jPoint extends Neo4jGeometry implements org.neo4j.graphdb.spatial.Point {
    private final org.neo4j.graphdb.spatial.Coordinate coordinate;

    public Neo4jPoint(double[] coordinate, Neo4jCRS crs) {
        super("Point", new ArrayList<>(), crs);
        this.coordinate = new org.neo4j.graphdb.spatial.Coordinate(coordinate);
        this.coordinates.add(this.coordinate);
    }

    public Neo4jPoint(Coordinate coord, Neo4jCRS crs) {
        super("Point", new ArrayList<>(), crs);
        this.coordinate = (crs.dimensions() == 3) ?
                new org.neo4j.graphdb.spatial.Coordinate(coord.x, coord.y, coord.z) :
                new org.neo4j.graphdb.spatial.Coordinate(coord.x, coord.y);
        this.coordinates.add(this.coordinate);
    }

    public Neo4jPoint(Point point, Neo4jCRS crs) {
        this(point.getCoordinate(), crs);
    }
}
