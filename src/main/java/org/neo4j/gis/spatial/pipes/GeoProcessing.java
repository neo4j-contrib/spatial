package org.neo4j.gis.spatial.pipes;

import org.neo4j.gis.spatial.SpatialDatabaseRecord;

public interface GeoProcessing
{
    public GeoFilteringPipeline<SpatialDatabaseRecord, SpatialDatabaseRecord> filter();
    public GeoProcessingPipeline<SpatialDatabaseRecord, SpatialDatabaseRecord> process();

}
