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
package org.neo4j.gis.spatial.pipes.osm;

import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.filter.SearchRecords;
import org.neo4j.gis.spatial.pipes.AbstractGeoPipe;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.pipes.impl.FilterPipe;
import org.neo4j.gis.spatial.pipes.osm.filtering.FilterOSMAttributes;
import org.neo4j.gis.spatial.pipes.osm.processing.ExtractOSMPoints;
import org.neo4j.gis.spatial.rtree.filter.SearchAll;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Transaction;

public class OSMGeoPipeline extends GeoPipeline {

	protected OSMGeoPipeline(Layer layer) {
		super(layer);
	}

	public static OSMGeoPipeline startOsm(Transaction tx, Layer layer, final SearchRecords records) {
		OSMGeoPipeline pipeline = new OSMGeoPipeline(layer);
		return (OSMGeoPipeline) pipeline.add(createStartPipe(records));
	}

	public static OSMGeoPipeline startOsm(Transaction tx, Layer layer, SearchFilter searchFilter) {
		return startOsm(tx, layer, layer.getIndex().search(tx, searchFilter));
	}

	public static OSMGeoPipeline startOsm(Transaction tx, Layer layer) {
		return startOsm(tx, layer, new SearchAll());
	}

	public OSMGeoPipeline addOsmPipe(AbstractGeoPipe geoPipe) {
		return (OSMGeoPipeline) add(geoPipe);
	}

	public OSMGeoPipeline extractOsmPoints() {
		return addOsmPipe(new ExtractOSMPoints(layer.getGeometryFactory()));
	}

	public OSMGeoPipeline osmAttributeFilter(String key, Object value) {
		return addOsmPipe(new FilterOSMAttributes(key, value));
	}

	public OSMGeoPipeline osmAttributeFilter(String key, String value, FilterPipe.Filter comparison) {
		return addOsmPipe(new FilterOSMAttributes(key, value, comparison));
	}
}
