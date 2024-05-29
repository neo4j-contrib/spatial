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
package org.geotools.data.neo4j;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import org.geotools.api.data.ResourceInfo;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.feature.FeatureTypes;
import org.geotools.geometry.jts.ReferencedEnvelope;

/**
 * ResourceInfo implementation.
 *
 * @author Davide Savazzi, Andreas Wilhelm
 */
public class DefaultResourceInfo implements ResourceInfo {

	private final String name;
	private final Set<String> keywords = new HashSet<>();
	private final CoordinateReferenceSystem crs;
	private final ReferencedEnvelope bbox;

	/**
	 * @param name The name of the resource.
	 * @param crs  The CoordinateReferenceSystem of the resource.
	 * @param bbox The bounding box of the resource.
	 */
	public DefaultResourceInfo(String name, CoordinateReferenceSystem crs, ReferencedEnvelope bbox) {
		this.name = name;
		this.crs = crs;
		this.bbox = bbox;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getTitle() {
		return name;
	}

	@Override
	public String getDescription() {
		return "";
	}

	@Override
	public Set<String> getKeywords() {
		return keywords;
	}

	@Override
	public URI getSchema() {
		return FeatureTypes.DEFAULT_NAMESPACE;
	}

	@Override
	public CoordinateReferenceSystem getCRS() {
		return crs;
	}

	@Override
	public ReferencedEnvelope getBounds() {
		return bbox;
	}

}
