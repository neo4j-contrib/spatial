/*
 * Copyright (c) 2010
 *
 * This program is free software: you can redistribute it and/or modify
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
package org.neo4j.gis.spatial;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Node;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;


/**
 * @author Davide Savazzi
 */
public abstract class AbstractSearch implements Search {
	
	// Constructor
	
	public AbstractSearch() {
		this.results = new ArrayList<SpatialDatabaseRecord>();
	}
	
	
	// Public methods

	@Override
	public void setGeometryFactory(GeometryFactory geometryFactory) {
		this.geometryFactory = geometryFactory;		
	}	
	
	@Override	
	public List<SpatialDatabaseRecord> getResults() {
		return results;
	}
	
	
	// Private methods
	
	protected void add(Node geomNode) {
		results.add(new SpatialDatabaseRecord(geomNode, geometryFactory));
	}

	protected void add(Node geomNode, Geometry geom) {
		results.add(new SpatialDatabaseRecord(geomNode, geom));
	}
		
	
	// Attributes
	
	protected GeometryFactory geometryFactory;
	private List<SpatialDatabaseRecord> results;
}