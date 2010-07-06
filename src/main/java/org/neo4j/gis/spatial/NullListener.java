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


/**
 * @author Davide Savazzi
 */
public class NullListener implements Listener {

	// Constructor
	
	public NullListener() {
		this(1000);
	}
	
	public NullListener(int commitInterval) {
		if (commitInterval < 1) {
			throw new IllegalArgumentException("commitInterval must be > 0");
		}
		this.commitInterval = commitInterval;
	}
	
	
	// Public methods
	
	public void begin(int unitsOfWork) {
	}

	public void worked(int workedSinceLastNotification) {
	}	
	
	public void done() {
	}

	public int suggestedCommitInterval() {
		return commitInterval;
	}


	// Attributes
	
	private int commitInterval;
}
