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
package org.neo4j.gis.spatial;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.geotools.geometry.jts.ReferencedEnvelope;
import org.neo4j.collections.rtree.Envelope;
import org.neo4j.gis.spatial.geotools.data.StyledImageExporter;
import org.neo4j.gis.spatial.osm.OSMRelation;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Coordinate;


public class DavideOsmAnalysisTest extends Neo4jTestCase implements Constants {

    /*static enum RelTypes implements RelationshipType {
    	OSMANALYSIS_USERINDEX
    } */	
	
	private String layerPrefix = "berlin_osm_topten_1315917644187";
	
    public void testAnalys() throws Exception {
    	String dbPath = "/Users/davidesavazzi/Desktop/amanzi/neo4j-dbs/berlin_osm";		
		EmbeddedGraphDatabase graphDb = new EmbeddedGraphDatabase(dbPath);
		try {
			Node osmRoot = graphDb.getReferenceNode().getSingleRelationship(OSMRelation.OSM, Direction.OUTGOING).getEndNode();
			Node osmImport = osmRoot.getSingleRelationship(OSMRelation.OSM, Direction.OUTGOING).getEndNode();			
			Node usersNode = osmImport.getSingleRelationship(OSMRelation.USERS, Direction.OUTGOING).getEndNode();
								
			Map<String,User> userIndex = collectUserChangesetData(usersNode);
			SortedSet<User> topTen = getTopTen(userIndex);					
			
			SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb);
			SortedMap<String,Layer> layers = exportPoints(graphDb, spatialService, topTen);
						
			layers = removeEmptyLayers(layers);
			ReferencedEnvelope bbox = getEnvelope(layers.values());
			
			StyledImageExporter imageExporter = new StyledImageExporter(graphDb);
			String exportDir = "/Users/davidesavazzi/Desktop/amanzi/tmp";
			imageExporter.setExportDir(exportDir);
			imageExporter.setZoom(2.0);
			imageExporter.setOffset(-0.05, -0.05);
			imageExporter.setSize(1280, 800);				
			
			for (String layerName : layers.keySet()) {
				SortedMap<String,Layer> layersSubset = new TreeMap<String,Layer>(layers.headMap(layerName));
				
				String[] to_render = new String[Math.min(10, layersSubset.size() + 1)];
				to_render[0] = layerName;
				if (layersSubset.size() > 0) {
					for (int i = 1; i < to_render.length; i++) {
						String name = layersSubset.lastKey();
						layersSubset.remove(name);
						to_render[i] = name;
					}
				}
				
				System.out.println("exporting " + layerName);
				imageExporter.saveLayerImage(to_render, // (String[]) layersSubset.keySet().toArray(new String[] {}), 
						"/Users/davidesavazzi/Desktop/amanzi/awe trial/osm_germany/germany_poi_small.sld", 
						new File(layerName + ".png"), bbox);
			}
		} finally {
			graphDb.shutdown();
		}
    }
    
    private ReferencedEnvelope getEnvelope(Collection<Layer> layers) {
    	CoordinateReferenceSystem crs = null;
    	
    	Envelope envelope = new Envelope();
    	for (Layer layer : layers) {
    		envelope.expandToInclude(layer.getIndex().getBoundingBox());
    		if (crs == null) {
    			crs = layer.getCoordinateReferenceSystem();
    		}
    	}
    	
    	return new ReferencedEnvelope(EnvelopeUtils.fromNeo4jToJts(envelope), crs);
    }
    
    private SortedMap<String,Layer> removeEmptyLayers(Map<String,Layer> layers) {
    	SortedMap<String,Layer> result = new TreeMap<String,Layer>();
    	
    	for (Entry<String,Layer> entry : layers.entrySet()) {
    		if (entry.getValue().getIndex().count() > 0) {
    			result.put(entry.getKey(), entry.getValue());
    		}
    	}
    	
    	return result;
    }
    
    private SortedMap<String,Layer> exportPoints(GraphDatabaseService graphDb, SpatialDatabaseService spatialService, Set<User> users) {
    	SortedMap<String,Layer> layers = new TreeMap<String,Layer>();
    	int startYear = 2009;
    	int endYear = 2011;

    	for (int y = startYear; y <= endYear; y++) {
    		for (int w = 1; w <= 52; w++) {
    			if (y == 2011 && w == 36) {
    				break;
    			}

    			String name = layerPrefix + "-" + y + "_";
    			if (w >= 10) name += w; 
    			else name += "0" + w;
    			
    			EditableLayerImpl layer = (EditableLayerImpl) spatialService.createLayer(name, WKBGeometryEncoder.class, EditableLayerImpl.class);
    			layer.setExtraPropertyNames(new String[] { "user_id", "user_name", "year", "month", "dayOfMonth", "weekOfYear" });
    			
    			// EditableLayerImpl layer = (EditableLayerImpl) spatialService.getLayer(name);    			
    			
    			layers.put(name, layer);
    		}
    	}
    	
		for (User user : users) {
			Node userNode = graphDb.getNodeById(user.id);
			System.out.println("analyzing user: " + userNode.getProperty("name"));
			for (Relationship r : userNode.getRelationships(Direction.INCOMING, OSMRelation.USER)) {
				Node changeset = r.getStartNode();
				if (changeset.hasProperty("changeset")) {
					System.out.println("analyzing changeset: " + changeset.getProperty("changeset"));
					Transaction tx = graphDb.beginTx();
					try {
						for (Relationship nr : changeset.getRelationships(Direction.INCOMING, OSMRelation.CHANGESET)) {
							Node changedNode = nr.getStartNode();
							if (changedNode.hasProperty("node_osm_id") && changedNode.hasProperty("timestamp")) {
								long timestamp = (Long) changedNode.getProperty("timestamp");

								Calendar c = Calendar.getInstance();
								c.setTimeInMillis(timestamp);
								int nodeYear = c.get(Calendar.YEAR);
								int nodeWeek = c.get(Calendar.WEEK_OF_YEAR);
								
								if (layers.containsKey(layerPrefix + "-" + nodeYear + "_" + nodeWeek)) {
									EditableLayer l = (EditableLayer) layers.get(layerPrefix + "-" + nodeYear + "_" + nodeWeek);
									l.add(l.getGeometryFactory().createPoint(new Coordinate(
											(Double) changedNode.getProperty("lon"), 
											(Double) changedNode.getProperty("lat"))), 
											new String[] { "user_id", "user_name", "year", "month", "dayOfMonth", "weekOfYear" }, 
											new Object[] { user.internalId, user.name, 
												c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH), c.get(Calendar.WEEK_OF_YEAR) });
								}
							}
						}
						
						tx.success();
					} finally {
						tx.finish();
					}
				}
			} 
		}
	       	
       	return layers;
    }
    
    private SortedSet<User> getTopTen(Map<String,User> userIndex) {
    	SortedSet<User> userList = new TreeSet<User>();
		for (String name : userIndex.keySet()) {
			userList.add(userIndex.get(name));
		}
		
		SortedSet<User> topTen = new TreeSet<User>();
		
		int count = 0;
		for (User user : userList) {
			if (count < 10) {
				topTen.add(user);
				user.internalId = count++;
			} else {
				break;
			}
		}
		
		for (User user : topTen) {
			System.out.println(user.id + "# " + user.name + " = " + user.changesets.size());
		}
		
		return topTen;
    }
    
    private Map<String,User> collectUserChangesetData(Node usersNode) {
    	Map<String,User> userIndex = new HashMap<String,User>();
		for (Relationship r : usersNode.getRelationships(Direction.OUTGOING, OSMRelation.OSM_USER)) {
			Node userNode = r.getEndNode();
			String name = (String) userNode.getProperty("name");
			
			User user = new User(userNode.getId(), name);
			userIndex.put(name, user);
			
			for (Relationship ur : userNode.getRelationships(Direction.INCOMING, OSMRelation.USER)) {
				Node node = ur.getStartNode();
				if (node.hasProperty("changeset")) {
					user.changesets.add((Long) node.getId());
				}
			}
		}    	
		
		return userIndex;
    }
    
    class User implements Comparable<User> {

    	long id;
    	int internalId;
    	String name;
    	List<Long> changesets = new ArrayList<Long>();
    	
    	public User(long id, String name) {
    		this.id = id;
    		this.name = name;
    	}
    	
		@Override
		public int compareTo(User other) {
			return -1 * ((Integer) changesets.size()).compareTo((Integer) other.changesets.size());
		}
    }
}