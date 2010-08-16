/* AWE - Amanzi Wireless Explorer
 * http://awe.amanzi.org
 * (C) 2008-2009, AmanziTel AB
 *
 * This library is provided under the terms of the Eclipse Public License
 * as described at http://www.eclipse.org/legal/epl-v10.html. Any use,
 * reproduction or distribution of the library constitutes recipient's
 * acceptance of this agreement.
 *
 * This library is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 */

package org.neo4j.gis.spatial.osm;

import org.neo4j.graphdb.RelationshipType;

public enum OSMRelation implements RelationshipType {
    FIRST_NODE, LAST_NODE, OTHER, NEXT, OSM, WAYS, RELATIONS, MEMBERS, MEMBER, TAGS, GEOM, BBOX, NODE;
}