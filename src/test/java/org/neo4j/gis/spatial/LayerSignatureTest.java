/**
 * Copyright (c) 2010-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j Spatial.
 * <p>
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial;

import org.junit.Test;

public class LayerSignatureTest extends Neo4jTestCase implements Constants {

    @Test
    public void testSimplePointLayer() {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        Layer layer = spatialService.createSimplePointLayer("test", "lng", "lat");
        assertEquals("EditableLayer(name='test', encoder=SimplePointEncoder(x='lng', y='lat', bbox='bbox'))", layer.getSignature());
    }

    @Test
    public void testDefaultSimplePointLayer() {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        Layer layer = spatialService.createSimplePointLayer("test");
        assertEquals("EditableLayer(name='test', encoder=SimplePointEncoder(x='longitude', y='latitude', bbox='bbox'))", layer.getSignature());
    }

    @Test
    public void testSimpleWKBLayer() {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        Layer layer = spatialService.createWKBLayer("test");
        assertEquals("EditableLayer(name='test', encoder=WKBGeometryEncoder(geom='geometry', bbox='bbox'))", layer.getSignature());
    }

    @Test
    public void testWKBLayer() {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        Layer layer = spatialService.getOrCreateEditableLayer("test", "wkb", "wkb");
        assertEquals("EditableLayer(name='test', encoder=WKBGeometryEncoder(geom='wkb', bbox='bbox'))", layer.getSignature());
    }

    @Test
    public void testWKTLayer() {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        Layer layer = spatialService.getOrCreateEditableLayer("test", "wkt", "wkt");
        assertEquals("EditableLayer(name='test', encoder=WKTGeometryEncoder(geom='wkt', bbox='bbox'))", layer.getSignature());
    }

    @Test
    public void testDynamicLayer() {
        SpatialDatabaseService spatialService = new SpatialDatabaseService(graphDb());
        Layer layer = spatialService.getOrCreateEditableLayer("test", "wkt", "wkt");
        assertEquals("EditableLayer(name='test', encoder=WKTGeometryEncoder(geom='wkt', bbox='bbox'))", layer.getSignature());
        DynamicLayer dynamic = spatialService.asDynamicLayer(layer);
        assertEquals("EditableLayer(name='test', encoder=WKTGeometryEncoder(geom='wkt', bbox='bbox'))", dynamic.getSignature());
        DynamicLayerConfig points = dynamic.addCQLDynamicLayerOnAttribute("is_a", "point", GTYPE_POINT);
        assertEquals("DynamicLayer(name='CQL:is_a-point', config={layer='CQL:is_a-point', query=\"geometryType(the_geom) = 'Point' AND is_a = 'point'\"})", points.getSignature());
    }

}
