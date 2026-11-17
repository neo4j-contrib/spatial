UNWIND ['p_SimplePoint','p_Geohash','p_ZOrder','p_Hilbert','p_NativePoint','p_NativeGeohash','p_NativeZOrder','p_NativeHilbert','xy','native_cfg','sfc_geohash','sfc_hilbert'] AS layer
CALL spatial.addWKTs(layer, ['POINT (15.2 60.1)','POINT (15.5 60.4)','POINT (15.9 60.9)','POINT (16.4 61.3)','POINT (20.0 65.0)']) YIELD node
RETURN layer, count(node) AS added;
CALL spatial.addWKTs('p_WKT', ['POINT (15.2 60.1)','LINESTRING (15.0 60.0, 16.0 61.0)','POLYGON ((15.5 60.5, 16.5 60.5, 16.5 61.5, 15.5 61.5, 15.5 60.5))','POINT (20.0 65.0)']) YIELD node RETURN count(node) AS wkt;
CALL spatial.addWKTs('p_WKB', ['POINT (15.2 60.1)','LINESTRING (15.0 60.0, 16.0 61.0)','POLYGON ((15.5 60.5, 16.5 60.5, 16.5 61.5, 15.5 61.5, 15.5 60.5))','POINT (20.0 65.0)']) YIELD node RETURN count(node) AS wkb;
UNWIND range(0, 59) AS i
WITH 'POINT (' + toString(15.0 + (i % 10) * 0.1) + ' ' + toString(60.0 + (i / 10) * 0.1) + ')' AS wkt
WITH collect(wkt) AS wkts
CALL spatial.addWKTs('rtree20', wkts) YIELD node RETURN count(node) AS rtree20;
CALL spatial.importOSMToLayer('p_OSM', 'two-street.osm') YIELD count RETURN count AS osm;
