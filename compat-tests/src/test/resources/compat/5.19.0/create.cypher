// Layer names are used by UpgradeCompatibilityTest: keep them across fixture versions.
// no NativePoints preset before 5.26
CALL spatial.addLayer('p_SimplePoint', 'SimplePoint', 'longitude:latitude');
CALL spatial.addLayer('p_Geohash', 'Geohash', 'longitude:latitude');
CALL spatial.addLayer('p_ZOrder', 'ZOrder', 'longitude:latitude');
CALL spatial.addLayer('p_Hilbert', 'Hilbert', 'longitude:latitude');
CALL spatial.addLayer('p_NativePoint', 'NativePoint', 'location');
CALL spatial.addLayer('p_NativeGeohash', 'NativeGeohash', 'location');
CALL spatial.addLayer('p_NativeZOrder', 'NativeZOrder', 'location');
CALL spatial.addLayer('p_NativeHilbert', 'NativeHilbert', 'location');
CALL spatial.addLayer('p_WKT', 'WKT', 'wkt');
CALL spatial.addLayer('p_WKB', 'WKB', 'wkb');
CALL spatial.addLayer('p_OSM', 'OSM', '');
CALL spatial.addPointLayerXY('xy', 'x', 'y');
CALL spatial.addNativePointLayerWithConfig('native_cfg', 'pos:mbr', 'rtree');
// no indexConfig parameter before 5.26
CALL spatial.addPointLayer('rtree20', 'rtree', '');
CALL spatial.addPointLayerGeohash('sfc_geohash');
CALL spatial.addPointLayerHilbert('sfc_hilbert');
// layers indexing user nodes (spatial.addNodes), the common usage
CALL spatial.addPointLayer('places');
CALL spatial.addNativePointLayer('native_places');
