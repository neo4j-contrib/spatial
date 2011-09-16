<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor xmlns:xlink="http://www.w3.org/1999/xlink" xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd" xmlns:ogc="http://www.opengis.net/ogc" version="1.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.opengis.net/sld">
    <NamedLayer>
        <Name>Example Neo4j Spatial OSM Style</Name>
        <UserStyle>
            <Name>Example Neo4j Spatial OSM Style</Name>
            <!-- Style geometries where the user_rank is 1 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>1</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#FFFF00</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#FFFF00</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#FFFF00</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>1</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#FFFF00</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>1</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#FFFF00</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#FFFF00</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 2 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>2</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#7FFF00</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#7FFF00</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#7FFF00</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>2</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#7FFF00</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>2</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#7FFF00</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#7FFF00</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 3 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>3</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#00FF00</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#00FF00</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#00FF00</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>3</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#00FF00</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>3</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#00FF00</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#00FF00</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 4 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>4</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#00FF7F</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#00FF7F</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#00FF7F</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>4</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#00FF7F</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>4</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#00FF7F</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#00FF7F</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 5 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>5</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#00FFFF</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#00FFFF</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#00FFFF</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>5</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#00FFFF</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>5</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#00FFFF</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#00FFFF</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 6 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>6</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#007FFF</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#007FFF</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#007FFF</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>6</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#007FFF</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>6</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#007FFF</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#007FFF</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 7 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>7</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#0000FF</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#0000FF</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#0000FF</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>7</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#0000FF</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>7</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#0000FF</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#0000FF</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 8 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>8</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#7F00FF</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#7F00FF</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#7F00FF</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>8</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#7F00FF</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>8</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#7F00FF</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#7F00FF</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 9 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>9</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#FF00FF</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#FF00FF</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#FF00FF</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>9</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#FF00FF</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>9</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#FF00FF</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#FF00FF</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 10 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>10</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#FF007F</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#FF007F</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#FF007F</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>10</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#FF007F</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>10</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#FF007F</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#FF007F</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 11 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>11</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#FE0EF0</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#FE0EF0</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#FE0EF0</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>11</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#FE0EF0</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>11</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#FE0EF0</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#FE0EF0</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 12 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>12</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#FF07F0</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#FF07F0</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#FF07F0</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>12</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#FF07F0</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>12</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#FF07F0</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#FF07F0</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 13 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>13</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#0F00F0</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#0F00F0</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#0F00F0</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>13</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#0F00F0</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>13</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#0F00F0</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#0F00F0</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 14 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>14</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#0F70FF</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#0F70FF</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#0F70FF</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>14</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#0F70FF</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>14</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#0F70FF</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#0F70FF</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 15 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>15</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#0FFF0F</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#0FFF0F</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#0FFF0F</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>15</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#0FFF0F</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>15</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#0FFF0F</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#0FFF0F</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 16 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>16</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#070FFF</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#070FFF</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#070FFF</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>16</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#070FFF</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>16</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#070FFF</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#070FFF</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 17 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>17</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#00F00F</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#00F00F</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#00F00F</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>17</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#00F00F</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>17</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#00F00F</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#00F00F</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 18 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>18</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#07F0FF</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#07F0FF</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#07F0FF</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>18</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#07F0FF</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>18</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#07F0FF</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#07F0FF</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 19 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>19</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#F0FFF0</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#F0FFF0</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#F0FFF0</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>19</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#F0FFF0</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>19</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#F0FFF0</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#F0FFF0</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 20 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>20</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#7F00FF</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#7F00FF</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#7F00FF</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>20</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#7F00FF</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>20</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#7F00FF</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#7F00FF</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 21 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>21</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#060EEE</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#060EEE</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#060EEE</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>21</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#060EEE</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>21</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#060EEE</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#060EEE</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 22 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>22</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#00E00E</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#00E00E</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#00E00E</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>22</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#00E00E</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>22</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#00E00E</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#00E00E</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 23 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>23</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#06E0EE</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#06E0EE</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#06E0EE</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>23</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#06E0EE</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>23</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#06E0EE</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#06E0EE</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 24 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>24</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#E0EEE0</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#E0EEE0</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#E0EEE0</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>24</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#E0EEE0</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>24</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#E0EEE0</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#E0EEE0</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- Style geometries where the user_rank is 25 -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Point</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>25</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PointSymbolizer>
                        <Graphic>
                            <Mark>
                                <WellKnownName>circle</WellKnownName>
                                <Fill>
                                    <CssParameter name="fill">#4E00EE</CssParameter>
                                    <CssParameter name="fill-opacity">0.5</CssParameter>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#4E00EE</CssParameter>
                                </Fill>
                                <Stroke>
                                    <CssParameter name="stroke-width">1</CssParameter>
                                    <CssParameter name="stroke">#4E00EE</CssParameter>
                                </Stroke>
                            </Mark>
                            <Size>
                                <Literal>5</Literal>
                            </Size>
                        </Graphic>
                    </PointSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>LineString</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>25</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#4E00EE</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:And>
                            <ogc:PropertyIsEqualTo>
                                <ogc:Function name="geometryType">
                                    <ogc:PropertyName>the_geom</ogc:PropertyName>
                                </ogc:Function>
                                <ogc:Literal>Polygon</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                            <ogc:PropertyIsEqualTo>
                                <ogc:PropertyName>user_rank</ogc:PropertyName>
                                <ogc:Literal>25</ogc:Literal>
                            </ogc:PropertyIsEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#4E00EE</CssParameter>
                            <CssParameter name="fill-opacity">0.5</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#4E00EE</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
        </UserStyle>
    </NamedLayer>
</StyledLayerDescriptor>
