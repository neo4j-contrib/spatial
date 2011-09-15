<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor xmlns:xlink="http://www.w3.org/1999/xlink" xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd" xmlns:ogc="http://www.opengis.net/ogc" version="1.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.opengis.net/sld">
    <NamedLayer>
        <Name>Example Neo4j Spatial OSM Style</Name>
        <UserStyle>
            <Name>Example Neo4j Spatial OSM Style</Name>
            <!-- A catch-all style for all ways, drawn first so it only shows if anyther style does not apply -->
            <FeatureTypeStyle>
                <Rule>
                    <LineSymbolizer>
                        <Stroke>
                            <CssParameter name="stroke-width">1</CssParameter>
                            <CssParameter name="stroke">#444444</CssParameter>
                        </Stroke>
                    </LineSymbolizer>
                </Rule>
            </FeatureTypeStyle>
            <!-- A catch-all style for all Polygons -->
            <FeatureTypeStyle>
                <Rule>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:Function name="geometryType">
                                <ogc:PropertyName>the_geom</ogc:PropertyName>
                            </ogc:Function>
                            <ogc:Literal>Polygon</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill-opacity">0.4</CssParameter>
                            <CssParameter name="fill">#444444</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#444444</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>
            </FeatureTypeStyle>
        </UserStyle>
    </NamedLayer>
</StyledLayerDescriptor>
