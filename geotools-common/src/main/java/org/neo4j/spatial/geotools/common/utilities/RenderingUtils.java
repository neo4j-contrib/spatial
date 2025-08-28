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

package org.neo4j.spatial.geotools.common.utilities;

import java.awt.*;
import org.geotools.api.feature.type.FeatureType;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.style.FeatureTypeStyle;
import org.geotools.api.style.Fill;
import org.geotools.api.style.Graphic;
import org.geotools.api.style.LineSymbolizer;
import org.geotools.api.style.Mark;
import org.geotools.api.style.PointSymbolizer;
import org.geotools.api.style.PolygonSymbolizer;
import org.geotools.api.style.Rule;
import org.geotools.api.style.Stroke;
import org.geotools.api.style.Style;
import org.geotools.api.style.StyleFactory;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;

public class RenderingUtils {

	static final StyleFactory styleFactory = CommonFactoryFinder.getStyleFactory(null);
	static final FilterFactory filterFactory = CommonFactoryFinder.getFilterFactory(null);

	public static Style createDefaultStyle(Color strokeColor, Color fillColor) {
		return createStyleFromGeometry(null, strokeColor, fillColor);
	}

	/**
	 * Here is a programmatic alternative to using JSimpleStyleDialog to
	 * get a Style. These methods works out what sort of feature geometry
	 * we have in the shapefile and then delegates to an appropriate style
	 * creating method.
	 * <a href="https://docs.geotools.org/stable/userguide/examples/stylefunctionlab.html">stylefunctionlab</a>
	 */
	// TODO: Consider adding support for attribute based color schemes like in
	public static Style createStyleFromGeometry(FeatureType schema, Color strokeColor, Color fillColor) {
		if (schema != null) {
			Class<?> geomType = schema.getGeometryDescriptor().getType().getBinding();
			if (org.locationtech.jts.geom.Polygon.class.isAssignableFrom(geomType)
					|| MultiPolygon.class.isAssignableFrom(geomType)) {
				return createPolygonStyle(strokeColor, fillColor);
			}
			if (LineString.class.isAssignableFrom(geomType)
					|| LinearRing.class.isAssignableFrom(geomType)
					|| MultiLineString.class.isAssignableFrom(geomType)) {
				return createLineStyle(strokeColor);
			}
			if (Point.class.isAssignableFrom(geomType)
					|| MultiPoint.class.isAssignableFrom(geomType)) {
				return createPointStyle(strokeColor, fillColor);
			}
		}

		Style style = styleFactory.createStyle();
		style.featureTypeStyles().addAll(createPolygonStyle(strokeColor, fillColor).featureTypeStyles());
		style.featureTypeStyles().addAll(createLineStyle(strokeColor).featureTypeStyles());
		style.featureTypeStyles().addAll(createPointStyle(strokeColor, fillColor).featureTypeStyles());
//        System.out.println("Created Geometry Style: "+style);
		return style;
	}

	/**
	 * Create a Style to draw polygon features
	 */
	public static Style createPolygonStyle(Color strokeColor, Color fillColor) {
		return createPolygonStyle(strokeColor, fillColor, 0.5, 0.5, 1);
	}

	/**
	 * Create a Style to draw polygon features
	 */
	public static Style createPolygonStyle(Color strokeColor, Color fillColor, double stokeGamma, double fillGamma,
			int strokeWidth) {
		// create a partially opaque outline stroke
		org.geotools.api.style.Stroke stroke = styleFactory.createStroke(
				filterFactory.literal(strokeColor),
				filterFactory.literal(strokeWidth),
				filterFactory.literal(stokeGamma));

		// create a partial opaque fill
		Fill fill = styleFactory.createFill(
				filterFactory.literal(fillColor),
				filterFactory.literal(fillGamma));

		/*
		 * Setting the geometryPropertyName arg to null signals that we want to
		 * draw the default geomettry of features
		 */
		PolygonSymbolizer sym = styleFactory.createPolygonSymbolizer(stroke, fill, null);

		Rule rule = styleFactory.createRule();
		rule.symbolizers().add(sym);
		try {
			rule.setFilter(ECQL.toFilter("geometryType(the_geom)='Polygon' or geometryType(the_geom)='MultiPoligon'"));
		} catch (CQLException e) {
			// TODO
			e.printStackTrace();
		}

		FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle(rule);
		Style style = styleFactory.createStyle();
		style.featureTypeStyles().add(fts);
//        System.out.println("Created Polygon Style: " + style);

		return style;
	}

	/**
	 * Create a Style to draw line features
	 */
	private static Style createLineStyle(Color strokeColor) {
		Stroke stroke = styleFactory.createStroke(
				filterFactory.literal(strokeColor),
				filterFactory.literal(1));

		/*
		 * Setting the geometryPropertyName arg to null signals that we want to
		 * draw the default geomettry of features
		 */
		LineSymbolizer sym = styleFactory.createLineSymbolizer(stroke, null);

		Rule rule = styleFactory.createRule();
		rule.symbolizers().add(sym);
		try {
			rule.setFilter(ECQL.toFilter(
					"geometryType(the_geom)='LineString' or geometryType(the_geom)='LinearRing' or geometryType(the_geom)='MultiLineString'"));
		} catch (CQLException e) {
			// TODO
			e.printStackTrace();
		}

		FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle(rule);
		Style style = styleFactory.createStyle();
		style.featureTypeStyles().add(fts);
//        System.out.println("Created Line Style: "+style);

		return style;
	}

	/**
	 * Create a Style to draw point features as circles with blue outlines
	 * and cyan fill
	 */
	private static Style createPointStyle(Color strokeColor, Color fillColor) {
		Mark mark = styleFactory.getCircleMark();
		mark.setStroke(styleFactory.createStroke(filterFactory.literal(strokeColor), filterFactory.literal(2)));
		mark.setFill(styleFactory.createFill(filterFactory.literal(fillColor)));

		Graphic gr = styleFactory.createDefaultGraphic();
		gr.graphicalSymbols().clear();
		gr.graphicalSymbols().add(mark);
		gr.setSize(filterFactory.literal(5));

		/*
		 * Setting the geometryPropertyName arg to null signals that we want to
		 * draw the default geomettry of features
		 */
		PointSymbolizer sym = styleFactory.createPointSymbolizer(gr, null);

		Rule rule = styleFactory.createRule();
		rule.symbolizers().add(sym);
		try {
			rule.setFilter(ECQL.toFilter("geometryType(the_geom)='Point' or geometryType(the_geom)='MultiPoint'"));
		} catch (CQLException e) {
			// TODO
			e.printStackTrace();
		}

		FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle(rule);
		Style style = styleFactory.createStyle();
		style.featureTypeStyles().add(fts);
//        System.out.println("Created Point Style: " + style);

		return style;
	}

}
