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
package org.neo4j.gis.spatial.index;

import static org.neo4j.internal.helpers.collection.Iterators.emptyResourceIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.cs.CoordinateSystemAxis;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.gis.spatial.rtree.filter.AbstractSearchEnvelopeIntersection;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.coreapi.internal.CursorIterator;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.token.api.TokenConstants;

public abstract class LayerSpaceFillingCurvePointIndex extends ExplicitIndexBackedPointIndex<Long> {

	private SpaceFillingCurve curve = null;

	@Override
	protected String indexTypeName() {
		return "hilbert";
	}

	private SpaceFillingCurve getCurve(Transaction tx) {
		if (this.curve == null) {
			CoordinateReferenceSystem crs = layer.getCoordinateReferenceSystem(tx);
			if (crs == null) {
				throw new IllegalArgumentException("HilbertPointIndex cannot support layers without CRS");
			}
			if (crs.getCoordinateSystem().getDimension() != 2) {
				throw new IllegalArgumentException(
						"HilbertPointIndex cannot support CRS that is not 2D: " + crs.getName());
			}
			Envelope envelope = new Envelope(
					getMin(crs.getCoordinateSystem().getAxis(0)),
					getMax(crs.getCoordinateSystem().getAxis(0)),
					getMin(crs.getCoordinateSystem().getAxis(1)),
					getMax(crs.getCoordinateSystem().getAxis(1))
			);
			this.curve = makeCurve(envelope, 12);//new HilbertSpaceFillingCurve2D(envelope, 12);
		}
		return this.curve;
	}

	protected abstract SpaceFillingCurve makeCurve(Envelope envelope, int maxLevels);

	private static double getMin(CoordinateSystemAxis axis) {
		double min = axis.getMinimumValue();
		if (Double.isInfinite(min)) {
			return 0.0;
		}
		return min;
	}

	private static double getMax(CoordinateSystemAxis axis) {
		double max = axis.getMaximumValue();
		if (Double.isInfinite(max)) {
			return 1.0;
		}
		return max;
	}

	@Override
	protected Long getIndexValueFor(Transaction tx, Node geomNode) {
		//TODO: Make this code projection aware - currently it assumes lat/lon
		Geometry geom = layer.getGeometryEncoder().decodeGeometry(geomNode);
		Point point = geom.getCentroid();   // Other code is ensuring only point layers use this, but just in case we encode the centroid
		return getCurve(tx).derivedValueFor(new double[]{point.getX(), point.getY()});
	}

	@Override
	protected Neo4jIndexSearcher searcherFor(Transaction tx, SearchFilter filter) {
		if (filter instanceof AbstractSearchEnvelopeIntersection) {
			org.neo4j.gis.spatial.rtree.Envelope referenceEnvelope = ((AbstractSearchEnvelopeIntersection) filter).getReferenceEnvelope();
			return new RangeSearcher(
					getCurve(tx).getTilesIntersectingEnvelope(referenceEnvelope.getMin(), referenceEnvelope.getMax(),
							new StandardConfiguration()));
		}
		throw new UnsupportedOperationException(
				"Hilbert Index only supports searches based on AbstractSearchEnvelopeIntersection, not "
						+ filter.getClass().getCanonicalName());
	}

	public static class RangeSearcher implements Neo4jIndexSearcher {

		private final List<SpaceFillingCurve.LongRange> tiles;

		RangeSearcher(List<SpaceFillingCurve.LongRange> tiles) {
			this.tiles = tiles;
		}

		@Override
		public Iterator<Node> search(KernelTransaction ktx, Label label, String propertyKey) {
			int labelId = ktx.tokenRead().nodeLabel(label.name());
			int propId = ktx.tokenRead().propertyKey(propertyKey);
			ArrayList<Iterator<Node>> results = new ArrayList<>();
			for (SpaceFillingCurve.LongRange range : tiles) {
				PropertyIndexQuery indexQuery = PropertyIndexQuery.range(propId, range.min, true, range.max, true);
				results.add(nodesByLabelAndProperty(ktx, labelId, indexQuery));
			}
			return Iterators.concat(results.iterator());
		}

		private static ResourceIterator<Node> nodesByLabelAndProperty(KernelTransaction transaction, int labelId,
				PropertyIndexQuery query) {
			Read read = transaction.dataRead();

			if (query.propertyKeyId() == TokenConstants.NO_TOKEN || labelId == TokenConstants.NO_TOKEN) {
				return emptyResourceIterator();
			}
			Iterator<IndexDescriptor> iterator = transaction.schemaRead()
					.index(SchemaDescriptors.forLabel(labelId, query.propertyKeyId()));
			while (iterator.hasNext()) {
				IndexDescriptor index = iterator.next();
				// TODO: Do we need to support the new IndexType.RANGE index in Neo4j 4.4?
				// Skip special indexes, such as the full-text indexes, because they can't handle all the queries we might throw at them.
				if (index.getIndexType() != IndexType.RANGE) {
					continue;
				}
				// Ha! We found an index - let's use it to find matching nodes
				try {
					NodeValueIndexCursor cursor = transaction.cursors()
							.allocateNodeValueIndexCursor(CursorContext.NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
					IndexReadSession indexSession = read.indexReadSession(index);
					read.nodeIndexSeek(transaction.queryContext(), indexSession, cursor,
							IndexQueryConstraints.unordered(false), query);

					return new CursorIterator<>(cursor, NodeIndexCursor::nodeReference,
							c -> new NodeEntity(transaction.internalTransaction(), c.nodeReference()));
				} catch (KernelException e) {
					// weird at this point but ignore and fallback to a label scan
				}
			}

			return Iterators.emptyResourceIterator();
		}
	}
}
