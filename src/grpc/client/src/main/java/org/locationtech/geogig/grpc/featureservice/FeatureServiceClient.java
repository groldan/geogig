/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.featureservice;

import static com.google.common.base.Preconditions.checkState;
import static org.locationtech.geogig.grpc.common.Utils.object;
import static org.locationtech.geogig.grpc.common.Utils.str;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.eclipse.jdt.annotation.Nullable;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.visitor.DefaultFilterVisitor;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.geogig.grpc.common.Utils;
import org.locationtech.geogig.grpc.model.RevObject;
import org.locationtech.geogig.grpc.stream.AttributeValue;
import org.locationtech.geogig.grpc.stream.AttributeValue.ValueCase;
import org.locationtech.geogig.grpc.stream.BoundsMessage;
import org.locationtech.geogig.grpc.stream.FeatureMessage;
import org.locationtech.geogig.grpc.stream.FeatureServiceGrpc.FeatureServiceBlockingStub;
import org.locationtech.geogig.grpc.stream.GetFeaturesQuery;
import org.locationtech.geogig.grpc.stream.GetFeaturesQuery.Builder;
import org.locationtech.geogig.model.RevFeatureType;
import org.opengis.feature.Feature;
import org.opengis.feature.FeatureFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.expression.Literal;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt64Value;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequenceFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;

public class FeatureServiceClient {

    private final FeatureServiceBlockingStub blockingStub;

    private static final FeatureFactory FF = CommonFactoryFinder.getFeatureFactory(null);

    public FeatureServiceClient(FeatureServiceBlockingStub clientStub) {
        blockingStub = clientStub;
    }

    public List<String> getTypeNames() {
        Iterator<StringValue> typeNames = blockingStub.getTypeNames(Utils.EMPTY);
        return Lists.newArrayList(Iterators.transform(typeNames, (s) -> s.getValue()));
    }

    public SimpleFeatureType getSchema(String typeName) throws NoSuchElementException {
        RevObject rpcObj = blockingStub.getSchema(str(typeName));
        org.locationtech.geogig.model.RevObject object = object(rpcObj);
        if (object == null) {
            throw new NoSuchElementException("No feature type found named " + typeName);
        }
        checkState(object instanceof RevFeatureType);
        return (SimpleFeatureType) ((RevFeatureType) object).type();
    }

    public int getCount(Query query, SimpleFeatureType fullSchema) {
        GetFeaturesQuery request = toRequest(query, fullSchema);
        UInt64Value res = blockingStub.getCount(request);
        int count = (int) res.getValue();
        return count;
    }

    public ReferencedEnvelope getBounds(Query query, SimpleFeatureType fullSchema) {
        GetFeaturesQuery request = toRequest(query, fullSchema);
        BoundsMessage bounds = blockingStub.getBounds(request);
        ReferencedEnvelope result = toBounds(bounds, fullSchema);
        return result;
    }

    public FeatureReader<SimpleFeatureType, SimpleFeature> getFeatures(Query query,
            SimpleFeatureType fullSchema) {

        SimpleFeatureType targetSchema = targetSchema(query, fullSchema);
        SimpleFeatureBuilder fbuilder = new SimpleFeatureBuilder(targetSchema, FF);

        GetFeaturesQuery request = toRequest(query, fullSchema);

        Iterator<SimpleFeature> features;

        Iterator<FeatureMessage> featureMessages = blockingStub.getFeatures(request);
        features = Iterators.transform(featureMessages, (f) -> toFeature(f, fbuilder));
        return new FeatureReaderAdapter<SimpleFeatureType, SimpleFeature>(targetSchema, features);
    }

    private SimpleFeatureType targetSchema(Query query, SimpleFeatureType fullSchema) {
        if (query.retrieveAllProperties()) {
            return fullSchema;
        }

        final SimpleFeatureType targetSchema;
        String[] properties = query.getPropertyNames();
        try {
            targetSchema = DataUtilities.createSubType(fullSchema, properties);
        } catch (SchemaException e) {
            throw Throwables.propagate(e);
        }
        return targetSchema;
    }

    @VisibleForTesting
    SimpleFeature toFeature(FeatureMessage f, SimpleFeatureBuilder builder) {
        builder.reset();
        for (AttributeValue av : f.getValuesList()) {
            Object v = parse(av);
            builder.add(v);
        }
        SimpleFeature feature = builder.buildFeature(f.getFid());
        return feature;
    }

    private static final GeometryFactory DEFAULT_GEOMETRY_FACTORY = new GeometryFactory(
            new PackedCoordinateSequenceFactory());

    private @Nullable Object parse(AttributeValue av) {
        final ValueCase valueCase = av.getValueCase();
        switch (valueCase) {
        case VALUE_NOT_SET:
            return null;
        case INTVAL:
            return Integer.valueOf(av.getIntval());
        case LONGVAL:
            return Long.valueOf(av.getLongval());
        case STRINGVAL:
            return av.getStringval();
        case WKB: {
            byte[] array = av.getWkb().toByteArray();
            Geometry geom;
            try {
                geom = new WKBReader(DEFAULT_GEOMETRY_FACTORY).read(array);
            } catch (ParseException e) {
                throw Throwables.propagate(e);
            }
            return geom;
        }
        case BOOLVAL:
            return Boolean.valueOf(av.getBoolval());
        case DOUBLEVAL:
            return new Double(av.getDoubleval());
        case FLOATVAL:
            return new Float(av.getFloatval());
        default:
            throw new IllegalStateException("Unknown value type: " + valueCase);

        }
    }

    @VisibleForTesting
    GetFeaturesQuery toRequest(Query query, SimpleFeatureType fullSchema) {
        Builder builder = GetFeaturesQuery.newBuilder();

        String geomName = fullSchema.getGeometryDescriptor().getLocalName();
        builder.setHead("HEAD");
        builder.setTree(fullSchema.getTypeName());
        builder.addAttributes(geomName);

        Filter filter = query.getFilter();
        List<Envelope> bounds = ExtractBounds.getBounds(filter);
        if (!bounds.isEmpty()) {
            Envelope envelope = bounds.get(0);
            BoundsMessage.Builder bb = BoundsMessage.newBuilder();
            bb.setMinX(envelope.getMinX());
            bb.setMinY(envelope.getMinY());
            bb.setMaxX(envelope.getMaxX());
            bb.setMaxY(envelope.getMaxY());
            BoundsMessage boundsFilter = bb.build();
            builder.setBoundsFilter(boundsFilter);
        }
        return builder.build();
    }

    @VisibleForTesting
    ReferencedEnvelope toBounds(BoundsMessage bounds, SimpleFeatureType fullSchema) {
        CoordinateReferenceSystem crs = fullSchema.getCoordinateReferenceSystem();
        ReferencedEnvelope b = new ReferencedEnvelope(crs);
        b.init(bounds.getMinX(), bounds.getMaxX(), bounds.getMinY(), bounds.getMaxY());
        return b;
    }

    /**
     * Adapts a closeable iterator of features as a {@link FeatureReader}
     */
    private static class FeatureReaderAdapter<T extends FeatureType, F extends Feature>
            implements FeatureReader<T, F> {

        private final T schema;

        final Iterator<? extends F> iterator;

        public FeatureReaderAdapter(T schema, Iterator<? extends F> iterator) {
            this.schema = schema;
            this.iterator = iterator;
        }

        @Override
        public T getFeatureType() {
            return schema;
        }

        @Override
        public F next() throws NoSuchElementException {
            try {
                return iterator.next();
            } catch (RuntimeException e) {
                close();
                throw Throwables.propagate(e);
            }
        }

        @Override
        public boolean hasNext() throws IOException {
            try {
                return iterator.hasNext();
            } catch (RuntimeException e) {
                close();
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void close() {
            // iterator.close();
        }
    }

    private static class ExtractBounds extends DefaultFilterVisitor {

        private List<Envelope> bounds = new ArrayList<>(2);

        @Override
        public List<Envelope> visit(Literal literal, @Nullable Object data) {

            Object value = literal.getValue();
            if (value instanceof Geometry) {
                Geometry geom = (Geometry) value;
                Envelope literalEnvelope = geom.getEnvelopeInternal();
                bounds.add(literalEnvelope);
            }
            return bounds;
        }

        @SuppressWarnings("unchecked")
        public static List<Envelope> getBounds(Filter filterInNativeCrs) {
            List<Envelope> result = (List<Envelope>) filterInNativeCrs.accept(new ExtractBounds(),
                    null);
            return result == null ? Collections.emptyList() : result;
        }
    }
}
