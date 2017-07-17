/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.server;

import static org.locationtech.geogig.grpc.common.Utils.object;
import static org.locationtech.geogig.grpc.common.Utils.str;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.eclipse.jdt.annotation.Nullable;
import org.geotools.data.DataStore;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.renderer.ScreenMap;
import org.locationtech.geogig.geotools.data.GeoGigDataStore;
import org.locationtech.geogig.grpc.model.RevObject;
import org.locationtech.geogig.grpc.stream.AttributeValue;
import org.locationtech.geogig.grpc.stream.BoundsMessage;
import org.locationtech.geogig.grpc.stream.FeatureMessage;
import org.locationtech.geogig.grpc.stream.FeatureMessage.Builder;
import org.locationtech.geogig.grpc.stream.FeatureServiceGrpc;
import org.locationtech.geogig.grpc.stream.GetFeaturesQuery;
import org.locationtech.geogig.model.FieldType;
import org.locationtech.geogig.model.RevFeatureType;
import org.locationtech.geogig.model.impl.RevFeatureTypeBuilder;
import org.locationtech.geogig.repository.Repository;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.protobuf.ByteString;
import com.google.protobuf.UInt64Value;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.OutStream;
import com.vividsolutions.jts.io.WKBWriter;

import io.grpc.stub.StreamObserver;

public class FeatureService extends FeatureServiceGrpc.FeatureServiceImplBase {

    private final Supplier<Repository> repo;

    @VisibleForTesting
    public FeatureService(Repository repo) {
        this(Suppliers.ofInstance(repo));
    }

    public FeatureService(Supplier<Repository> repo) {
        this.repo = repo;
    }

    Repository repo() {
        return repo.get();
    }

    public @Override void getTypeNames(com.google.protobuf.Empty request,
            io.grpc.stub.StreamObserver<com.google.protobuf.StringValue> responseObserver) {

        try {
            DataStore dataStore = dataStore(null);// TODO: get head argument and pass it
            String[] typeNames = dataStore.getTypeNames();
            for (String typeName : typeNames) {
                responseObserver.onNext(str(typeName));
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    public @Override void getSchema(com.google.protobuf.StringValue request,
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.model.RevObject> responseObserver) {

        try {
            DataStore dataStore = dataStore(null);// TODO: get head argument and pass it
            String typeName = request.getValue();
            SimpleFeatureType schema = dataStore.getSchema(typeName);
            RevFeatureType revFeatureType = RevFeatureTypeBuilder.build(schema);

            RevObject messageObject = object(revFeatureType);

            responseObserver.onNext(messageObject);

            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    public @Override void getCount(GetFeaturesQuery request,
            StreamObserver<UInt64Value> responseObserver) {

        int count;
        Query query = toQuery(request);
        try {
            count = featureSource(request).getCount(query);
            responseObserver.onNext(UInt64Value.newBuilder().setValue(count).build());
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(e);
        }
    }

    public @Override void getBounds(GetFeaturesQuery request,
            StreamObserver<BoundsMessage> responseObserver) {

        ReferencedEnvelope bounds;
        Query query = toQuery(request);
        try {
            bounds = featureSource(request).getBounds(query);
            BoundsMessage result = BoundsMessage.newBuilder()//
                    .setMinX(bounds.getMinX())//
                    .setMaxX(bounds.getMaxX())//
                    .setMinY(bounds.getMinY())//
                    .setMaxY(bounds.getMaxY())//
                    // TODO: CRS?
                    .build();
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(e);
        }
    }

    public @Override void getFeatures(GetFeaturesQuery request,
            StreamObserver<FeatureMessage> responseObserver) {

        Query query = toQuery(request);
        try {
            SimpleFeatureCollection coll = featureSource(request).getFeatures(query);
            try (SimpleFeatureIterator it = coll.features()) {
                while (it.hasNext()) {
                    SimpleFeature next = it.next();
                    FeatureMessage feature = toMessage(next);
                    responseObserver.onNext(feature);
                }
            }
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(e);
        }
    }

    private DataStore dataStore(final @Nullable String head) {
        Repository repo = repo();
        GeoGigDataStore store = new GeoGigDataStore(repo, false);
        if (!Strings.isNullOrEmpty(head)) {
            store.setHead(head);
        }
        return store;
    }

    private SimpleFeatureSource featureSource(GetFeaturesQuery query) throws IOException {
        final String typeName = query.getTree();
        Preconditions.checkNotNull(typeName, "tree name not provided");
        DataStore dataStore = dataStore(query.getHead());
        SimpleFeatureSource source = dataStore.getFeatureSource(typeName);
        return source;
    }

    private static final FilterFactory2 FF = CommonFactoryFinder.getFilterFactory2();

    private Query toQuery(GetFeaturesQuery request) {
        // TODO Auto-generated method stub
        final String typeName = request.getTree();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(typeName), "tree name not provided");
        Query query = new Query(typeName);
        if (request.hasBoundsFilter()) {
            BoundsMessage boundsFilter = request.getBoundsFilter();
            double minx = boundsFilter.getMinX();
            double miny = boundsFilter.getMinY();
            double maxx = boundsFilter.getMaxX();
            double maxy = boundsFilter.getMaxY();
            String srs = boundsFilter.getCrs();
            Filter filter = FF.bbox("", minx, miny, maxx, maxy, srs);
            query.setFilter(filter);
        }

        final int attributesCount = request.getAttributesCount();
        if (attributesCount > 0) {
            String[] propNames = new String[attributesCount];
            for (int i = 0; i < attributesCount; i++) {
                propNames[i] = request.getAttributes(i);
            }
            query.setPropertyNames(propNames);
        }

        int canvasHeight = request.getCanvasHeight();
        int canvasWidth = request.getCanvasWidth();
        if (canvasWidth > 0 && canvasHeight > 0) {
            new ScreenMap(0, 0, canvasWidth, canvasHeight);

        }

        return query;
    }

    private FeatureMessage toMessage(SimpleFeature f) throws IOException {
        Builder b = FeatureMessage.newBuilder();
        b.setFid(f.getID());

        final int attributeCount = f.getAttributeCount();
        for (int i = 0; i < attributeCount; i++) {
            Object value = f.getAttribute(i);
            AttributeValue attmessage = toAttribute(value);
            b.addValues(attmessage);
        }
        return b.build();
    }

    private AttributeValue toAttribute(Object value) throws IOException {
        final FieldType fieldType = FieldType.forValue(value);
        AttributeValue.Builder builder = AttributeValue.newBuilder();
        switch (fieldType) {
        case NULL:
            break;
        case STRING:
        case BIG_DECIMAL:
        case BIG_INTEGER:
            return builder.setStringval(value.toString()).build();
        case BOOLEAN:
            return builder.setBoolval(((Boolean) value).booleanValue()).build();
        case BYTE:
        case CHAR:
        case SHORT:
        case INTEGER:
            return builder.setIntval(((Number) value).intValue()).build();
        case LONG:
            return builder.setLongval(((Number) value).longValue()).build();
        case FLOAT:
            return builder.setFloatval(((Number) value).floatValue()).build();
        case DOUBLE:
            return builder.setDoubleval(((Number) value).doubleValue()).build();

        case GEOMETRY:
        case GEOMETRYCOLLECTION:
        case LINESTRING:
        case MULTILINESTRING:
        case MULTIPOINT:
        case MULTIPOLYGON:
        case POINT:
        case POLYGON: {
            BytearrayOutStream out = new BytearrayOutStream();
            new WKBWriter().write((Geometry) value, out);
            byte[] bytes = out.buff();
            int len = out.size();
            return builder.setWkb(ByteString.copyFrom(bytes, 0, len)).build();
        }

        case MAP:
        case DATE:
        case DATETIME:
        case TIME:
        case TIMESTAMP:
        case INTEGER_ARRAY:
        case LONG_ARRAY:
        case SHORT_ARRAY:
        case STRING_ARRAY:
        case BOOLEAN_ARRAY:
        case BYTE_ARRAY:
        case CHAR_ARRAY:
        case FLOAT_ARRAY:
        case DOUBLE_ARRAY:
        case ENVELOPE_2D:
        case UUID: {
            System.err.println("Not yet supported: " + fieldType);
        }
        default:
            throw new UnsupportedOperationException("Unknown value type: " + fieldType);
        }
        return builder.build();
    }

    private static class BytearrayOutStream extends ByteArrayOutputStream implements OutStream {

        BytearrayOutStream() {
            super(512);
        }

        public @Override void write(byte[] buf, int len) throws IOException {
            super.write(buf, 0, len);
        }

        public byte[] buff() {
            return super.buf;
        }
    }
}
