/*******************************************************************************
 * Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *******************************************************************************/
package org.locationtech.geogig.storage.datastream;

import static org.locationtech.geogig.storage.datastream.Varint.readSignedVarLong;
import static org.locationtech.geogig.storage.datastream.Varint.readUnsignedVarInt;
import static org.locationtech.geogig.storage.datastream.Varint.writeSignedVarLong;
import static org.locationtech.geogig.storage.datastream.Varint.writeUnsignedVarInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequenceFilter;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;

class GeometryEncoder {
    private static final int TYPE_POINT = 0x01;

    private static final int TYPE_LINESTRING = 0x02;

    private static final int TYPE_POLYGON = 0x03;

    private static final int TYPE_MULTIPOINT = 0x04;

    private static final int TYPE_MULTILINESTRING = 0x05;

    private static final int TYPE_MULTIPOLYGON = 0x06;

    private static final int TYPE_GEOMETRYCOLLECTION = 0x07;

    private static final int MASK_GEOMETRY_TYPE = 0b00000111;

    private static final int MASK_EMPTY_GEOMETRY = 0b00001000;

    private static final int MASK_DECIMAL_PLACES = 0b11110000;

    private static final Map<String, Integer> GEOMETRY_TYPES = ImmutableMap
            .<String, Integer> builder().put("Point", Integer.valueOf(TYPE_POINT))//
            .put("LineString", Integer.valueOf(TYPE_LINESTRING))//
            .put("Polygon", Integer.valueOf(TYPE_POLYGON))//
            .put("MultiPoint", Integer.valueOf(TYPE_MULTIPOINT))//
            .put("MultiLineString", Integer.valueOf(TYPE_MULTILINESTRING))//
            .put("MultiPolygon", Integer.valueOf(TYPE_MULTIPOLYGON))//
            .put("GeometryCollection", Integer.valueOf(TYPE_GEOMETRYCOLLECTION))//
            .build();

    private static GeometryEncoder[] ENCODERS = new GeometryEncoder[] {//
    new GeometryEncoder(),//
            new PointEncoder(),//
            new LineStringEncoder(),//
            new PolygonEncoder(),//
            new MultiPointEncoder(),//
            new MultiLineStringEncoder(),//
            new MultiPolygonEncoder(),//
            new GeometryCollectionEncoder() //
    };

    public static final GeometryEncoder INSTANCE = ENCODERS[0];

    private GeometryEncoder() {
        // private constructor
    }

    public void write(Geometry geom, DataOutput out, GeometryFactory factory) throws IOException {
        double scale = factory.getPrecisionModel().getScale();
        write(geom, out, scale);
    }

    protected final void write(Geometry geom, DataOutput out, double scale) throws IOException {
        final int geometryType = getGeometryType(geom);
        final int emptyMask = geom.isEmpty() ? MASK_EMPTY_GEOMETRY : 0x0;
        int decimalPlaces = (int) Math.log10(scale);
        decimalPlaces <<= 4;

        final int typeAndMasks = geometryType | emptyMask | decimalPlaces;

        writeUnsignedVarInt(typeAndMasks, out);

        if (emptyMask != MASK_EMPTY_GEOMETRY) {
            GeometryEncoder concreteEncoder = ENCODERS[geometryType];
            concreteEncoder.writeBody(geom, out, scale);
        }
    }

    public Geometry read(DataInput in, GeometryFactory factory) throws IOException {

        final int typeAndMasks = readUnsignedVarInt(in);
        final int type = typeAndMasks & MASK_GEOMETRY_TYPE;
        final boolean empty = (typeAndMasks & MASK_EMPTY_GEOMETRY) == MASK_EMPTY_GEOMETRY;
        final int numDecimalPlaces = (typeAndMasks & MASK_DECIMAL_PLACES) >> 4;

        GeometryEncoder concreteEncoder = ENCODERS[type];
        Geometry geom;
        if (empty) {
            geom = concreteEncoder.createEmpty(factory);
        } else {
            double scale = Math.pow(10, numDecimalPlaces);
            geom = concreteEncoder.readBody(in, factory, scale);
        }
        return geom;
    }

    protected Geometry readBody(DataInput in, GeometryFactory factory, double scale)
            throws IOException {
        throw new UnsupportedOperationException("Must be overriden");
    }

    protected Geometry createEmpty(GeometryFactory factory) {
        throw new UnsupportedOperationException("Must be overriden");
    }

    protected void writeBody(Geometry geom, DataOutput out, double scale) throws IOException {
        throw new UnsupportedOperationException("Must be overriden");
    }

    private static int getGeometryType(Geometry geom) {
        return GEOMETRY_TYPES.get(geom.getGeometryType()).intValue();
    }

    static final CoordinateSequence newSequence(GeometryFactory factory, final int size) {
        CoordinateSequence seq = factory.getCoordinateSequenceFactory().create(size, 2);
        return seq;
    }

    /**
     * Converts the requested coordinate from double to fixed precision.
     */
    static final long toFixedPrecision(final double ordinate, final GeometryFactory factory) {
        assert factory.getPrecisionModel().getType() == PrecisionModel.FIXED;
        double scale = factory.getPrecisionModel().getScale();
        return toFixedPrecision(ordinate, scale);
    }

    static final long toFixedPrecision(final double ordinate, final double scale) {
        double scaled = ordinate * scale;
        if (scaled > Long.MAX_VALUE || scaled < Long.MIN_VALUE) {
            throw new IllegalArgumentException(String.format(
                    "Ordinate %e scaled by %e (%e) out of long range", ordinate, scale, scaled));
        }

        long fixedPrecisionOrdinate = Math.round(scaled);
        return fixedPrecisionOrdinate;
    }

    static final void writeSequence(final Geometry geom, final DataOutput out, final double scale)
            throws IOException {
        writeSequence(geom, out, scale, true);
    }

    static final void writeSequence(final Geometry geom, final DataOutput out, final double scale,
            final boolean writeLength) throws IOException {

        // trick to get the internal coord sequence without duplicating code with
        // writeSequence(final CoordinateSequence, ...) and having to wrap any eventual IOException
        // with a RTE
        final CoordinateSequence[] seqHolder = new CoordinateSequence[1];
        geom.apply(new CoordinateSequenceFilter() {
            @Override
            public boolean isDone() {
                return true;
            }

            @Override
            public void filter(CoordinateSequence seq, int i) {
                seqHolder[0] = seq;
            }

            @Override
            public boolean isGeometryChanged() {
                return false;
            }
        });

        writeSequence(seqHolder[0], out, scale, writeLength);

    }

    static final void writeSequence(final CoordinateSequence seq, final DataOutput out,
            final double scale, final boolean writeLength) throws IOException {

        final int size = seq.size();

        long previousX = 0;

        long previousY = 0;

        if (writeLength) {
            writeUnsignedVarInt(size, out);
        }
        for (int i = 0; i < size; i++) {
            double x = seq.getOrdinate(i, 0);
            double y = seq.getOrdinate(i, 1);

            long fixedX = toFixedPrecision(x, scale);
            long fixedY = toFixedPrecision(y, scale);

            long deltaX = fixedX - previousX;
            long deltaY = fixedY - previousY;
            previousX = fixedX;
            previousY = fixedY;
            writeSignedVarLong(deltaX, out);
            writeSignedVarLong(deltaY, out);
        }
    }

    static double toDoublePrecision(final long fixedPrecisionOrdinate, final double scale) {
        double ordinate = fixedPrecisionOrdinate / scale;
        return ordinate;
    }

    static final CoordinateSequence readSequence(DataInput in, GeometryFactory factory, double scale)
            throws IOException {
        final int size = readUnsignedVarInt(in);
        return readSequence(in, factory, size, scale);
    }

    static final CoordinateSequence readSequence(DataInput in, GeometryFactory factory,
            final int size, double scale) throws IOException {

        long deltaX;
        long deltaY;

        long fixedX = readSignedVarLong(in);
        long fixedY = readSignedVarLong(in);
        double x = toDoublePrecision(fixedX, scale);
        double y = toDoublePrecision(fixedY, scale);

        CoordinateSequence seq = factory.getCoordinateSequenceFactory().create(size, 2);
        seq.setOrdinate(0, 0, x);
        seq.setOrdinate(0, 1, y);

        for (int i = 1; i < size; i++) {
            deltaX = readSignedVarLong(in);
            deltaY = readSignedVarLong(in);

            fixedX += deltaX;
            fixedY += deltaY;

            x = toDoublePrecision(fixedX, scale);
            y = toDoublePrecision(fixedY, scale);
            if (Double.isInfinite(x) || Double.isInfinite(y) || Double.isNaN(x) || Double.isNaN(y)) {
                throw new IllegalArgumentException();
            }
            seq.setOrdinate(i, 0, x);
            seq.setOrdinate(i, 1, y);
        }

        return seq;
    }

    private static Polygon readPolygon(DataInput in, GeometryFactory factory, double scale)
            throws IOException {
        LinearRing shell = factory.createLinearRing(readSequence(in, factory, scale));
        final int numHoles = readUnsignedVarInt(in);
        LinearRing[] holes = new LinearRing[numHoles];
        for (int h = 0; h < numHoles; h++) {
            holes[h] = factory.createLinearRing(readSequence(in, factory, scale));
        }
        Polygon polygon = factory.createPolygon(shell, holes);
        return polygon;
    }

    private static void writePolygon(DataOutput out, double scale, Polygon p) throws IOException {
        writeSequence(p.getExteriorRing(), out, scale);
        final int numHoles = p.getNumInteriorRing();
        writeUnsignedVarInt(numHoles, out);
        LineString hole;
        for (int h = 0; h < numHoles; h++) {
            hole = p.getInteriorRingN(h);
            writeSequence(hole, out, scale);
        }
    }

    private static final class PointEncoder extends GeometryEncoder {
        @Override
        protected Geometry createEmpty(GeometryFactory factory) {
            return factory.createPoint((CoordinateSequence) null);
        }

        @Override
        protected Geometry readBody(DataInput in, GeometryFactory factory, double scale)
                throws IOException {
            CoordinateSequence seq = readSequence(in, factory, 1, scale);
            return factory.createPoint(seq);
        }

        @Override
        protected void writeBody(Geometry geom, DataOutput out, double scale) throws IOException {
            writeSequence(geom, out, scale, false);
        }
    }

    private static final class LineStringEncoder extends GeometryEncoder {
        @Override
        protected Geometry createEmpty(GeometryFactory factory) {
            return factory.createLineString((CoordinateSequence) null);
        }

        @Override
        protected Geometry readBody(DataInput in, GeometryFactory factory, double scale)
                throws IOException {
            CoordinateSequence seq = readSequence(in, factory, scale);
            return factory.createLineString(seq);
        }

        @Override
        protected void writeBody(Geometry geom, DataOutput out, double scale) throws IOException {
            writeSequence(geom, out, scale);
        }
    }

    private static final class PolygonEncoder extends GeometryEncoder {
        @Override
        protected Geometry createEmpty(GeometryFactory factory) {
            return factory.createPolygon((CoordinateSequence) null);
        }

        @Override
        protected Geometry readBody(DataInput in, GeometryFactory factory, double scale)
                throws IOException {
            return readPolygon(in, factory, scale);
        }

        @Override
        protected void writeBody(Geometry geom, DataOutput out, double scale) throws IOException {
            Polygon p = (Polygon) geom;
            writePolygon(out, scale, p);
        }

    }

    private static final class MultiPointEncoder extends GeometryEncoder {
        @Override
        protected Geometry createEmpty(GeometryFactory factory) {
            return factory.createMultiPoint((Point[]) null);
        }

        @Override
        protected Geometry readBody(DataInput in, GeometryFactory factory, double scale)
                throws IOException {
            CoordinateSequence seq = readSequence(in, factory, scale);
            return factory.createMultiPoint(seq);
        }

        @Override
        protected void writeBody(Geometry geom, DataOutput out, double scale) throws IOException {
            MultiPoint mp = (MultiPoint) geom;
            final int npoints = mp.getNumGeometries();
            final GeometryFactory factory = geom.getFactory();

            // use writeSequence to take advantage of delta compression
            CoordinateSequence seq = factory.getCoordinateSequenceFactory().create(npoints, 2);
            Point p;
            for (int i = 0; i < npoints; i++) {
                p = (Point) mp.getGeometryN(i);
                seq.setOrdinate(i, 0, p.getX());
                seq.setOrdinate(i, 1, p.getY());
            }
            writeSequence(seq, out, scale, true);
        }
    }

    private static final class MultiLineStringEncoder extends GeometryEncoder {
        @Override
        protected Geometry createEmpty(GeometryFactory factory) {
            return factory.createMultiLineString((LineString[]) null);
        }

        @Override
        protected Geometry readBody(DataInput in, GeometryFactory factory, double scale)
                throws IOException {
            final int nlines = readUnsignedVarInt(in);
            LineString[] lines = new LineString[nlines];
            CoordinateSequence seq;
            for (int i = 0; i < nlines; i++) {
                seq = readSequence(in, factory, scale);
                lines[i] = factory.createLineString(seq);
            }
            return factory.createMultiLineString(lines);
        }

        @Override
        protected void writeBody(final Geometry geom, final DataOutput out, final double scale)
                throws IOException {
            final MultiLineString ml = (MultiLineString) geom;
            final int nlines = ml.getNumGeometries();
            writeUnsignedVarInt(nlines, out);
            for (int i = 0; i < nlines; i++) {
                LineString line = (LineString) ml.getGeometryN(i);
                writeSequence(line, out, scale);
            }
        }
    }

    private static final class MultiPolygonEncoder extends GeometryEncoder {
        @Override
        protected Geometry createEmpty(GeometryFactory factory) {
            return factory.createMultiPolygon((Polygon[]) null);
        }

        @Override
        protected Geometry readBody(DataInput in, GeometryFactory factory, double scale)
                throws IOException {
            final int npolys = readUnsignedVarInt(in);
            Polygon[] polys = new Polygon[npolys];
            for (int i = 0; i < npolys; i++) {
                polys[i] = readPolygon(in, factory, scale);
            }
            return factory.createMultiPolygon(polys);
        }

        @Override
        protected void writeBody(final Geometry geom, final DataOutput out, final double scale)
                throws IOException {
            final MultiPolygon mp = (MultiPolygon) geom;
            final int npolys = mp.getNumGeometries();
            writeUnsignedVarInt(npolys, out);
            for (int i = 0; i < npolys; i++) {
                writePolygon(out, scale, (Polygon) mp.getGeometryN(i));
            }
        }
    }

    private static final class GeometryCollectionEncoder extends GeometryEncoder {
        @Override
        protected Geometry createEmpty(GeometryFactory factory) {
            return factory.createGeometryCollection((Geometry[]) null);
        }

        @Override
        protected Geometry readBody(DataInput in, GeometryFactory factory, double scale)
                throws IOException {
            final int ngeoms = readUnsignedVarInt(in);
            Geometry[] geoms = new Geometry[ngeoms];
            for (int i = 0; i < ngeoms; i++) {
                geoms[i] = super.read(in, factory);
            }
            return factory.createGeometryCollection(geoms);
        }

        @Override
        protected void writeBody(final Geometry geom, final DataOutput out, final double scale)
                throws IOException {
            final int ngeoms = geom.getNumGeometries();
            writeUnsignedVarInt(ngeoms, out);
            for (int i = 0; i < ngeoms; i++) {
                super.write(geom.getGeometryN(i), out, scale);
            }
        }
    }
}
