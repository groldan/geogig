/*******************************************************************************
 * Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *******************************************************************************/
package org.locationtech.geogig.storage.datastream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.base.Throwables;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequenceFilter;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class GeometrySerializerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private GeometrySerializer serializer;

    ByteArrayDataOutput out;

    @Before
    public void before() {
        out = ByteStreams.newDataOutput();
        serializer = GeometrySerializer.defaultInstance();
    }

    private Geometry geom(String wkt) {
        try {
            return new WKTReader().read(wkt);
        } catch (ParseException e) {
            throw Throwables.propagate(e);
        }
    }

    private ByteArrayDataInput getInput() {
        return ByteStreams.newDataInput(out.toByteArray());
    }

    @Test
    public void testNull() throws IOException {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("null geometry");
        serializer.write(null, out);
    }

    @Test
    public void testEmptyGeometry() throws IOException {
        testEmptyGeometry("POINT EMPTY");
        testEmptyGeometry("LINESTRING EMPTY");
        testEmptyGeometry("POLYGON EMPTY");
        testEmptyGeometry("MULTIPOINT EMPTY");
        testEmptyGeometry("MULTILINESTRING EMPTY");
        testEmptyGeometry("MULTIPOLYGON EMPTY");
        testEmptyGeometry("GEOMETRYCOLLECTION EMPTY");
    }

    private void testEmptyGeometry(String wkt) throws IOException {
        out = ByteStreams.newDataOutput();
        Geometry original = geom(wkt);
        serializer.write(original, out);
        Geometry read = serializer.read(getInput());
        assertNotNull(read);
        assertEquals(original.getGeometryType(), read.getGeometryType());
        assertSame(serializer.getFactory(), read.getFactory());
        assertTrue(read.isEmpty());
    }

    @Test
    public void testPoint() throws IOException {
        testGeom("POINT (1.0 1.1)");
        testGeom("POINT (1000 -1000)");
        testGeom("POINT (-32.34546 -17.45652546)");
        testGeom("POINT (1000.00000000000009 -1000.00000000000009)");
        testGeom("POINT (1000000.00000000009 -1000000.00000000009)");
        testGeom("POINT (10000000.0000000009 -10000000.0000000009)");
        testGeom("POINT (100000000.000000009 -100000000.000000009)");
        testGeom("POINT (1000000000.00000009 -1000000000.00000009)");

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("out of long range");
        testGeom("POINT (1e17 -1)");
    }

    @Test
    public void testMultiPoint() throws IOException {
        testGeom("MULTIPOINT (1.0 1.1, 2.0 2.1, 1000 2000, 50000000 25000000)");
    }

    @Test
    public void testLineString() throws IOException {
        testGeom("LINESTRING (1.0 1.1, 2.0 2.1, 1000 2000, 50000000 -25000000)");
    }

    @Test
    public void testMultiLineString() throws IOException {
        testGeom("MULTILINESTRING ((1.0 1.1, 2.0 2.1), (1000 2000, 50000000 -25000000))");
    }

    @Test
    public void testPolygon() throws IOException {
        testGeom("POLYGON ((1.0 1.1, 2.0 2.1, 3.0 3.1, 1.0 1.1))");
        testGeom("POLYGON ((-10 -10, -10 10, 10 10, 10 -10, -10 -10),(-1 -1, -1 1, 1 1, 1 -1, -1 -1))");
        testGeom("POLYGON ((-10 -10, -10 10, 10 10, 10 -10, -10 -10),(-1 -1, -1 1, 1 1, 1 -1, -1 -1),(3 3, 3 4, 4 4, 4 4, 3 3))");
    }

    @Test
    public void testMultiPolygon() throws IOException {
        testGeom("MULTIPOLYGON (((1.0 1.1, 2.0 2.1, 3.0 3.1, 1.0 1.1)))");

        testGeom("MULTIPOLYGON (((1.0 1.1, 2.0 2.1, 3.0 3.1, 1.0 1.1)),"//
                + "((1.0 1.1, 2.0 2.1, 3.0 3.1, 1.0 1.1)),"//
                + "((-10 -10, -10 10, 10 10, 10 -10, -10 -10),(-1 -1, -1 1, 1 1, 1 -1, -1 -1)),"//
                + "((-10 -10, -10 10, 10 10, 10 -10, -10 -10),(-1 -1, -1 1, 1 1, 1 -1, -1 -1),(3 3, 3 4, 4 4, 4 4, 3 3)))");
    }

    @Test
    public void testGeometryCollection() throws IOException {
        testGeom("GEOMETRYCOLLECTION( POINT (1.0 1.1) )");

        testGeom("GEOMETRYCOLLECTION(POINT (1.0 1.1)),"//
                + " MULTIPOLYGON (((1.0 1.1, 2.0 2.1, 3.0 3.1, 1.0 1.1)),"//
                + "  ((1.0 1.1, 2.0 2.1, 3.0 3.1, 1.0 1.1)),"//
                + "  ((-10 -10, -10 10, 10 10, 10 -10, -10 -10),(-1 -1, -1 1, 1 1, 1 -1, -1 -1)),"//
                + "  ((-10 -10, -10 10, 10 10, 10 -10, -10 -10),(-1 -1, -1 1, 1 1, 1 -1, -1 -1),(3 3, 3 4, 4 4, 4 4, 3 3))),"
                + " MULTILINESTRING ((1.0 1.1, 2.0 2.1), (1000 2000, 50000000 -25000000)))"//
        );
    }

    private void testGeom(String wkt) throws IOException {
        testGeom(wkt, 1);
        testGeom(wkt, 2);
        testGeom(wkt, 3);
        testGeom(wkt, 4);
        testGeom(wkt, 5);
        testGeom(wkt, 6);
        testGeom(wkt, 7);
        testGeom(wkt, 8);
        testGeom(wkt, 9);
    }

    private void testGeom(final String wkt, final int numDecimals) throws IOException {

        final GeometrySerializer serializer = GeometrySerializer.withDecimalPlaces(numDecimals);
        out = ByteStreams.newDataOutput();
        final Geometry geom = geom(wkt);
        serializer.write(geom, out);
        // byte[] bytes = out.toByteArray();
        // byte[] wkb = new WKBWriter().write(geom);
        // System.err.printf("Size: %,d, wkb: %,d (%,d%%) decimals:%d\t\t%s\n", bytes.length,
        // wkb.length, (bytes.length * 100 / wkb.length), numDecimals, wkt);

        Geometry read = serializer.read(getInput());

        Geometry expected = serializer.getFactory().createGeometry(geom);
        expected.apply(new CoordinateSequenceFilter() {

            @Override
            public boolean isGeometryChanged() {
                return true;
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public void filter(CoordinateSequence seq, int i) {
                PrecisionModel precisionModel = serializer.getFactory().getPrecisionModel();
                seq.setOrdinate(i, 0, precisionModel.makePrecise(seq.getOrdinate(i, 0)));
                seq.setOrdinate(i, 1, precisionModel.makePrecise(seq.getOrdinate(i, 1)));
            }
        });

        assertEquals(expected, read);
    }
}
