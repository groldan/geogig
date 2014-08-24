/* Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.datastream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.CoordinateSequenceFactory;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequenceFactory;

/**
 * Serialization format for JTS geometries more compact than WKB for fixed precision models.
 */
class GeometrySerializer implements ValueSerializer {

    private static final double DEFAULT_FIXED_PRECISION_FACTOR = 1e9;

    private static final CoordinateSequenceFactory DEFAULT_COORDINATE_SEQUENCE_FACTORY = new PackedCoordinateSequenceFactory();

    private static final PrecisionModel DEFAULT_PRECISION_MODEL = new PrecisionModel(
            DEFAULT_FIXED_PRECISION_FACTOR);

    private static final GeometryFactory DEFAULT_GEOMETRY_FACTORY = new GeometryFactory(
            DEFAULT_PRECISION_MODEL, 0, DEFAULT_COORDINATE_SEQUENCE_FACTORY);

    private static final GeometryEncoder GEOMETRY_ENCODER = GeometryEncoder.INSTANCE;

    private static final GeometrySerializer DEFAULT_PRECISION = new GeometrySerializer(
            DEFAULT_GEOMETRY_FACTORY);

    private GeometryFactory factory;

    GeometrySerializer(GeometryFactory factory) {
        this.factory = factory;
    }

    public static GeometrySerializer defaultInstance() {
        return DEFAULT_PRECISION;
    }

    public static GeometrySerializer withDecimalPlaces(int numDecimals) {
        Preconditions.checkArgument(numDecimals > 0 && numDecimals < 10);
        double scale = Math.pow(10, numDecimals);
        PrecisionModel pm = new PrecisionModel(scale);
        GeometryFactory factory = new GeometryFactory(pm, 0, DEFAULT_COORDINATE_SEQUENCE_FACTORY);
        return new GeometrySerializer(factory);
    }

    public GeometryFactory getFactory() {
        return factory;
    }

    @Override
    public void write(Object obj, final DataOutput out) throws IOException {
        final Geometry geom = (Geometry) obj;
        write(geom, out);
    }

    public void write(Geometry geom, final DataOutput out) throws IOException {
        Preconditions.checkNotNull(geom, "null geometry");
        GEOMETRY_ENCODER.write(geom, out, factory);
    }

    @Override
    public Geometry read(DataInput in) throws IOException {
        Geometry geom = GEOMETRY_ENCODER.read(in, factory);
        return geom;
    }

    /**
     * Converts the requested coordinate from double to fixed precision.
     */
    public static final int toFixedPrecision(double ordinate, double precision) {
        int fixedPrecisionOrdinate = (int) Math.round(ordinate * precision);
        return fixedPrecisionOrdinate;
    }

    /**
     * Converts the requested coordinate from fixed to double precision.
     */
    public static double toDoublePrecision(int fixedPrecisionOrdinate, double precision) {
        double ordinate = fixedPrecisionOrdinate / precision;
        return ordinate;
    }

}
