package org.locationtech.geogig.storage.datastream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.locationtech.geogig.repository.Hints;

abstract class ValueSerializer {
    public Object read(DataInput in, Hints hints) throws IOException {
        return read(in);
    }

    public Object read(DataInput in) throws IOException {
        throw new UnsupportedOperationException("Must override");
    }

    public abstract void write(Object obj, DataOutput out) throws IOException;

}