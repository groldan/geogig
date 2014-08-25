package org.locationtech.geogig.storage.datastream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

interface ValueSerializer {

    public Object read(DataInput in) throws IOException;

    public void write(Object obj, DataOutput out) throws IOException;

}