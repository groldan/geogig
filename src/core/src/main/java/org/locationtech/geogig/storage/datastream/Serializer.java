package org.locationtech.geogig.storage.datastream;

import static org.locationtech.geogig.storage.datastream.FormatCommonV2.requireHeader;
import static org.locationtech.geogig.storage.datastream.FormatCommonV2.writeHeader;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.RevObject;
import org.locationtech.geogig.api.RevObject.TYPE;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.ObjectReader;
import org.locationtech.geogig.storage.ObjectWriter;

import com.google.common.base.Throwables;

/**
 * Provides an interface for reading and writing objects.
 */
abstract class Serializer<T extends RevObject> implements ObjectReader<T>, ObjectWriter<T> {

    private final TYPE header;

    Serializer(TYPE type) {
        this.header = type;
    }

    @Override
    public T read(ObjectId id, InputStream rawData) throws IllegalArgumentException {
        return read(id, rawData, Hints.nil());
    }

    @Override
    public T read(ObjectId id, InputStream rawData, Hints hints) throws IllegalArgumentException {
        DataInput in = new DataInputStream(rawData);
        try {
            requireHeader(in, header);
            return readBody(id, in, hints);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    protected T readBody(ObjectId id, DataInput in) throws IOException {
        throw new UnsupportedOperationException("Must override");
    }

    protected T readBody(ObjectId id, DataInput in, Hints hints) throws IOException {
        return readBody(id, in);
    }

    /**
     * Writers must call
     * {@link FormatCommonV2#writeHeader(java.io.DataOutput, org.locationtech.geogig.api.RevObject.TYPE)}
     * , readers must not, in order for {@link ObjectReaderV2} to be able of parsing the header and
     * call the appropriate read method.
     */
    @Override
    public void write(T object, OutputStream out) throws IOException {
        DataOutput data = new DataOutputStream(out);
        writeHeader(data, object.getType());
        writeBody(object, data);
    }

    public abstract void writeBody(T object, DataOutput data) throws IOException;
}