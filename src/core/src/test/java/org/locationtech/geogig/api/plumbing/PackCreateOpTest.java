package org.locationtech.geogig.api.plumbing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2;
import org.locationtech.geogig.storage.impl.ObjectSerializingFactory;
import org.locationtech.geogig.test.integration.RepositoryTestCase;

import com.google.common.base.Suppliers;
import com.google.common.io.ByteStreams;

public class PackCreateOpTest extends RepositoryTestCase {

    RevCommit c1, c2, c3, c4;

    @Override
    protected void setUpInternal() throws Exception {
        insertAndAdd(points1);
        c1 = commit("points1");
        insertAndAdd(lines1);
        c2 = commit("lines1");
        insertAndAdd(points2);
        c3 = commit("points2");
        insertAndAdd(points1_modified);
        c4 = commit("points1 modified");
    }

    @Test
    public void test() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        geogig.command(PackCreateOp.class)//
                .setOutput(Suppliers.ofInstance(out))//
                .setOldTree(c1.getTreeId())//
                .setNewTree(c4.getTreeId())//
                .call();
        out.close();

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        ObjectSerializingFactory serializer = DataStreamSerializationFactoryV2.INSTANCE;
        byte[] rawid = new byte[ObjectId.NUM_BYTES];
        List<RevObject> readed = new ArrayList<>();
        while (true) {
            try {
                ByteStreams.readFully(in, rawid);
            } catch (EOFException eof) {
                break;
            }
            ObjectId id = new ObjectId(rawid);
            RevObject obj = serializer.read(id, in);
            System.err.println("Readed " + obj);
            readed.add(obj);
        }
        assertEquals(11, readed.size());
    }
}
