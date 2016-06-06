package org.locationtech.geogig.api.porcelain;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.locationtech.geogig.porcelain.BranchCreateOp;
import org.locationtech.geogig.porcelain.CheckoutOp;
import org.locationtech.geogig.test.integration.RepositoryTestCase;

import com.google.common.base.Suppliers;

public class DumpTest extends RepositoryTestCase {

    @Override
    protected void setUpInternal() throws Exception {
        insertAndAdd(points1);
        commit("points1");
        insertAndAdd(lines1);
        commit("lines1");
        geogig.command(BranchCreateOp.class).setName("b1").call();
        insertAndAdd(points2);
        commit("points2");
        geogig.command(CheckoutOp.class).setSource("b1").call();
        insertAndAdd(points1_modified);
        commit("points1 modified");
        geogig.command(CheckoutOp.class).setSource("master").call();
    }

    @Test
    public void test() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        geogig.command(DumpOp.class).setOutput(Suppliers.ofInstance(out)).call();
        out.close();

    }
}
