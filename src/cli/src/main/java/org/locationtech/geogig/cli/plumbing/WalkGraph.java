/* Copyright (c) 2013-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Johnathan Garrett (LMN Solutions) - initial implementation
 */
package org.locationtech.geogig.cli.plumbing;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.locationtech.geogig.cli.AbstractCommand;
import org.locationtech.geogig.cli.CLICommand;
import org.locationtech.geogig.cli.CommandFailedException;
import org.locationtech.geogig.cli.Console;
import org.locationtech.geogig.cli.GeogigCLI;
import org.locationtech.geogig.cli.annotation.ReadOnly;
import org.locationtech.geogig.cli.porcelain.GraphMLPrinter;
import org.locationtech.geogig.model.Bounded;
import org.locationtech.geogig.model.Bucket;
import org.locationtech.geogig.model.Node;
import org.locationtech.geogig.model.NodeRef;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.Ref;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.model.RevFeatureType;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevObjects;
import org.locationtech.geogig.model.RevTag;
import org.locationtech.geogig.plumbing.WalkGraphOp;
import org.locationtech.geogig.plumbing.WalkGraphOp.Listener;
import org.locationtech.geogig.plumbing.diff.PreOrderDiffWalk.BucketIndex;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 *
 */
@ReadOnly
@Parameters(commandNames = "walk-graph", commandDescription = "Visit objects in history graph in post order (referenced objects before referring objects)")
public class WalkGraph extends AbstractCommand implements CLICommand {

    @Parameter(description = "<[refspec]:[path]>", arity = 1)
    private List<String> refList = Lists.newArrayList();

    @Parameter(names = { "-v",
            "--verbose" }, description = "Verbose output, include metadata, object id, and object type among object path.")
    private boolean verbose;

    @Parameter(names = "--graphml", description = "Print as a GraphML document")
    public boolean graphml;

    @Parameter(names = { "--skip-features", "-sf" }, description = "Do not report feature nodes")
    public boolean skipFeatures;

    @Parameter(names = { "--skip-buckets",
            "-sb" }, description = "Do not report bucket tree nodes (implies --skip-featues)")
    public boolean skipBuckets;

    @Parameter(names = {
            "--verify" }, description = "Verify every reported object exists in the database")
    public boolean verify;

    @Parameter(names = { "--all",
            "-a" }, description = "Follow all reachable objects from all (local) refs")
    public boolean all;

    @Parameter(names = { "--remotes" }, description = "Include remotes refs")
    public boolean remotes;

    @Override
    public void runInternal(final GeogigCLI cli) throws IOException {
        String ref;
        if (refList.isEmpty()) {
            ref = null;
        } else {
            ref = refList.get(0);
        }

        Console console = cli.getConsole();
        Listener listener;
        if (graphml) {
            listener = new GraphMLListener(System.out);
        } else {
            final Function<Object, CharSequence> printFunctor = verbose ? VERBOSE_FORMATTER
                    : FORMATTER;
            listener = new PrintListener(console, printFunctor);
        }
        try {
            cli.getGeogig().command(WalkGraphOp.class)//
                    .setReference(ref)//
                    .setAll(all)//
                    .setIncludeRemotes(remotes)//
                    .setListener(listener)//
                    .setSkipFeatures(skipFeatures)//
                    .setSkipBuckets(skipBuckets)//
                    .setVerify(verify)//
                    .call();
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new CommandFailedException(e.getMessage(), true);
        } finally {
            console.flush();
        }
        if (graphml) {
            ((GraphMLListener) listener).end();
        }
    }

    private static class GraphMLListener implements WalkGraphOp.Listener {

        private GraphMLPrinter printer;

        private Set<String> visited = new HashSet<>();

        private Set<String> edges = new HashSet<>();

        private Stack<String> stack = new Stack<>();

        private StringBuilder idhelp = new StringBuilder();

        public GraphMLListener(OutputStream out) {
            printer = new GraphMLPrinter(out);
            printer.start();
            printer.node("Repository");
            stack.add("Repository");
        }

        public void end() {
            printer.end();
        }

        private String id(ObjectId id) {
            idhelp.setLength(0);
            RevObjects.toString(id, 4, idhelp);
            return idhelp.toString();
        }

        private String id(RevObject o) {
            return id(o.getId());
        }

        @Override
        public void ref(Ref ref) {
            String name = ref.getName();
            if (visited.add(name)) {
                printer.startNode(name).startData().label(name).shapeParallelogram().endData()
                        .endNode();
            }
            if (!stack.isEmpty()) {
                String parentRef = stack.peek();
                printer.edge(parentRef, name);
            }
            stack.push(name);
        }

        @Override
        public void endRef(Ref ref) {
            stack.pop();
        }

        @Override
        public void commit(RevCommit commit) {
            String id = id(commit);
            if (visited.add(id)) {
                printer.startNode(id).startData().shapeEllipse().label(id).endData().endNode();
                // commit.getParentIds().forEach((parentId) -> {
                // printer.edge(id, id(parentId));
                // });
            }
            if (!stack.isEmpty()) {
                String ref = stack.peek();
                if (addEdge(ref, id)) {
                    printer.edge(ref, id);
                }
            }
            stack.push(id);
        }

        @Override
        public void endCommit(RevCommit commit) {
            stack.pop();
        }

        @Override
        public void starTree(NodeRef treeNode) {
            String treeId = id(treeNode.getObjectId());
            if (visited.add(treeId)) {
                printer.startNode(treeId).startData().label(treeId).shapeHexagon().endData()
                        .endNode();
            }

            if (!stack.isEmpty()) {
                String parentId = stack.peek();
                if (addEdge(parentId, treeId)) {
                    printer.startEdge(parentId, treeId).startData().label(treeNode.name()).endData()
                            .endEdge();
                }
            }

            ObjectId metadataId = treeNode.getMetadataId();
            if (!metadataId.isNull()) {
                String mdId = id(metadataId);
                if (addEdge(treeId, mdId)) {
                    printer.startEdge(treeId, mdId).startData().label("metadata").endData()
                            .endEdge();
                }
            }

            stack.push(treeId);

        }

        @Override
        public void endTree(NodeRef treeNode) {
            String treeId = id(treeNode.getObjectId());
            String pop = stack.pop();
            Preconditions.checkState(treeId.equals(pop));
        }

        @Override
        public void featureType(RevFeatureType ftype) {
            String id = id(ftype);
            if (visited.add(id)) {
                printer.startNode(id).startData().label(id).shapeParallelogram().endData()
                        .endNode();
            }
        }

        @Override
        public void feature(NodeRef featureNode) {
            String parentId = stack.peek();
            String name = featureNode.name();
            String id = id(featureNode.getObjectId());
            Optional<ObjectId> explicitMetadataId = featureNode.getNode().getMetadataId();

            if (visited.add(id)) {
                printer.startNode(id).startData().label(id).shapeDiamond().endData().endNode();
            }
            if (addEdge(parentId, id)) {
                printer.startEdge(parentId, id).startData().label(name).shapeDiamond().endData()
                        .endEdge();
            }
            if (explicitMetadataId.isPresent()) {
                String mdid = id(explicitMetadataId.get());
                if (addEdge(id, mdid)) {
                    printer.startEdge(id, mdid).startData().label("metadata").endData().endEdge();
                }
            }
        }

        @Override
        public void bucket(BucketIndex bucketIndex, Bucket bucket) {
            String bucketId = id(bucket.getObjectId());
            if (visited.add(bucketId)) {
                printer.startNode(bucketId).startData().label(bucketId).shapeTrapezoid().endData()
                        .endNode();
            }
            String parentId = stack.peek();
            if (addEdge(parentId, bucketId)) {
                printer.edge(parentId, bucketId);
            }
            stack.push(bucketId);
        }

        @Override
        public void endBucket(BucketIndex bucketIndex, Bucket bucket) {
            String bucketId = id(bucket.getObjectId());
            String pop = stack.pop();
            Preconditions.checkState(bucketId.equals(pop));
        }

        @Override
        public void startTag(RevTag tag) {
            String name = tag.getName();
            printer.node(name);
            stack.push(name);
        }

        @Override
        public void endTag(RevTag tag) {
            stack.pop();
        }

        private boolean addEdge(String parent, String child) {
            String edge = parent + "/" + child;
            if (edges.contains(edge)) {
                return false;
            }
            edges.add(edge);
            return true;
        }
    }

    private static class PrintListener implements WalkGraphOp.Listener {

        private final Console console;

        private final Function<Object, CharSequence> printFunctor;

        public PrintListener(Console console, Function<Object, CharSequence> printFunctor) {
            this.console = console;
            this.printFunctor = printFunctor;
        }

        private void print(Object b) {
            try {
                CharSequence line = printFunctor.apply(b);
                console.println(line);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void starTree(NodeRef treeNode) {
            print(treeNode);
        }

        @Override
        public void feature(NodeRef featureNode) {
            print(featureNode);
        }

        @Override
        public void endTree(NodeRef treeNode) {

        }

        @Override
        public void bucket(BucketIndex bucketIndex, Bucket bucket) {
            print(bucket);
        }

        @Override
        public void endBucket(BucketIndex bucketIndex, Bucket bucket) {
        }

        @Override
        public void commit(RevCommit commit) {
            print(commit);
        }

        @Override
        public void featureType(RevFeatureType ftype) {
            print(ftype);
        }

        @Override
        public void ref(Ref ref) {
            print(ref);
        }

        @Override
        public void endCommit(RevCommit commit) {
            // TODO Auto-generated method stub

        }

        @Override
        public void endRef(Ref ref) {
            // TODO Auto-generated method stub

        }

        @Override
        public void startTag(RevTag tag) {
            // TODO Auto-generated method stub

        }

        @Override
        public void endTag(RevTag tag) {
            // TODO Auto-generated method stub

        }

    };

    private static final Function<Object, CharSequence> FORMATTER = new Function<Object, CharSequence>() {

        /**
         * @param o a {@link Node}, {@link Bucket}, or {@link RevObject}
         */
        @Override
        public CharSequence apply(Object o) {
            ObjectId id;
            String type;
            if (o instanceof Bounded) {
                Bounded b = (Bounded) o;
                id = b.getObjectId();
                if (b instanceof NodeRef) {
                    type = ((NodeRef) b).getType().toString();
                } else {
                    type = "BUCKET";
                }
            } else if (o instanceof RevObject) {
                id = ((RevObject) o).getId();
                type = ((RevObject) o).getType().toString();
            } else {
                throw new IllegalArgumentException();
            }
            return String.format("%s: %s", id, type);
        }
    };

    private static final Function<Object, CharSequence> VERBOSE_FORMATTER = new Function<Object, CharSequence>() {

        /**
         * @param o a {@link Node}, {@link Bucket}, or {@link RevObject}
         */
        @Override
        public CharSequence apply(Object o) {
            ObjectId id;
            String type;
            String extraData = "";
            if (o instanceof Ref) {
                return ((Ref) o).toString();
            }
            if (o instanceof Bounded) {
                Bounded b = (Bounded) o;
                id = b.getObjectId();
                if (b instanceof NodeRef) {
                    NodeRef node = (NodeRef) b;
                    type = node.getType().toString();
                    extraData = node.path();
                    if (!node.getMetadataId().isNull()) {
                        extraData += " [" + node.getMetadataId() + "]";
                    }
                } else {
                    type = "BUCKET";
                }
            } else if (o instanceof RevObject) {
                id = ((RevObject) o).getId();
                type = ((RevObject) o).getType().toString();
            } else {
                throw new IllegalArgumentException();
            }
            return String.format("%s: %s %s", id, type, extraData);
        }
    };
}
