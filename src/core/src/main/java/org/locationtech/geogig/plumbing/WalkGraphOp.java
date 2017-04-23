/* Copyright (c) 2013-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Johnathan Garrett (LMN Solutions) - initial implementation
 */
package org.locationtech.geogig.plumbing;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.model.Bucket;
import org.locationtech.geogig.model.NodeRef;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.Ref;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.model.RevFeatureType;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevTag;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.model.SymRef;
import org.locationtech.geogig.plumbing.diff.PreOrderDiffWalk;
import org.locationtech.geogig.plumbing.diff.PreOrderDiffWalk.BucketIndex;
import org.locationtech.geogig.plumbing.diff.PreOrderDiffWalk.Consumer;
import org.locationtech.geogig.porcelain.BranchListOp;
import org.locationtech.geogig.porcelain.TagListOp;
import org.locationtech.geogig.repository.AbstractGeoGigOp;
import org.locationtech.geogig.storage.ObjectStore;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class WalkGraphOp extends AbstractGeoGigOp<Void> {

    private String reference;

    private Listener listener;

    private boolean skipFeatures;

    private boolean verify;

    private boolean all;

    private boolean includeRemoteRefs;

    private boolean followHistory;

    private boolean skipBuckets;

    public static interface Listener {

        public void ref(Ref ref);

        public void endRef(Ref ref);

        public void featureType(RevFeatureType ftype);

        public void commit(RevCommit commit);

        public void endCommit(RevCommit commit);

        public void feature(final NodeRef featureNode);

        public void starTree(final NodeRef treeNode);

        public void endTree(final NodeRef treeNode);

        public void bucket(final BucketIndex bucketIndex, final Bucket bucket);

        public void endBucket(final BucketIndex bucketIndex, final Bucket bucket);

        public void startTag(RevTag tag);

        public void endTag(RevTag tag);
    }

    public WalkGraphOp setListener(final Listener listener) {
        this.listener = listener;
        return this;
    }

    public WalkGraphOp setReference(final String reference) {
        this.reference = reference;
        return this;
    }

    @Override
    protected Void _call() {
        if (all || includeRemoteRefs) {
            checkArgument(reference == null,
                    "'tree-ish' (%s) argument is mutually exclusive with 'all' (%s) and 'remotes' (%s)",
                    reference, all, includeRemoteRefs);
        } else {
            checkArgument(reference != null, "Reference not provided");
        }
        Preconditions.checkState(listener != null, "Listener not provided");

        ObjectStore source = objectDatabase();
        List<Object> rootElements = new ArrayList<>();

        if (reference == null) {
            if (all || includeRemoteRefs) {
                Iterable<Object> allRefs = allRefs();
                rootElements = Lists.newArrayList(allRefs);
            }
        } else {
            Optional<Ref> ref = command(RefParse.class).setName(reference).call();
            if (ref.isPresent()) {
                rootElements.add(ref.get());
            } else {
                Optional<ObjectId> oid = command(RevParse.class).setRefSpec(reference).call();

                if (!oid.isPresent()) {
                    source = indexDatabase();
                    oid = command(RevParse.class).setRefSpec(reference).setSource(source).call();
                }

                Preconditions.checkArgument(oid.isPresent(), "Can't resolve reference '%s' at %s",
                        reference, repository().getLocation());

                RevObject revObject = source.get(oid.get());
                Preconditions.checkArgument(
                        revObject instanceof RevCommit || revObject instanceof RevTree,
                        "'%s' can't be resolved to a tree: %s", reference, revObject.getType());

                rootElements.add(revObject);
            }
        }

        final ObjectStore treeSource = source;
        Consumer consumer = new Consumer() {

            private WalkGraphOp.Listener listener = WalkGraphOp.this.listener;

            private final ObjectStore odb = objectDatabase();

            // used to report feature types only once
            private Set<ObjectId> visitedTypes = new HashSet<ObjectId>();

            @Override
            public boolean tree(@Nullable NodeRef left, @Nullable NodeRef right) {
                if (!right.getMetadataId().isNull()) {
                    ObjectId featureTypeId = right.getMetadataId();
                    if (!visitedTypes.contains(featureTypeId)) {
                        visitedTypes.add(featureTypeId);
                        listener.featureType(odb.getFeatureType(featureTypeId));
                        if (!featureTypeId.isNull()) {
                            checkExists(featureTypeId, featureTypeId, odb);
                        }
                    }
                }
                listener.starTree(right);
                checkExists(right.getObjectId(), right, treeSource);
                return true;
            }

            @Override
            public boolean feature(@Nullable NodeRef left, @Nullable NodeRef right) {
                if (!skipFeatures) {
                    listener.feature(right);
                    checkExists(right.getObjectId(), right, odb);
                }
                return true;
            }

            @Override
            public void endTree(@Nullable NodeRef left, @Nullable NodeRef right) {
                listener.endTree(right);
            }

            @Override
            public boolean bucket(NodeRef lp, NodeRef rp, BucketIndex bucketIndex,
                    @Nullable Bucket left, @Nullable Bucket right) {
                if (skipBuckets) {
                    return false;
                }
                listener.bucket(bucketIndex, right);
                checkExists(right.getObjectId(), right, treeSource);
                return true;
            }

            @Override
            public void endBucket(NodeRef lp, NodeRef rp, BucketIndex bucketIndex,
                    @Nullable Bucket left, @Nullable Bucket right) {
                if (!skipBuckets) {
                    listener.endBucket(bucketIndex, right);
                }
            }

            private void checkExists(ObjectId id, Object o, ObjectStore store) {
                if (verify && !store.exists(id)) {
                    throw new IllegalStateException("Object " + o + " not found.");
                }
            }

        };

        final RevTree left = RevTree.EMPTY;

        Set<Ref> visitedRefs = new HashSet<>();
        for (Object root : rootElements) {
            Ref ref = null;
            RevTag tag = null;
            RevCommit commit = null;

            RevTree tree = null;

            if (root instanceof Ref) {
                ref = (Ref) root;
            } else if (root instanceof RevTag) {
                tag = (RevTag) root;
            } else if (root instanceof RevCommit) {
                commit = (RevCommit) root;
            } else if (root instanceof RevTree) {
                tree = (RevTree) root;
            }

            if (ref != null) {
                if (visitedRefs.contains(ref)) {
                    continue;
                }
                listener.ref(ref);
                if (ref instanceof SymRef) {
                    String targetRefName = ((SymRef) ref).getTarget();
                    Optional<Ref> targetref = command(RefParse.class).setName(targetRefName).call();
                    Preconditions.checkState(targetref.isPresent(), "Target ref %s not found",
                            targetRefName);
                    Ref target = targetref.get();
                    if (!visitedRefs.contains(target)) {
                        listener.ref(target);
                    }
                }
                RevObject obj = source.get(ref.getObjectId());
                if (obj instanceof RevTag) {
                    tag = (RevTag) obj;
                } else if (obj instanceof RevCommit) {
                    commit = (RevCommit) obj;
                } else if (obj instanceof RevTree) {
                    tree = (RevTree) obj;
                } else {
                    throw new UnsupportedOperationException(String.valueOf(obj));
                }
            }
            if (tag != null) {
                listener.startTag(tag);
                commit = source.getCommit(tag.getCommitId());
            }
            if (commit != null) {
                listener.commit(commit);
                tree = source.getTree(commit.getTreeId());
            }

            Preconditions.checkNotNull(tree);

            PreOrderDiffWalk walk = new PreOrderDiffWalk(left, tree, source, source, true);
            walk.walk(consumer);

            if (commit != null) {
                listener.endCommit(commit);
            }
            if (tag != null) {
                listener.endTag(tag);
            }
            if (ref != null && !visitedRefs.contains(ref)) {
                if (ref instanceof SymRef) {
                    String targetRefName = ((SymRef) ref).getTarget();
                    Optional<Ref> targetref = command(RefParse.class).setName(targetRefName).call();
                    Ref target = targetref.get();
                    if (!visitedRefs.contains(target)) {
                        listener.endRef(target);
                        visitedRefs.add(target);
                    }
                }
                listener.endRef(ref);
                visitedRefs.add(ref);
            }
        }
        return null;
    }

    private Iterable<Object> allRefs() {

        ImmutableList<Ref> branches = command(BranchListOp.class).setLocal(true)
                .setRemotes(includeRemoteRefs).call();

        ImmutableList<RevTag> tags = command(TagListOp.class).call();

        List<Ref> knownDanlingRefs = findKnownDanlingRefs();

        Iterable<Object> result = Iterables.concat(knownDanlingRefs, branches, tags);

        return result;
    }

    private List<Ref> findKnownDanlingRefs() {
        List<String> names = Lists.newArrayList(Ref.CHERRY_PICK_HEAD, Ref.HEAD, Ref.MERGE_HEAD,
                Ref.ORIG_HEAD, Ref.STAGE_HEAD, Ref.WORK_HEAD);
        return findRefs(names);
    }

    private List<Ref> findRefs(List<String> names) {
        List<Ref> refs = new ArrayList<>();
        for (String name : names) {
            Optional<Ref> ref = command(RefParse.class).setName(name).call();
            if (ref.isPresent()) {
                refs.add(ref.get());
            }
        }
        return refs;
    }

    /**
     * Do note report feature nodes
     * 
     * @param skipFeatures
     * @return
     */
    public WalkGraphOp setSkipFeatures(boolean skipFeatures) {
        this.skipFeatures = skipFeatures;
        return this;
    }

    /**
     * Do not report bucket nodes (implies skipFeatures == true)
     * 
     * @param skipBuckets
     * @return
     */
    public WalkGraphOp setSkipBuckets(boolean skipBuckets) {
        this.skipBuckets = skipBuckets;
        return this;
    }

    public WalkGraphOp setVerify(boolean verify) {
        this.verify = verify;
        return this;
    }

    public WalkGraphOp setAll(boolean all) {
        this.all = all;
        return this;
    }

    public WalkGraphOp setIncludeRemotes(boolean includeRemoteRefs) {
        this.includeRemoteRefs = includeRemoteRefs;
        return this;
    }

    public WalkGraphOp setFollowHistory(boolean follow) {
        this.followHistory = follow;
        return this;
    }
}
