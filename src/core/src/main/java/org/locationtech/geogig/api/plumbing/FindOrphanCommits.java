package org.locationtech.geogig.api.plumbing;

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.transform;

import java.util.HashSet;
import java.util.Set;

import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.Ref;
import org.locationtech.geogig.model.RevTag;
import org.locationtech.geogig.porcelain.BranchListOp;
import org.locationtech.geogig.porcelain.TagListOp;
import org.locationtech.geogig.repository.AbstractGeoGigOp;
import org.locationtech.geogig.storage.GraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/**
 * Walks the commit graph backwards starting at the current tips (commits pointed out to by any
 * current branch or tag ref) until an orphan commit is found (i.e. an initial commit with no
 * parent) and returns it.
 * <p>
 * In general, all branches lead to a single initial commit except if orphan branches have been
 * created, in which case there's more than one "orphan" commit.
 */
public class FindOrphanCommits extends AbstractGeoGigOp<Set<ObjectId>> {

    private static final Logger LOG = LoggerFactory.getLogger(FindOrphanCommits.class);

    private Set<ObjectId> tips = new HashSet<>();

    @Override
    protected Set<ObjectId> _call() {
        Set<ObjectId> tips = resolveTips();

        Set<ObjectId> visited = new HashSet<>();
        Set<ObjectId> orphan = new HashSet<>();

        for (ObjectId tip : tips) {
            traverse(tip, visited, orphan);
        }

        return orphan;
    }

    private void traverse(ObjectId tip, Set<ObjectId> visited, Set<ObjectId> orphan) {
        final GraphDatabase graph = graphDatabase();

        ImmutableList<ObjectId> parents = graph.getParents(tip);
        if (parents.isEmpty()) {
            LOG.debug("Found orphan " + tip);
            orphan.add(tip);
            return;
        }
        for (ObjectId parentId : parents) {
            if (visited.contains(parentId)) {
                LOG.debug("Ignoring commit {}, parent {} already traversed.", tip, parentId);
                continue;
            }
            visited.add(tip);
            traverse(parentId, visited, orphan);
        }
    }

    private Set<ObjectId> resolveTips() {
        if (!this.tips.isEmpty()) {
            return this.tips;
        }
        ImmutableList<Ref> branches = command(BranchListOp.class).setLocal(true).setRemotes(true)
                .call();

        ImmutableList<RevTag> tags = command(TagListOp.class).call();

        Set<ObjectId> tips = Sets.newHashSet(//
                concat(//
                        transform(branches.iterator(), (ref) -> ref.getObjectId()), //
                        transform(tags.iterator(), (t) -> t.getCommitId())//
                )//
        );

        return tips;
    }
}
