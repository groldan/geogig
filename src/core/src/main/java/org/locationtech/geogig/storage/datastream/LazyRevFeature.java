package org.locationtech.geogig.storage.datastream;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.locationtech.geogig.api.AbstractRevObject;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.RevFeature;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

class LazyRevFeature extends AbstractRevObject implements RevFeature {

    private List<Optional<Object>> lazyList;

    private List<Optional> resolvedList;

    LazyRevFeature(ObjectId id, List<Optional<Object>> lazyList) throws IOException {
        super(id);
        this.lazyList = lazyList;
        this.resolvedList = Arrays.asList(new Optional[lazyList.size()]);
    }

    @Override
    public TYPE getType() {
        return TYPE.FEATURE;
    }

    @Override
    public ImmutableList<Optional<Object>> getValues() {
        if (!(lazyList instanceof ImmutableList)) {
            lazyList = ImmutableList.copyOf(lazyList);
        }
        return (ImmutableList<Optional<Object>>) lazyList;
    }

    @Override
    public int valueCount() {
        return lazyList.size();
    }

    @Override
    public Optional<Object> getValue(int index) {
        Optional<Object> val = resolvedList.get(index);
        if (val == null) {
            val = lazyList.get(index);
            resolvedList.set(index, val);
        }
        return val;
    }
}
