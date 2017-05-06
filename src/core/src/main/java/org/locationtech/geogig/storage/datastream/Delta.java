package org.locationtech.geogig.storage.datastream;

import org.locationtech.geogig.model.ObjectId;

public interface Delta {

    public int getDeltaLevel();
    
    public ObjectId getOriginalId();
}
