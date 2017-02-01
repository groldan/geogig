package org.locationtech.geogig.di;

import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.storage.RefDatabase;
import org.locationtech.geogig.storage.memory.HeapRefDatabase;

public class RefDatabaseSnapshot implements RefDatabase {

    private RefDatabase refs;

    private HeapRefDatabase snapshot;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public RefDatabaseSnapshot(RefDatabase refs) {
        this.refs = refs;
    }

    @Override
    public void lock() throws TimeoutException {
        throw new UnsupportedOperationException("this is a read-only RefDatabase");
    }

    @Override
    public void unlock() {
        throw new UnsupportedOperationException("this is a read-only RefDatabase");
    }

    private void invalidate() {
        if (snapshot != null) {
            snapshot.close();
            snapshot = null;
        }
    }

    private RefDatabase snapshot() {
        if (snapshot == null) {
            Map<String, String> all = refs.getAll();
            snapshot = new HeapRefDatabase();
            snapshot.create();
            snapshot.putAll(all);
        }
        return snapshot;
    }

    @Override
    public void putRef(String refName, String refValue) {
        throw new UnsupportedOperationException("this is a read-only RefDatabase");
    }

    @Override
    public void putSymRef(String name, String val) {
        throw new UnsupportedOperationException("this is a read-only RefDatabase");
    }

    @Override
    public String remove(String refName) {
        throw new UnsupportedOperationException("this is a read-only RefDatabase");
    }

    @Override
    public Map<String, String> removeAll(String namespace) {
        throw new UnsupportedOperationException("this is a read-only RefDatabase");
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        lock.writeLock().lock();
        try {
            refs.configure();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean checkConfig() throws RepositoryConnectionException {
        lock.writeLock().lock();
        try {
           return refs.checkConfig();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void create() {
        lock.writeLock().lock();
        try {
            refs.create();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            invalidate();
            refs.close();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String getRef(String name) {
        lock.readLock().lock();
        try {
            return snapshot().getRef(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String getSymRef(String name) {
        lock.readLock().lock();
        try {
            return snapshot().getSymRef(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Map<String, String> getAll() {
        lock.readLock().lock();
        try {
            return snapshot().getAll();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Map<String, String> getAll(String prefix) {
        lock.readLock().lock();
        try {
            return snapshot().getAll(prefix);
        } finally {
            lock.readLock().unlock();
        }
    }

}